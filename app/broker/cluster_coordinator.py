from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional

from app.storage.log_store import LogStore


class ClusterError(Exception):
    pass


class BrokerNotRegisteredError(ClusterError):
    pass


class NotLeaderError(ClusterError):
    pass


@dataclass
class ClusterCoordinator:
    cluster_file: Path
    data_dir: Path
    replication_factor: int = 2
    heartbeat_timeout: int = 5
    # Phase 4 ISR smoothing parameters
    lag_threshold: int = 1  # max allowed smoothed lag to be considered in-sync
    freshness_seconds: int = 5  # how recent a replication must be to count as fresh
    smoothing_alpha: float = 0.3  # EMA alpha for smoothing instantaneous lag

    def __post_init__(self) -> None:
        self._lock = Lock()
        self._state: Dict = {"brokers": {}, "topics": {}}
        self.cluster_file.parent.mkdir(parents=True, exist_ok=True)
        self._load()
        self._logstores: Dict[str, LogStore] = {}

    def _load(self) -> None:
        if self.cluster_file.exists():
            try:
                with self.cluster_file.open("r", encoding="utf-8") as fh:
                    self._state = json.load(fh)
            except Exception:
                self._state = {"brokers": {}, "topics": {}}

    def _save(self) -> None:
        tmp = self.cluster_file.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(self._state, fh)
        tmp.replace(self.cluster_file)

    def register_broker(self, broker_id: str) -> dict:
        with self._lock:
            now = time.time()
            brokers = self._state.setdefault("brokers", {})
            brokers[broker_id] = {"broker_id": broker_id, "status": "active", "registered_at": now, "last_heartbeat_at": now}
            self._save()
            return brokers[broker_id]

    def heartbeat(self, broker_id: str) -> dict:
        with self._lock:
            brokers = self._state.setdefault("brokers", {})
            if broker_id not in brokers:
                raise BrokerNotRegisteredError(broker_id)
            brokers[broker_id]["last_heartbeat_at"] = time.time()
            brokers[broker_id]["status"] = "active"
            self._save()
            return brokers[broker_id]

    def list_brokers(self) -> list:
        with self._lock:
            return list(self._state.get("brokers", {}).values())

    def assign_replicas_for_topic(self, topic_name: str, partitions: int, replication_factor: Optional[int] = None) -> dict:
        with self._lock:
            rf = replication_factor or self.replication_factor
            brokers = sorted(self._state.get("brokers", {}).keys())
            if not brokers:
                # leave topics entry empty; assignment deferred
                self._state.setdefault("topics", {}).setdefault(topic_name, {})
                self._save()
                return {}
            topic_meta = {str(p): {"replicas": [], "leader": None, "leader_epoch": 0} for p in range(partitions)}
            for p in range(partitions):
                # choose replicas round-robin starting at p
                replicas = []
                for i in range(rf):
                    replicas.append(brokers[(p + i) % len(brokers)])
                leader = replicas[0] if replicas else None
                topic_meta[str(p)]["replicas"] = replicas
                topic_meta[str(p)]["leader"] = leader
                topic_meta[str(p)]["leader_epoch"] = 0
                topic_meta[str(p)]["replica_lags"] = {b: 0 for b in replicas}
                topic_meta[str(p)]["replica_offsets"] = {b: 0 for b in replicas}
                topic_meta[str(p)]["replica_last_replication"] = {b: 0 for b in replicas}
                topic_meta[str(p)]["replica_lag_smoothed"] = {b: 0.0 for b in replicas}
                topic_meta[str(p)]["in_sync_replicas"] = replicas.copy()
            self._state.setdefault("topics", {})[topic_name] = topic_meta
            self._save()
            return topic_meta

    def get_partition_info(self, topic_name: str, partition: int) -> dict:
        with self._lock:
            topic = self._state.get("topics", {}).get(topic_name)
            if not topic:
                raise ClusterError("topic not found")
            part = topic.get(str(partition))
            if not part:
                raise ClusterError("partition not found")
            # check broker heartbeats to mark failed
            self._refresh_broker_states_locked()
            return part

    def get_topic_info(self, topic_name: str) -> dict:
        with self._lock:
            topic = self._state.get("topics", {}).get(topic_name)
            if topic is None:
                raise ClusterError("topic not found")
            self._refresh_broker_states_locked()
            return topic

    def _refresh_broker_states_locked(self) -> None:
        now = time.time()
        brokers = self._state.setdefault("brokers", {})
        for bid, info in brokers.items():
            last = info.get("last_heartbeat_at", 0)
            if now - last > self.heartbeat_timeout:
                info["status"] = "failed"

    def leader_for(self, topic_name: str, partition: int) -> Optional[str]:
        with self._lock:
            try:
                part = self._state.get("topics", {}).get(topic_name, {}).get(str(partition))
                if not part:
                    return None
                leader = part.get("leader")
                # ensure leader is active
                brokers = self._state.get("brokers", {})
                if leader and brokers.get(leader, {}).get("status") == "active":
                    return leader
                return None
            finally:
                pass

    def replicate_record(self, topic_name: str, partition: int, record: dict) -> None:
        """Replicate a record (dict with offset,key,value,timestamp) to follower replica stores."""
        with self._lock:
            part = self._state.get("topics", {}).get(topic_name, {}).get(str(partition))
            if not part:
                return
            replicas = part.get("replicas", [])
            leader = part.get("leader")
            now = time.time()
            leader_offset = part.get("leader_offset", record.get("offset", 0))
            for broker_id in replicas:
                if broker_id == leader:
                    continue
                store = self._get_logstore_for_broker(broker_id)
                try:
                    rec = store.append_message(topic_name, partition, record.get("key"), record.get("value"))
                    # update raw offset and timestamp
                    part.setdefault("replica_offsets", {})[broker_id] = rec.offset
                    part.setdefault("replica_last_replication", {})[broker_id] = now
                    # instantaneous lag
                    inst_lag = max(0, leader_offset - rec.offset)
                    prev = float(part.setdefault("replica_lag_smoothed", {}).get(broker_id, 0.0))
                    smooth = float(self.smoothing_alpha * inst_lag + (1 - self.smoothing_alpha) * prev)
                    part.setdefault("replica_lag_smoothed", {})[broker_id] = smooth
                except Exception:
                    # replication failed; increment smoothed lag toward large value
                    prev = float(part.setdefault("replica_lag_smoothed", {}).get(broker_id, 0.0))
                    smooth = float(min(prev + 1.0, max(prev * 1.5, self.lag_threshold * 10)))
                    part.setdefault("replica_lag_smoothed", {})[broker_id] = smooth
                # recompute in_sync list per replica
            in_sync = []
            now = time.time()
            for b in replicas:
                sm = float(part.setdefault("replica_lag_smoothed", {}).get(b, 0.0))
                last = float(part.setdefault("replica_last_replication", {}).get(b, 0))
                fresh = (now - last) <= float(self.freshness_seconds)
                if sm <= float(self.lag_threshold) and fresh:
                    in_sync.append(b)
            part["in_sync_replicas"] = in_sync
            self._save()

    def _replica_offset_locked(self, topic_name: str, partition: int, broker_id: str) -> int:
        store = self._get_logstore_for_broker(broker_id)
        try:
            stats = store.partition_stats(topic_name, partition)
            return stats.end_offset
        except Exception:
            return 0

    def recompute_isr_for_partition(self, topic_name: str, partition: int) -> None:
        """Recompute in-sync replicas using smoothed lag and freshness rules."""
        with self._lock:
            part = self._state.get("topics", {}).get(topic_name, {}).get(str(partition))
            if not part:
                return
            leader_offset = part.get("leader_offset", 0)
            replicas = part.get("replicas", [])
            now = time.time()
            in_sync = []
            for b in replicas:
                sm = float(part.setdefault("replica_lag_smoothed", {}).get(b, 0.0))
                last = float(part.setdefault("replica_last_replication", {}).get(b, 0))
                fresh = (now - last) <= float(self.freshness_seconds)
                if sm <= float(self.lag_threshold) and fresh:
                    in_sync.append(b)
            part["in_sync_replicas"] = in_sync
            self._save()

    def _get_logstore_for_broker(self, broker_id: str) -> LogStore:
        if broker_id in self._logstores:
            return self._logstores[broker_id]
        base = self.data_dir / "replicas" / broker_id
        store = LogStore(base, max_records_per_segment=1000, retention_segments=100)
        self._logstores[broker_id] = store
        return store

    def note_leader_append(self, topic_name: str, partition: int, offset: int) -> None:
        with self._lock:
            part = self._state.setdefault("topics", {}).setdefault(topic_name, {}).setdefault(str(partition), {})
            part["leader_offset"] = offset
            # update smoothed lags by sampling current replica offsets
            replicas = part.get("replicas", [])
            now = time.time()
            for b in replicas:
                roff = self._replica_offset_locked(topic_name, partition, b)
                prev = float(part.setdefault("replica_lag_smoothed", {}).get(b, 0.0))
                inst = max(0, offset - roff)
                smooth = float(self.smoothing_alpha * inst + (1 - self.smoothing_alpha) * prev)
                part.setdefault("replica_lag_smoothed", {})[b] = smooth
                # if the store reports recent replication, update last_replication
                part.setdefault("replica_last_replication", {}).setdefault(b, now if roff > 0 else 0)
            # recompute in-sync replicas
            self.recompute_isr_for_partition(topic_name, partition)

    def failover_partition(self, topic_name: str, partition: int) -> str:
        with self._lock:
            part = self._state.get("topics", {}).get(topic_name, {}).get(str(partition))
            if not part:
                raise ClusterError("partition not found")
            replicas = part.get("replicas", [])
            leader = part.get("leader")
            # find next active follower
            brokers = self._state.get("brokers", {})
            for b in replicas:
                if b == leader:
                    continue
                if brokers.get(b, {}).get("status") == "active":
                    part["leader"] = b
                    part["leader_epoch"] = part.get("leader_epoch", 0) + 1
                    self._save()
                    return b
            raise ClusterError("no eligible follower for failover")

    def set_broker_status(self, broker_id: str, status: str) -> dict:
        """Force-set broker status to 'active' or 'failed'. Returns broker info and affected partitions."""
        if status not in ("active", "failed"):
            raise ClusterError("invalid status")
        with self._lock:
            brokers = self._state.setdefault("brokers", {})
            if broker_id not in brokers:
                raise BrokerNotRegisteredError(broker_id)
            brokers[broker_id]["status"] = status
            brokers[broker_id]["last_heartbeat_at"] = time.time()
            affected: List[dict] = []
            if status == "failed":
                # for partitions led by this broker, attempt failover
                topics = self._state.get("topics", {})
                for tname, parts in topics.items():
                    for p, meta in parts.items():
                        if meta.get("leader") == broker_id:
                            old = broker_id
                            # attempt to pick next active follower
                            replicas = meta.get("replicas", [])
                            new_leader = None
                            for b in replicas:
                                if b == broker_id:
                                    continue
                                if brokers.get(b, {}).get("status") == "active":
                                    new_leader = b
                                    break
                            if new_leader:
                                meta["leader"] = new_leader
                                meta["leader_epoch"] = meta.get("leader_epoch", 0) + 1
                                affected.append({"topic": tname, "partition": int(p), "old_leader": old, "new_leader": new_leader})
                            else:
                                # no eligible follower; leave leader as-is but mark none
                                meta["leader"] = None
                                affected.append({"topic": tname, "partition": int(p), "old_leader": old, "new_leader": None})
                self._save()
            elif status == "active":
                # broker becomes eligible again; no immediate reassignment to avoid surprise
                self._save()
            return {"broker": brokers[broker_id], "affected": affected}


__all__ = ["ClusterCoordinator", "ClusterError", "BrokerNotRegisteredError", "NotLeaderError"]
