from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Dict, List


@dataclass
class GroupCoordinator:
    groups_file: Path
    topic_manager: object
    expiration_seconds: int = 10

    def __post_init__(self) -> None:
        self._lock = Lock()
        self._groups: Dict[str, dict] = {}
        self.groups_file.parent.mkdir(parents=True, exist_ok=True)
        self._load()

    def _load(self) -> None:
        if self.groups_file.exists():
            try:
                with self.groups_file.open("r", encoding="utf-8") as fh:
                    self._groups = json.load(fh)
            except Exception:
                self._groups = {}

    def _save(self) -> None:
        tmp = self.groups_file.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(self._groups, fh)
        tmp.replace(self.groups_file)

    def _now(self) -> float:
        return time.time()

    def register_consumer(self, group_id: str, consumer_id: str, topic_name: str) -> dict:
        with self._lock:
            group = self._groups.setdefault(group_id, {"members": {}, "assignments": {}})
            members = group.setdefault("members", {})
            members[consumer_id] = {
                "topics": [topic_name],
                "last_seen": self._now(),
            }
            # expire stale and rebalance
            removed = self._expire_stale_members_locked(group_id)
            self._rebalance_locked(group_id)
            self._save()
            return {"removed": removed, "assignments": group.get("assignments", {})}

    def heartbeat(self, group_id: str, consumer_id: str) -> dict:
        with self._lock:
            group = self._groups.get(group_id)
            if not group or consumer_id not in group.get("members", {}):
                return {"ok": False}
            group["members"][consumer_id]["last_seen"] = self._now()
            removed = self._expire_stale_members_locked(group_id)
            if removed:
                self._rebalance_locked(group_id)
            self._save()
            return {"ok": True, "removed": removed}

    def unregister_consumer(self, group_id: str, consumer_id: str) -> dict:
        with self._lock:
            group = self._groups.get(group_id)
            if not group:
                return {"ok": False}
            members = group.get("members", {})
            if consumer_id in members:
                del members[consumer_id]
            self._rebalance_locked(group_id)
            self._save()
            return {"ok": True}

    def get_group_state(self, group_id: str) -> dict | None:
        with self._lock:
            group = self._groups.get(group_id)
            if not group:
                return None
            # lazy expire before returning
            removed = self._expire_stale_members_locked(group_id)
            if removed:
                self._rebalance_locked(group_id)
                self._save()
            return self._groups.get(group_id)

    def get_assignments(self, group_id: str) -> dict | None:
        with self._lock:
            group = self._groups.get(group_id)
            if not group:
                return None
            # lazy expire
            removed = self._expire_stale_members_locked(group_id)
            if removed:
                self._rebalance_locked(group_id)
                self._save()
            return group.get("assignments", {})

    def _expire_stale_members_locked(self, group_id: str) -> List[str]:
        group = self._groups.get(group_id)
        if not group:
            return []
        cutoff = self._now() - float(self.expiration_seconds)
        members = group.get("members", {})
        removed: List[str] = []
        for consumer_id, info in list(members.items()):
            if info.get("last_seen", 0) < cutoff:
                removed.append(consumer_id)
                del members[consumer_id]
        return removed

    def _rebalance_locked(self, group_id: str) -> None:
        group = self._groups.get(group_id)
        if not group:
            return
        assignments: Dict[str, Dict[str, str]] = {}
        members = group.get("members", {})
        # gather topics subscribed by members
        topic_to_consumers: Dict[str, List[str]] = {}
        for consumer_id, info in members.items():
            for topic in info.get("topics", []):
                topic_to_consumers.setdefault(topic, []).append(consumer_id)

        for topic, consumers in topic_to_consumers.items():
            consumers = sorted(consumers)
            try:
                partition_count = self.topic_manager.partition_count(topic)
            except Exception:
                partition_count = 0
            topic_assign: Dict[str, str] = {}
            if consumers and partition_count > 0:
                for p in range(partition_count):
                    owner = consumers[p % len(consumers)]
                    topic_assign[str(p)] = owner
            assignments[topic] = topic_assign

        group["assignments"] = assignments


__all__ = ["GroupCoordinator"]
