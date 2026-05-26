from __future__ import annotations

import time
from pathlib import Path

from fastapi.testclient import TestClient

from main import create_app


class FailingStore:
    """Simulate a follower that refuses to accept replication temporarily."""

    def __init__(self, base: Path):
        from app.storage.log_store import LogStore

        self._base = base
        self._real = LogStore(base)

    def append_message(self, topic_name: str, partition: int, key: str | None, value):
        raise RuntimeError("simulated replication failure")

    def partition_stats(self, topic_name: str, partition: int):
        # delegate to real store for offsets that were prior
        return self._real.partition_stats(topic_name, partition)


def test_isr_smoothing_and_recovery(tmp_path: Path) -> None:
    app = create_app(tmp_path)
    tc = TestClient(app)

    # register follower
    assert tc.post("/cluster/brokers/register", json={"broker_id": "b-follower"}).status_code == 200
    # create topic
    assert tc.post("/topics", json={"topic_name": "orders", "partitions": 1}).status_code == 201

    coord = tc.app.state.broker_service.cluster_coordinator

    # initial produce: both replicas should be in-sync
    assert tc.post("/produce", json={"topic_name": "orders", "value": {"n": 1}}).status_code == 201
    rep = tc.get("/cluster/replication/orders/0").json()
    assert len(rep["in_sync_replicas"]) >= 1

    # simulate follower failure to accept replication
    coord._logstores["b-follower"] = FailingStore(tmp_path / "replicas" / "b-follower")

    # produce several messages to grow leader offset and smoothed lag
    for i in range(5):
        assert tc.post("/produce", json={"topic_name": "orders", "value": {"n": i}}).status_code == 201

    rep2 = tc.get("/cluster/replication/orders/0").json()
    # follower should have smoothed lag > threshold and likely be out of ISR
    lags = rep2.get("replica_lags", {})
    assert any(float(v) > coord.lag_threshold for v in lags.values())

    # restore real store to allow replication
    del coord._logstores["b-follower"]

    # produce one more message to trigger replication to the follower
    assert tc.post("/produce", json={"topic_name": "orders", "value": {"n": 999}}).status_code == 201

    # give a short moment for smoothed lag to reduce (synchronous in our impl)
    time.sleep(0.1)
    rep3 = tc.get("/cluster/replication/orders/0").json()
    lags3 = rep3.get("replica_lags", {})
    assert any(float(v) <= coord.lag_threshold for v in lags3.values())
