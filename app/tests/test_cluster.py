from __future__ import annotations

import time
from pathlib import Path

from fastapi.testclient import TestClient

from main import create_app


def client(tmp_path: Path, broker_id: str = "broker-A", **kwargs) -> TestClient:
    app = create_app(tmp_path, broker_id=broker_id, **kwargs)
    return TestClient(app)


def test_broker_registration_and_replica_assignment(tmp_path: Path) -> None:
    test_client = client(tmp_path, broker_id="broker-A")

    # register another broker
    resp = test_client.post("/cluster/brokers/register", json={"broker_id": "broker-B"})
    assert resp.status_code == 200

    # create topic
    r = test_client.post("/topics", json={"topic_name": "orders", "partitions": 2})
    assert r.status_code == 201

    # check partition info
    p0 = test_client.get("/cluster/partitions/orders/0")
    assert p0.status_code == 200
    info = p0.json()
    assert "leader" in info and info["leader"] in ("broker-A", "broker-B")
    assert len(info["replicas"]) >= 1


def test_produce_only_allowed_on_leader_and_replication(tmp_path: Path) -> None:
    test_client = client(tmp_path, broker_id="broker-A")

    # register follower
    assert test_client.post("/cluster/brokers/register", json={"broker_id": "broker-B"}).status_code == 200
    # create topic
    assert test_client.post("/topics", json={"topic_name": "orders", "partitions": 1}).status_code == 201

    # leader should be broker-A for partition 0
    rep = test_client.get("/cluster/partitions/orders/0").json()
    leader = rep["leader"]
    assert leader == "broker-A"

    # produce should succeed when local is leader
    p = test_client.post("/produce", json={"topic_name": "orders", "value": {"n": 1}})
    assert p.status_code == 201

    # replication status should show follower replica lag (eventually 0 after replication)
    rs = test_client.get("/cluster/replication/orders/0")
    assert rs.status_code == 200
    body = rs.json()
    assert "replica_lags" in body


def test_failover_promotes_follower_and_writes_continue(tmp_path: Path) -> None:
    test_client = client(tmp_path, broker_id="broker-A")
    # register follower
    assert test_client.post("/cluster/brokers/register", json={"broker_id": "broker-B"}).status_code == 200
    # create topic
    assert test_client.post("/topics", json={"topic_name": "orders", "partitions": 1}).status_code == 201

    # simulate broker-A failure by marking its status to failed in cluster state
    app = test_client.app
    coord = app.state.broker_service.cluster_coordinator
    with coord._lock:
        coord._state["brokers"]["broker-A"]["status"] = "failed"
        coord._save()

    # failover partition
    fo = test_client.post("/cluster/failover/orders/0")
    assert fo.status_code == 200
    new = fo.json()["new_leader"]
    assert new == "broker-B"

    # produce should now fail if local broker is broker-A (not leader)
    p = test_client.post("/produce", json={"topic_name": "orders", "value": {"x": 1}})
    # local broker is still broker-A in this TestClient, so expect conflict
    assert p.status_code == 409
