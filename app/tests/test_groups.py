from __future__ import annotations

import time
from pathlib import Path

from fastapi.testclient import TestClient

from main import create_app


def client(tmp_path: Path, **kwargs) -> TestClient:
    app = create_app(tmp_path, **kwargs)
    return TestClient(app)


def test_group_registration_and_deterministic_assignment(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    # create topic with 3 partitions
    resp = test_client.post("/topics", json={"topic_name": "orders", "partitions": 3})
    assert resp.status_code == 201

    # register two consumers
    r1 = test_client.post("/groups/g1/register", json={"consumer_id": "c1", "topic_name": "orders"})
    assert r1.status_code == 200
    r2 = test_client.post("/groups/g1/register", json={"consumer_id": "c2", "topic_name": "orders"})
    assert r2.status_code == 200

    # fetch assignments
    assign = test_client.get("/groups/g1/assignments/orders")
    assert assign.status_code == 200
    body = assign.json()
    parts = {p["partition"]: p["consumer_id"] for p in body["assignments"]}
    # with consumers ['c1','c2'] sorted, expect partition -> consumer mapping 0->c1,1->c2,2->c1
    assert parts[0] == "c1"
    assert parts[1] == "c2"
    assert parts[2] == "c1"

    # add third consumer and expect rebalance
    r3 = test_client.post("/groups/g1/register", json={"consumer_id": "c3", "topic_name": "orders"})
    assert r3.status_code == 200
    assign2 = test_client.get("/groups/g1/assignments/orders").json()
    parts2 = {p["partition"]: p["consumer_id"] for p in assign2["assignments"]}
    # now consumers ['c1','c2','c3'] -> 0->c1,1->c2,2->c3
    assert parts2[0] == "c1"
    assert parts2[1] == "c2"
    assert parts2[2] == "c3"


def test_heartbeat_and_expiration_triggers_rebalance(tmp_path: Path) -> None:
    # use small expiration for test
    test_client = client(tmp_path, group_expiration_seconds=1)

    resp = test_client.post("/topics", json={"topic_name": "orders", "partitions": 3})
    assert resp.status_code == 201

    # register two consumers
    assert test_client.post("/groups/g2/register", json={"consumer_id": "a", "topic_name": "orders"}).status_code == 200
    assert test_client.post("/groups/g2/register", json={"consumer_id": "b", "topic_name": "orders"}).status_code == 200

    assign = test_client.get("/groups/g2/assignments/orders").json()
    parts = {p["partition"]: p["consumer_id"] for p in assign["assignments"]}
    assert len(parts) == 3

    # don't heartbeat 'b', wait for expiration
    time.sleep(1.2)
    # heartbeat 'a' to mark it alive
    assert test_client.post("/groups/g2/heartbeat", json={"consumer_id": "a"}).status_code == 200

    # after heartbeat, 'b' should be expired and assignments should only point to 'a'
    assign2 = test_client.get("/groups/g2/assignments/orders").json()
    parts2 = {p["partition"]: p["consumer_id"] for p in assign2["assignments"]}
    assert all(cid == "a" for cid in parts2.values())
