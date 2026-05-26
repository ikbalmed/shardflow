from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from fastapi.testclient import TestClient

from main import create_app
from app import cli


def test_set_unknown_broker_rejected(tmp_path: Path) -> None:
    app = create_app(tmp_path)
    tc = TestClient(app)
    resp = tc.post("/cluster/brokers/unknown/status", json={"status": "failed"})
    assert resp.status_code == 404


def test_invalid_status_rejected(tmp_path: Path) -> None:
    app = create_app(tmp_path)
    tc = TestClient(app)
    resp = tc.post("/cluster/brokers/broker-A/status", json={"status": "down"})
    assert resp.status_code == 422


def test_set_broker_failed_and_recover(tmp_path: Path) -> None:
    app = create_app(tmp_path)
    tc = TestClient(app)

    # register follower
    assert tc.post("/cluster/brokers/register", json={"broker_id": "broker-B"}).status_code == 200
    # create topic
    assert tc.post("/topics", json={"topic_name": "orders", "partitions": 1}).status_code == 201

    # local broker is 'broker-1' by default; set it to failed
    r = tc.post("/cluster/brokers/broker-1/status", json={"status": "failed"})
    assert r.status_code == 200
    body = r.json()
    assert body["broker"]["status"] == "failed"
    assert isinstance(body["affected"], list)

    # now set back to active
    r2 = tc.post("/cluster/brokers/broker-1/status", json={"status": "active"})
    assert r2.status_code == 200
    assert r2.json()["broker"]["status"] == "active"

    # test CLI path
    client = type("X", (), {"post": tc.post, "get": tc.get})
    args = Namespace(broker_id="broker-1", status="failed")
    cli._set_broker_status(client, args)
