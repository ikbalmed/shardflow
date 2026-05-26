from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from fastapi.testclient import TestClient

from app import cli
from main import create_app


class FakeClient:
    def __init__(self, test_client: TestClient):
        self._tc = test_client

    def get(self, path, params=None):
        if params:
            return self._tc.get(path, params=params)
        return self._tc.get(path)

    def post(self, path, json=None):
        return self._tc.post(path, json=json)


def test_cli_register_and_list_brokers(tmp_path: Path) -> None:
    app = create_app(tmp_path)
    tc = TestClient(app)
    client = FakeClient(tc)

    args = Namespace(broker_id="cli-broker-1")
    cli._register_broker(client, args)

    # list brokers
    args2 = Namespace()
    cli._list_brokers(client, args2)


def test_cli_partition_and_failover(tmp_path: Path) -> None:
    app = create_app(tmp_path)
    tc = TestClient(app)
    client = FakeClient(tc)

    # register follower
    client.post("/cluster/brokers/register", json={"broker_id": "b2"})
    # create topic
    tc.post("/topics", json={"topic_name": "orders", "partitions": 1})

    args = Namespace(topic="orders")
    cli._topic_cluster(client, args)

    args_p = Namespace(topic="orders", partition=0)
    cli._partition_cluster(client, args_p)
    cli._trigger_failover(client, args_p)
