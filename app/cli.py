from __future__ import annotations

import argparse
import json
from typing import Any

import httpx


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mini Kafka CLI")
    parser.add_argument("--base-url", default="http://127.0.0.1:8001", help="API base URL")

    subparsers = parser.add_subparsers(dest="command", required=True)

    produce_parser = subparsers.add_parser("produce", help="Produce a message")
    produce_parser.add_argument("--topic", required=True)
    produce_parser.add_argument("--value", required=True, help="JSON value payload")
    produce_parser.add_argument("--key", default=None)

    consume_parser = subparsers.add_parser("consume", help="Consume messages")
    consume_parser.add_argument("--topic", required=True)
    consume_parser.add_argument("--partition", type=int, required=True)
    consume_parser.add_argument("--offset", type=int, default=0)
    consume_parser.add_argument("--limit", type=int, default=10)

    # broker admin
    subparsers.add_parser("list-brokers", help="List cluster brokers")

    register_broker = subparsers.add_parser("register-broker", help="Register a broker")
    register_broker.add_argument("--broker-id", required=True)

    heartbeat_broker = subparsers.add_parser("heartbeat-broker", help="Send heartbeat for broker")
    heartbeat_broker.add_argument("--broker-id", required=True)

    topic_cluster = subparsers.add_parser("topic-cluster", help="Show topic replication metadata")
    topic_cluster.add_argument("--topic", required=True)

    partition_cluster = subparsers.add_parser("partition-cluster", help="Show partition replication metadata")
    partition_cluster.add_argument("--topic", required=True)
    partition_cluster.add_argument("--partition", type=int, required=True)

    failover = subparsers.add_parser("trigger-failover", help="Trigger failover for partition")
    failover.add_argument("--topic", required=True)
    failover.add_argument("--partition", type=int, required=True)

    set_status = subparsers.add_parser("set-broker-status", help="Force-set broker status")
    set_status.add_argument("--broker-id", required=True)
    set_status.add_argument("--status", required=True, choices=["active", "failed"])

    return parser


def _parse_json_payload(value: str) -> Any:
    return json.loads(value)


def _produce(client: httpx.Client, args: argparse.Namespace) -> None:
    payload = {
        "topic_name": args.topic,
        "key": args.key,
        "value": _parse_json_payload(args.value),
    }
    response = client.post("/produce", json=payload)
    response.raise_for_status()
    print(response.json())


def _consume(client: httpx.Client, args: argparse.Namespace) -> None:
    response = client.get(
        "/consume",
        params={
            "topic_name": args.topic,
            "partition": args.partition,
            "offset": args.offset,
            "limit": args.limit,
        },
    )
    response.raise_for_status()
    print(response.json())


def _list_brokers(client: httpx.Client, args: argparse.Namespace) -> None:
    response = client.get("/cluster/brokers")
    response.raise_for_status()
    print(response.json())


def _register_broker(client: httpx.Client, args: argparse.Namespace) -> None:
    response = client.post("/cluster/brokers/register", json={"broker_id": args.broker_id})
    response.raise_for_status()
    print(response.json())


def _heartbeat_broker(client: httpx.Client, args: argparse.Namespace) -> None:
    response = client.post(f"/cluster/brokers/{args.broker_id}/heartbeat")
    response.raise_for_status()
    print(response.json())


def _topic_cluster(client: httpx.Client, args: argparse.Namespace) -> None:
    response = client.get(f"/cluster/topics/{args.topic}")
    response.raise_for_status()
    print(response.json())


def _partition_cluster(client: httpx.Client, args: argparse.Namespace) -> None:
    response = client.get(f"/cluster/partitions/{args.topic}/{args.partition}")
    response.raise_for_status()
    print(response.json())


def _trigger_failover(client: httpx.Client, args: argparse.Namespace) -> None:
    response = client.post(f"/cluster/failover/{args.topic}/{args.partition}")
    response.raise_for_status()
    print(response.json())


def _set_broker_status(client: httpx.Client, args: argparse.Namespace) -> None:
    response = client.post(f"/cluster/brokers/{args.broker_id}/status", json={"status": args.status})
    response.raise_for_status()
    print(response.json())


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    with httpx.Client(base_url=args.base_url, timeout=10.0) as client:
        if args.command == "produce":
            _produce(client, args)
        elif args.command == "consume":
            _consume(client, args)
        elif args.command == "list-brokers":
            _list_brokers(client, args)
        elif args.command == "register-broker":
            _register_broker(client, args)
        elif args.command == "heartbeat-broker":
            _heartbeat_broker(client, args)
        elif args.command == "topic-cluster":
            _topic_cluster(client, args)
        elif args.command == "partition-cluster":
            _partition_cluster(client, args)
        elif args.command == "trigger-failover":
            _trigger_failover(client, args)
        elif args.command == "set-broker-status":
            _set_broker_status(client, args)


if __name__ == "__main__":
    main()
