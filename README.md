# ShardFlow

ShardFlow is a Kafka-inspired event streaming backend built with Python and FastAPI. It is designed for local development, demos, and backend portfolio use: topics, partitions, offsets, consumer groups, simulated brokers, replication, failover, and operator-facing admin APIs all run in one process.

## Overview

ShardFlow provides a cohesive streaming backend that stores data on the local filesystem and exposes a clean HTTP API. It is intentionally practical rather than protocol-compatible. The project demonstrates durable log storage, partition-aware reads and writes, consumer group coordination, and a simplified cluster model with leader/follower metadata.

## What ShardFlow Demonstrates

- Topic and partition management
- Append-only partition logs with segment rotation and retention
- Offset-based produce and consume flows
- Consumer offset commits and lag calculation
- Deterministic consumer group assignment and rebalance behavior
- Broker metadata, broker health, and failover handling
- Leader/follower replica tracking with in-sync replica reporting
- A small operator CLI layered over the HTTP API

## Core Features

- Create topics with a fixed partition count.
- Produce messages using key-based partitioning or round-robin fallback.
- Consume messages by topic, partition, and offset.
- Persist records in segmented append-only logs.
- Retain a bounded number of segments per partition.
- Commit and inspect consumer-group offsets.
- Track lag per consumer group and partition.
- Register consumers, refresh heartbeats, and leave groups.
- Assign partitions deterministically and rebalance on membership changes.
- Register brokers, track heartbeats, update broker status, and simulate failover.
- Inspect partition leadership, replica sets, replica lag, and in-sync replicas.

## Architecture Overview

ShardFlow is organized around small, focused service modules.

```text
Client / CLI
	|
	v
FastAPI API
	|
	+--> BrokerService
	|      +--> TopicManager
	|      +--> GroupCoordinator
	|      +--> ClusterCoordinator
	|      +--> ConsumerOffsetStore
	|      +--> LogStore
	|
	+--> Local metadata + replica log storage
```

Supporting modules:

- `main.py` wires the app and service objects.
- `app/api/routes.py` exposes HTTP endpoints.
- `app/broker/service.py` coordinates topic, group, offset, and cluster behavior.
- `app/broker/topic_manager.py` stores topic metadata and validates partitions.
- `app/broker/consumer_offsets.py` persists committed offsets.
- `app/broker/group_coordinator.py` manages consumer membership and assignment.
- `app/broker/cluster_coordinator.py` manages brokers, replica sets, leaders, lag, and failover.
- `app/storage/log_store.py` stores segmented partition logs and retention state.
- `app/models/schemas.py` defines request and response models.
- `app/core/config.py` defines filesystem locations.
- `app/cli.py` provides an HTTP-based command-line client.

## Storage Model

ShardFlow stores metadata and log data locally on disk.

- Topic metadata: `data/topics.json`
- Consumer offsets: `data/consumer_offsets.json`
- Group metadata: `data/groups.json`
- Cluster metadata: `data/cluster.json`
- Partition logs: `data/<topic>/partition-<n>/segment-<start_offset>.log`
- Partition manifest: `data/<topic>/partition-<n>/manifest.json`
- Replica logs: `data/replicas/<broker_id>/...`

Each partition is backed by segmented append-only logs. Segments rotate after a configured record count, and old segments are pruned according to the retention policy. Reads remain offset-based and deterministic.

## Consumer Groups and Lag Tracking

Consumer group state is stored locally and coordinated by a lightweight group coordinator.

- Consumers register against a group and topic.
- Heartbeats refresh membership state.
- Stale members expire lazily when group state is accessed.
- Partition assignment is deterministic for the active member set.
- Rebalances occur when consumers join, leave, or expire.
- Committed offsets are stored per group, topic, and partition.
- Lag is calculated as `end_offset - committed_offset` with a lower bound of zero.

This keeps group behavior understandable while still showing the mechanics of consumer coordination.

## Replication and Failover Model

Replication is simulated locally and is intentionally simplified.

- Each partition has a leader broker and a replica set.
- Writes are accepted only by the current leader.
- Leader appends are copied to follower replica stores.
- Replica lag is tracked per broker and exposed through admin APIs.
- In-sync replicas are determined using a stable lag and freshness rule.
- Broker status changes and heartbeats affect leader eligibility.
- When a leader fails, the coordinator promotes the next eligible active follower.

This is a single-process simulation, not a distributed consensus system. It is useful for demos, testing, and systems discussion, but it is not production-grade multi-node replication.

## CLI Usage

The CLI uses the HTTP API and is intended for quick manual workflows.

```powershell
python -m app.cli --base-url http://127.0.0.1:8001 produce --topic orders --key user-1 --value '{"order_id":123,"status":"created"}'
python -m app.cli --base-url http://127.0.0.1:8001 consume --topic orders --partition 0 --offset 0 --limit 10
python -m app.cli --base-url http://127.0.0.1:8001 register-broker --broker-id broker-A
python -m app.cli --base-url http://127.0.0.1:8001 list-brokers
python -m app.cli --base-url http://127.0.0.1:8001 topic-cluster --topic orders
python -m app.cli --base-url http://127.0.0.1:8001 partition-cluster --topic orders --partition 0
python -m app.cli --base-url http://127.0.0.1:8001 trigger-failover --topic orders --partition 0
python -m app.cli --base-url http://127.0.0.1:8001 set-broker-status --broker-id broker-A --status failed
```

## API Examples

Create a topic:

```powershell
curl -Method Post http://127.0.0.1:8001/topics -ContentType "application/json" -Body '{"topic_name":"orders","partitions":3}'
```

Produce a message:

```powershell
curl -Method Post http://127.0.0.1:8001/produce -ContentType "application/json" -Body '{"topic_name":"orders","key":"user-1","value":{"order_id":123,"status":"created"}}'
```

Consume from offset 0:

```powershell
curl "http://127.0.0.1:8001/consume?topic_name=orders&partition=0&offset=0&limit=10"
```

Register a broker:

```powershell
curl -Method Post http://127.0.0.1:8001/cluster/brokers/register -ContentType "application/json" -Body '{"broker_id":"broker-A"}'
```

Inspect partition replication:

```powershell
curl http://127.0.0.1:8001/cluster/partitions/orders/0
```

Set broker status:

```powershell
curl -Method Post http://127.0.0.1:8001/cluster/brokers/broker-A/status -ContentType "application/json" -Body '{"status":"failed"}'
```

Trigger failover:

```powershell
curl -Method Post http://127.0.0.1:8001/cluster/failover/orders/0
```

## Running Tests

```powershell
pytest -q
```

The test suite covers topic creation, segmented logs, retention, offsets, lag, consumer groups, replication, broker state, failover, and CLI flows.

## Limitations

- The cluster model is simulated locally inside one process.
- Metadata is file-backed and intended for local development.
- There is no real distributed networking between brokers.
- There is no consensus protocol such as Raft or Paxos.
- This is not production-safe multi-node Kafka.
- Replication and failover are intentionally simplified.
- ISR and replica lag are heuristic and tuned for clarity rather than strict correctness.
