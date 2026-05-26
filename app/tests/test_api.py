from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from main import create_app


def client(tmp_path: Path, segment_max_records: int = 2, retention_segments: int = 10) -> TestClient:
    app = create_app(
        tmp_path,
        segment_max_records=segment_max_records,
        retention_segments=retention_segments,
    )
    return TestClient(app)


def test_create_topic(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 3},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["topic_name"] == "orders"
    assert body["partitions"] == 3
    assert body["next_partition"] == 0


def test_duplicate_topic_returns_409(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    first_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 2},
    )
    assert first_response.status_code == 201

    duplicate_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 2},
    )

    assert duplicate_response.status_code == 409


def test_unknown_topic_returns_404(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    response = test_client.get("/topics/missing")

    assert response.status_code == 404


def test_produce_unknown_topic_returns_404(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    response = test_client.post(
        "/produce",
        json={"topic_name": "missing", "value": {"x": 1}},
    )

    assert response.status_code == 404


def test_invalid_partition_returns_400(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    create_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 1},
    )
    assert create_response.status_code == 201

    response = test_client.get(
        "/consume",
        params={"topic_name": "orders", "partition": 5, "offset": 0, "limit": 10},
    )

    assert response.status_code == 400


def test_negative_offset_returns_validation_error(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    create_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 1},
    )
    assert create_response.status_code == 201

    response = test_client.get(
        "/consume",
        params={"topic_name": "orders", "partition": 0, "offset": -1, "limit": 10},
    )

    assert response.status_code == 422


def test_consume_empty_partition_returns_empty_list(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    create_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 1},
    )
    assert create_response.status_code == 201

    response = test_client.get(
        "/consume",
        params={"topic_name": "orders", "partition": 0, "offset": 0, "limit": 10},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["messages"] == []
    assert body["start_offset"] == 0
    assert body["next_offset"] == 0


def test_commit_invalid_partition_returns_400(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    create_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 1},
    )
    assert create_response.status_code == 201

    response = test_client.post(
        "/commit",
        json={
            "group_id": "billing-service",
            "topic_name": "orders",
            "partition": 9,
            "offset": 1,
        },
    )

    assert response.status_code == 400


def test_health_endpoint_returns_ok(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    response = test_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "mini-kafka", "version": "0.1.0"}


def test_produce_and_consume_from_zero(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    create_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 2},
    )
    assert create_response.status_code == 201

    produce_response = test_client.post(
        "/produce",
        json={"topic_name": "orders", "value": {"order_id": 123, "status": "created"}},
    )

    assert produce_response.status_code == 201
    produced = produce_response.json()
    assert produced["offset"] == 0
    assert produced["partition"] == 0

    consume_response = test_client.get(
        "/consume",
        params={"topic_name": "orders", "partition": 0, "offset": 0, "limit": 10},
    )

    assert consume_response.status_code == 200
    consumed = consume_response.json()
    assert consumed["start_offset"] == 0
    assert consumed["next_offset"] == 1
    assert len(consumed["messages"]) == 1
    assert consumed["messages"][0]["offset"] == 0
    assert consumed["messages"][0]["value"] == {"order_id": 123, "status": "created"}


def test_segmented_reads_across_multiple_segments(tmp_path: Path) -> None:
    test_client = client(tmp_path, segment_max_records=2, retention_segments=10)

    create_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 1},
    )
    assert create_response.status_code == 201

    for index in range(5):
        produce_response = test_client.post(
            "/produce",
            json={"topic_name": "orders", "value": {"n": index}},
        )
        assert produce_response.status_code == 201

    consume_response = test_client.get(
        "/consume",
        params={"topic_name": "orders", "partition": 0, "offset": 0, "limit": 10},
    )

    assert consume_response.status_code == 200
    body = consume_response.json()
    assert [message["offset"] for message in body["messages"]] == [0, 1, 2, 3, 4]

    partition_stats_response = test_client.get("/topics/orders/partitions/0/stats")
    assert partition_stats_response.status_code == 200
    partition_stats = partition_stats_response.json()
    assert partition_stats["end_offset"] == 5
    assert partition_stats["segment_count"] >= 3


def test_retention_prunes_old_segments(tmp_path: Path) -> None:
    test_client = client(tmp_path, segment_max_records=2, retention_segments=2)

    create_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 1},
    )
    assert create_response.status_code == 201

    for index in range(5):
        produce_response = test_client.post(
            "/produce",
            json={"topic_name": "orders", "value": {"n": index}},
        )
        assert produce_response.status_code == 201

    stats_response = test_client.get("/topics/orders/stats")
    assert stats_response.status_code == 200
    stats = stats_response.json()
    partition_stats = stats["partition_stats"][0]
    assert partition_stats["segment_count"] == 2
    assert partition_stats["earliest_offset"] == 2

    consume_response = test_client.get(
        "/consume",
        params={"topic_name": "orders", "partition": 0, "offset": 0, "limit": 10},
    )
    assert consume_response.status_code == 200
    body = consume_response.json()
    assert [message["offset"] for message in body["messages"]] == [2, 3, 4]


def test_commit_and_fetch_offsets(tmp_path: Path) -> None:
    test_client = client(tmp_path)

    create_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 1},
    )
    assert create_response.status_code == 201

    commit_response = test_client.post(
        "/commit",
        json={
            "group_id": "billing-service",
            "topic_name": "orders",
            "partition": 0,
            "offset": 7,
        },
    )

    assert commit_response.status_code == 200
    assert commit_response.json()["offset"] == 7

    offset_response = test_client.get("/offsets/billing-service/orders/0")

    assert offset_response.status_code == 200
    assert offset_response.json()["offset"] == 7


def test_end_offset_and_consumer_lag(tmp_path: Path) -> None:
    test_client = client(tmp_path, segment_max_records=2, retention_segments=10)

    create_response = test_client.post(
        "/topics",
        json={"topic_name": "orders", "partitions": 1},
    )
    assert create_response.status_code == 201

    for index in range(5):
        produce_response = test_client.post(
            "/produce",
            json={"topic_name": "orders", "value": {"n": index}},
        )
        assert produce_response.status_code == 201

    end_offset_response = test_client.get("/topics/orders/partitions/0/end-offset")
    assert end_offset_response.status_code == 200
    assert end_offset_response.json()["end_offset"] == 5

    commit_response = test_client.post(
        "/commit",
        json={
            "group_id": "billing-service",
            "topic_name": "orders",
            "partition": 0,
            "offset": 2,
        },
    )
    assert commit_response.status_code == 200

    lag_response = test_client.get("/lag/billing-service/orders/0")
    assert lag_response.status_code == 200
    lag = lag_response.json()
    assert lag["end_offset"] == 5
    assert lag["committed_offset"] == 2
    assert lag["lag"] == 3

    topic_lag_response = test_client.get("/lag/billing-service/orders")
    assert topic_lag_response.status_code == 200
    topic_lag = topic_lag_response.json()
    assert topic_lag["total_lag"] == 3
    assert topic_lag["partitions"][0]["lag"] == 3
