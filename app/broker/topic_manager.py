from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from app.broker.partitioner import Partitioner
from app.models.schemas import CreateTopicRequest, TopicInfoResponse


class TopicError(Exception):
    pass


class TopicNotFoundError(TopicError):
    pass


class TopicAlreadyExistsError(TopicError):
    pass


class InvalidPartitionError(TopicError):
    pass


@dataclass(slots=True, frozen=True)
class TopicRecord:
    topic_name: str
    partitions: int
    created_at: datetime
    next_partition: int


class TopicManager:
    def __init__(self, metadata_file: Path, data_dir: Path, partitioner: Partitioner) -> None:
        self._metadata_file = metadata_file
        self._data_dir = data_dir
        self._partitioner = partitioner
        self._lock = Lock()
        self._metadata_file.parent.mkdir(parents=True, exist_ok=True)

    def create_topic(self, request: CreateTopicRequest) -> TopicInfoResponse:
        with self._lock:
            metadata = self._load_metadata()
            if request.topic_name in metadata:
                raise TopicAlreadyExistsError(f"Topic '{request.topic_name}' already exists")

            created_at = datetime.now(timezone.utc)
            metadata[request.topic_name] = TopicRecord(
                topic_name=request.topic_name,
                partitions=request.partitions,
                created_at=created_at,
                next_partition=0,
            )
            self._ensure_topic_files(request.topic_name, request.partitions)
            self._save_metadata(metadata)
            return self._to_response(metadata[request.topic_name])

    def get_topic(self, topic_name: str) -> TopicInfoResponse:
        with self._lock:
            metadata = self._load_metadata()
            record = metadata.get(topic_name)
            if record is None:
                raise TopicNotFoundError(f"Topic '{topic_name}' was not found")
            return self._to_response(record)

    def resolve_partition(self, topic_name: str, key: str | None = None) -> int:
        with self._lock:
            metadata = self._load_metadata()
            record = metadata.get(topic_name)
            if record is None:
                raise TopicNotFoundError(f"Topic '{topic_name}' was not found")

            if key is not None:
                return self._partitioner.partition_for_key(key, record.partitions)

            partition, next_partition = self._partitioner.next_round_robin(
                record.next_partition,
                record.partitions,
            )
            metadata[topic_name] = TopicRecord(
                topic_name=record.topic_name,
                partitions=record.partitions,
                created_at=record.created_at,
                next_partition=next_partition,
            )
            self._save_metadata(metadata)
            return partition

    def validate_partition(self, topic_name: str, partition: int) -> TopicRecord:
        with self._lock:
            metadata = self._load_metadata()
            record = metadata.get(topic_name)
            if record is None:
                raise TopicNotFoundError(f"Topic '{topic_name}' was not found")

            if partition < 0 or partition >= record.partitions:
                raise InvalidPartitionError(
                    f"Partition {partition} is invalid for topic '{topic_name}'"
                )

            return record

    def partition_count(self, topic_name: str) -> int:
        return self.get_topic(topic_name).partitions

    def topic_directory(self, topic_name: str) -> Path:
        return self._data_dir / topic_name

    def partition_log_path(self, topic_name: str, partition: int) -> Path:
        return self.partition_directory(topic_name, partition) / "legacy.log"

    def partition_directory(self, topic_name: str, partition: int) -> Path:
        return self.topic_directory(topic_name) / f"partition-{partition}"

    def _ensure_topic_files(self, topic_name: str, partitions: int) -> None:
        topic_dir = self.topic_directory(topic_name)
        topic_dir.mkdir(parents=True, exist_ok=True)
        for partition in range(partitions):
            self.partition_directory(topic_name, partition).mkdir(parents=True, exist_ok=True)

    def _load_metadata(self) -> dict[str, TopicRecord]:
        if not self._metadata_file.exists():
            return {}

        raw = json.loads(self._metadata_file.read_text(encoding="utf-8"))
        topics: dict[str, TopicRecord] = {}
        for topic_name, data in raw.get("topics", {}).items():
            topics[topic_name] = TopicRecord(
                topic_name=topic_name,
                partitions=int(data["partitions"]),
                created_at=datetime.fromisoformat(data["created_at"]),
                next_partition=int(data.get("next_partition", 0)),
            )
        return topics

    def _save_metadata(self, metadata: dict[str, TopicRecord]) -> None:
        payload = {
            "topics": {
                topic_name: {
                    "partitions": record.partitions,
                    "created_at": record.created_at.isoformat(),
                    "next_partition": record.next_partition,
                }
                for topic_name, record in metadata.items()
            }
        }
        self._metadata_file.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _to_response(self, record: TopicRecord) -> TopicInfoResponse:
        return TopicInfoResponse(
            topic_name=record.topic_name,
            partitions=record.partitions,
            created_at=record.created_at,
            next_partition=record.next_partition,
        )
