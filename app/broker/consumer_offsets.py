from __future__ import annotations

import json
from pathlib import Path
from threading import Lock

from app.models.schemas import OffsetResponse
from app.storage.log_store import InvalidOffsetError


class ConsumerOffsetStore:
    def __init__(self, offsets_file: Path) -> None:
        self._offsets_file = offsets_file
        self._lock = Lock()
        self._offsets_file.parent.mkdir(parents=True, exist_ok=True)

    def commit_offset(self, group_id: str, topic_name: str, partition: int, offset: int) -> OffsetResponse:
        if offset < 0:
            raise InvalidOffsetError("offset must be non-negative")

        with self._lock:
            data = self._load_offsets()
            groups = data.setdefault("groups", {})
            group_entry = groups.setdefault(group_id, {})
            topic_entry = group_entry.setdefault(topic_name, {})
            topic_entry[str(partition)] = offset
            self._save_offsets(data)

        return OffsetResponse(
            group_id=group_id,
            topic_name=topic_name,
            partition=partition,
            offset=offset,
        )

    def get_offset(self, group_id: str, topic_name: str, partition: int) -> OffsetResponse:
        with self._lock:
            data = self._load_offsets()
            offset = (
                data.get("groups", {})
                .get(group_id, {})
                .get(topic_name, {})
                .get(str(partition), 0)
            )

        return OffsetResponse(
            group_id=group_id,
            topic_name=topic_name,
            partition=partition,
            offset=int(offset),
        )

    def get_topic_offsets(self, group_id: str, topic_name: str, partitions: int) -> dict[int, int]:
        with self._lock:
            data = self._load_offsets()
            topic_offsets = data.get("groups", {}).get(group_id, {}).get(topic_name, {})
            return {partition: int(topic_offsets.get(str(partition), 0)) for partition in range(partitions)}

    def _load_offsets(self) -> dict:
        if not self._offsets_file.exists():
            return {"groups": {}}

        return json.loads(self._offsets_file.read_text(encoding="utf-8"))

    def _save_offsets(self, data: dict) -> None:
        self._offsets_file.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
