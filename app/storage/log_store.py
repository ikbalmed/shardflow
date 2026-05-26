from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from app.models.schemas import MessageRecord


class LogStoreError(Exception):
    pass


class InvalidOffsetError(LogStoreError):
    pass


class PartitionLogMissingError(LogStoreError):
    pass


@dataclass(slots=True)
class SegmentStats:
    file_name: str
    start_offset: int
    end_offset: int
    record_count: int
    size_bytes: int


@dataclass(slots=True)
class PartitionStats:
    topic_name: str
    partition: int
    earliest_offset: int
    end_offset: int
    record_count: int
    segment_count: int
    size_bytes: int
    segments: list[SegmentStats]


@dataclass(slots=True)
class PartitionState:
    next_offset: int
    segments: list[SegmentStats]


class LogStore:
    def __init__(self, data_dir: Path, max_records_per_segment: int = 3, retention_segments: int = 4) -> None:
        self._data_dir = data_dir
        self._max_records_per_segment = max_records_per_segment
        self._retention_segments = retention_segments
        self._lock = Lock()

    def append_message(self, topic_name: str, partition: int, key: str | None, value) -> MessageRecord:
        if partition < 0:
            raise InvalidOffsetError("partition must be non-negative")

        with self._lock:
            state = self._load_partition_state(topic_name, partition)
            segment = self._active_segment(topic_name, partition, state)
            if segment.record_count >= self._max_records_per_segment:
                segment = self._create_segment(topic_name, partition, state.next_offset)
                state.segments.append(segment)

            record = MessageRecord(
                offset=state.next_offset,
                key=key,
                value=value,
                timestamp=datetime.now(timezone.utc),
            )
            segment_path = self._segment_path(topic_name, partition, segment.start_offset)
            segment_path.parent.mkdir(parents=True, exist_ok=True)
            with segment_path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(record.model_dump_json())
                handle.write("\n")

            segment.record_count += 1
            segment.end_offset = record.offset + 1
            segment.size_bytes = segment_path.stat().st_size
            state.next_offset += 1
            self._save_partition_state(topic_name, partition, state)
            self._apply_retention(topic_name, partition, state)
            return record

    def read_from(
        self,
        topic_name: str,
        partition: int,
        offset: int,
        limit: int,
    ) -> list[MessageRecord]:
        if offset < 0:
            raise InvalidOffsetError("offset must be non-negative")
        if limit < 1:
            return []

        with self._lock:
            state = self._load_partition_state(topic_name, partition)

        earliest_offset = state.segments[0].start_offset if state.segments else state.next_offset
        if offset < earliest_offset:
            offset = earliest_offset

        records: list[MessageRecord] = []
        for segment in state.segments:
            segment_path = self._segment_path(topic_name, partition, segment.start_offset)
            if not segment_path.exists():
                raise LogStoreError(f"Missing segment file {segment_path}")

            with segment_path.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue

                    try:
                        record = MessageRecord.model_validate_json(line)
                    except Exception as exc:  # pragma: no cover - defensive guard
                        raise LogStoreError(f"Malformed log line in {segment_path}") from exc
                    if record.offset >= offset:
                        records.append(record)
                        if len(records) >= limit:
                            return records
        return records

    def partition_end_offset(self, topic_name: str, partition: int) -> int:
        with self._lock:
            state = self._load_partition_state(topic_name, partition)
            return state.next_offset

    def partition_stats(self, topic_name: str, partition: int) -> PartitionStats:
        with self._lock:
            state = self._load_partition_state(topic_name, partition)

        earliest_offset = state.segments[0].start_offset if state.segments else 0
        size_bytes = sum(segment.size_bytes for segment in state.segments)
        record_count = sum(segment.record_count for segment in state.segments)
        return PartitionStats(
            topic_name=topic_name,
            partition=partition,
            earliest_offset=earliest_offset,
            end_offset=state.next_offset,
            record_count=record_count,
            segment_count=len(state.segments),
            size_bytes=size_bytes,
            segments=list(state.segments),
        )

    def topic_stats(self, topic_name: str, partitions: int) -> list[PartitionStats]:
        return [self.partition_stats(topic_name, partition) for partition in range(partitions)]

    def _load_partition_state(self, topic_name: str, partition: int) -> PartitionState:
        manifest_path = self._manifest_path(topic_name, partition)
        if manifest_path.exists():
            raw = json.loads(manifest_path.read_text(encoding="utf-8"))
            segments = [SegmentStats(**segment) for segment in raw.get("segments", [])]
            return PartitionState(next_offset=int(raw.get("next_offset", 0)), segments=segments)

        legacy_source = self._legacy_source_path(topic_name, partition)
        if legacy_source is not None:
            return self._migrate_legacy_partition(topic_name, partition, legacy_source)

        return PartitionState(next_offset=0, segments=[])

    def _migrate_legacy_partition(self, topic_name: str, partition: int, legacy_source: Path) -> PartitionState:
        records: list[MessageRecord] = []
        with legacy_source.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    record = MessageRecord.model_validate_json(line)
                except Exception as exc:  # pragma: no cover - defensive guard
                    raise LogStoreError(f"Malformed legacy log line in {legacy_source}") from exc
                records.append(record)

        state = PartitionState(next_offset=0, segments=[])
        for record in records:
            if not state.segments or state.segments[-1].record_count >= self._max_records_per_segment:
                segment = self._create_segment(topic_name, partition, record.offset)
                state.segments.append(segment)
            segment = state.segments[-1]
            segment_path = self._segment_path(topic_name, partition, segment.start_offset)
            with segment_path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(record.model_dump_json())
                handle.write("\n")
            segment.record_count += 1
            segment.end_offset = record.offset + 1
            segment.size_bytes = segment_path.stat().st_size
            state.next_offset = record.offset + 1

        if legacy_source.exists():
            migrated_path = self._migrated_flat_path(topic_name, partition)
            migrated_path.parent.mkdir(parents=True, exist_ok=True)
            legacy_source.replace(migrated_path)

        self._save_partition_state(topic_name, partition, state)
        return state

    def _apply_retention(self, topic_name: str, partition: int, state: PartitionState) -> None:
        while len(state.segments) > self._retention_segments:
            oldest = state.segments.pop(0)
            oldest_path = self._segment_path(topic_name, partition, oldest.start_offset)
            if oldest_path.exists():
                oldest_path.unlink()
        self._save_partition_state(topic_name, partition, state)

    def _active_segment(self, topic_name: str, partition: int, state: PartitionState) -> SegmentStats:
        if state.segments:
            return state.segments[-1]
        segment = self._create_segment(topic_name, partition, state.next_offset)
        state.segments.append(segment)
        return segment

    def _create_segment(self, topic_name: str, partition: int, start_offset: int) -> SegmentStats:
        segment = SegmentStats(
            file_name=self._segment_file_name(start_offset),
            start_offset=start_offset,
            end_offset=start_offset,
            record_count=0,
            size_bytes=0,
        )
        segment_path = self._segment_path(topic_name, partition, start_offset)
        segment_path.parent.mkdir(parents=True, exist_ok=True)
        segment_path.touch(exist_ok=True)
        return segment

    def _save_partition_state(self, topic_name: str, partition: int, state: PartitionState) -> None:
        manifest_path = self._manifest_path(topic_name, partition)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "next_offset": state.next_offset,
            "segments": [asdict(segment) for segment in state.segments],
        }
        manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def _legacy_source_path(self, topic_name: str, partition: int) -> Path | None:
        legacy_flat = self._legacy_flat_path(topic_name, partition)
        if legacy_flat.exists():
            return legacy_flat
        return None

    def _manifest_path(self, topic_name: str, partition: int) -> Path:
        return self._partition_directory(topic_name, partition) / "manifest.json"

    def _partition_directory(self, topic_name: str, partition: int) -> Path:
        return self._data_dir / topic_name / f"partition-{partition}"

    def _segment_path(self, topic_name: str, partition: int, start_offset: int) -> Path:
        return self._partition_directory(topic_name, partition) / self._segment_file_name(start_offset)

    def _segment_file_name(self, start_offset: int) -> str:
        return f"segment-{start_offset:020d}.log"

    def _legacy_flat_path(self, topic_name: str, partition: int) -> Path:
        return self._data_dir / topic_name / f"partition-{partition}.log"

    def _migrated_flat_path(self, topic_name: str, partition: int) -> Path:
        return self._data_dir / topic_name / f"partition-{partition}.log.legacy"
