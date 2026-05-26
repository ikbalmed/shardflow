from __future__ import annotations
from datetime import datetime
from typing import Any
from pydantic import BaseModel, ConfigDict, Field
from typing import Literal


TOPIC_NAME_PATTERN = r"^[A-Za-z0-9._-]+$"


class CreateTopicRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_name: str = Field(min_length=1, max_length=255, pattern=TOPIC_NAME_PATTERN)
    partitions: int = Field(ge=1, le=128)


class TopicInfoResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_name: str
    partitions: int
    created_at: datetime
    next_partition: int


class ProduceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_name: str = Field(min_length=1, max_length=255, pattern=TOPIC_NAME_PATTERN)
    key: str | None = Field(default=None, min_length=1, max_length=255)
    value: Any


class ProduceResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_name: str
    partition: int
    offset: int
    timestamp: datetime
    key: str | None = None


class MessageRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    offset: int
    key: str | None = None
    value: Any
    timestamp: datetime


class ConsumeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_name: str
    partition: int
    start_offset: int
    next_offset: int
    messages: list[MessageRecord]


class SegmentStatsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    file_name: str
    start_offset: int
    end_offset: int
    record_count: int
    size_bytes: int


class PartitionStatsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_name: str
    partition: int
    earliest_offset: int
    end_offset: int
    record_count: int
    segment_count: int
    size_bytes: int
    segments: list[SegmentStatsResponse]


class TopicStatsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_name: str
    partitions: int
    total_messages: int
    total_segments: int
    total_size_bytes: int
    partition_stats: list[PartitionStatsResponse]


class EndOffsetResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_name: str
    partition: int
    earliest_offset: int
    end_offset: int


class PartitionLagResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    partition: int
    committed_offset: int
    end_offset: int
    lag: int


class ConsumerLagResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str
    topic_name: str
    total_lag: int
    partitions: list[PartitionLagResponse]


class CommitOffsetRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str = Field(min_length=1, max_length=255)
    topic_name: str = Field(min_length=1, max_length=255, pattern=TOPIC_NAME_PATTERN)
    partition: int = Field(ge=0)
    offset: int = Field(ge=0)


class OffsetResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str
    topic_name: str
    partition: int
    offset: int


class GroupRegisterRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    consumer_id: str = Field(min_length=1, max_length=255)
    topic_name: str = Field(min_length=1, max_length=255, pattern=TOPIC_NAME_PATTERN)


class HeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    consumer_id: str = Field(min_length=1, max_length=255)


class LeaveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    consumer_id: str = Field(min_length=1, max_length=255)


class ConsumerInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    consumer_id: str
    topics: list[str]
    last_seen: float


class PartitionAssignment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    partition: int
    consumer_id: str


class AssignmentsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str
    topic_name: str
    assignments: list[PartitionAssignment]


class GroupStateResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str
    members: list[ConsumerInfo]
    assignments: dict[str, dict[int, str]]


class BrokerRegisterRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    broker_id: str = Field(min_length=1, max_length=255)


class BrokerInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    broker_id: str
    status: str
    registered_at: float
    last_heartbeat_at: float


class BrokerStatusRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["active", "failed"]


class BrokerStatusChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str
    partition: int
    old_leader: str | None
    new_leader: str | None


class BrokerStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    broker: BrokerInfo
    affected: list[BrokerStatusChange]


class PartitionReplicationInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_name: str
    partition: int
    leader: str | None
    replicas: dict[str, int] | list[str]
    in_sync_replicas: list[str]
    leader_epoch: int
    replica_lags: dict[str, int]
