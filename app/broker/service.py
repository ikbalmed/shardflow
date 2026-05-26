from __future__ import annotations

from dataclasses import dataclass

from app.broker.consumer_offsets import ConsumerOffsetStore
from app.broker.topic_manager import InvalidPartitionError, TopicManager
from app.broker.group_coordinator import GroupCoordinator
from app.broker.cluster_coordinator import ClusterCoordinator, ClusterError, NotLeaderError
from app.models.schemas import (
    ConsumerLagResponse,
    CommitOffsetRequest,
    ConsumeResponse,
    EndOffsetResponse,
    CreateTopicRequest,
    PartitionLagResponse,
    PartitionStatsResponse,
    OffsetResponse,
    ProduceRequest,
    ProduceResponse,
    SegmentStatsResponse,
    TopicStatsResponse,
    TopicInfoResponse,
)
from app.storage.log_store import LogStore


@dataclass(slots=True)
class BrokerService:
    topic_manager: TopicManager
    log_store: LogStore
    offset_store: ConsumerOffsetStore
    group_coordinator: GroupCoordinator | None = None
    cluster_coordinator: ClusterCoordinator | None = None
    local_broker_id: str | None = None

    def create_topic(self, request: CreateTopicRequest) -> TopicInfoResponse:
        info = self.topic_manager.create_topic(request)
        # inform cluster coordinator to assign replicas if present
        if self.cluster_coordinator is not None:
            try:
                self.cluster_coordinator.assign_replicas_for_topic(request.topic_name, request.partitions)
            except Exception:
                pass
        return info

    def get_topic(self, topic_name: str) -> TopicInfoResponse:
        return self.topic_manager.get_topic(topic_name)

    def produce(self, request: ProduceRequest) -> ProduceResponse:
        partition = self.topic_manager.resolve_partition(request.topic_name, request.key)
        # if cluster coordinator is configured, enforce leader-only writes
        if self.cluster_coordinator is not None and self.local_broker_id is not None:
            leader = self.cluster_coordinator.leader_for(request.topic_name, partition)
            if leader is None:
                raise ClusterError("no leader for partition")
            if leader != self.local_broker_id:
                raise NotLeaderError(f"broker {self.local_broker_id} is not leader for partition {partition}")

        record = self.log_store.append_message(request.topic_name, partition, request.key, request.value)
        # update cluster coordinator about leader append and replicate to followers
        if self.cluster_coordinator is not None:
            try:
                self.cluster_coordinator.note_leader_append(request.topic_name, partition, record.offset)
                # replicate asynchronously simulated (synchronously here)
                self.cluster_coordinator.replicate_record(request.topic_name, partition, record.model_dump())
            except Exception:
                pass
        return ProduceResponse(
            topic_name=request.topic_name,
            partition=partition,
            offset=record.offset,
            timestamp=record.timestamp,
            key=record.key,
        )

    def consume(
        self,
        topic_name: str,
        partition: int,
        offset: int,
        limit: int,
    ) -> ConsumeResponse:
        self.topic_manager.validate_partition(topic_name, partition)
        records = self.log_store.read_from(topic_name, partition, offset, limit)
        next_offset = records[-1].offset + 1 if records else offset
        return ConsumeResponse(
            topic_name=topic_name,
            partition=partition,
            start_offset=offset,
            next_offset=next_offset,
            messages=records,
        )

    def commit_offset(self, request: CommitOffsetRequest) -> OffsetResponse:
        self.topic_manager.validate_partition(request.topic_name, request.partition)
        return self.offset_store.commit_offset(
            group_id=request.group_id,
            topic_name=request.topic_name,
            partition=request.partition,
            offset=request.offset,
        )

    def get_committed_offset(self, group_id: str, topic_name: str, partition: int) -> OffsetResponse:
        self.topic_manager.validate_partition(topic_name, partition)
        return self.offset_store.get_offset(group_id, topic_name, partition)

    def get_end_offset(self, topic_name: str, partition: int) -> EndOffsetResponse:
        self.topic_manager.validate_partition(topic_name, partition)
        stats = self.log_store.partition_stats(topic_name, partition)
        return EndOffsetResponse(
            topic_name=topic_name,
            partition=partition,
            earliest_offset=stats.earliest_offset,
            end_offset=stats.end_offset,
        )

    def get_partition_stats(self, topic_name: str, partition: int) -> PartitionStatsResponse:
        self.topic_manager.validate_partition(topic_name, partition)
        return self._partition_stats_response(self.log_store.partition_stats(topic_name, partition))

    def get_topic_stats(self, topic_name: str) -> TopicStatsResponse:
        partition_count = self.topic_manager.partition_count(topic_name)
        partition_stats = self.log_store.topic_stats(topic_name, partition_count)
        partition_responses = [self._partition_stats_response(stats) for stats in partition_stats]
        return TopicStatsResponse(
            topic_name=topic_name,
            partitions=partition_count,
            total_messages=sum(stats.record_count for stats in partition_stats),
            total_segments=sum(stats.segment_count for stats in partition_stats),
            total_size_bytes=sum(stats.size_bytes for stats in partition_stats),
            partition_stats=partition_responses,
        )

    def get_consumer_lag(self, group_id: str, topic_name: str) -> ConsumerLagResponse:
        partition_count = self.topic_manager.partition_count(topic_name)
        partition_stats = self.log_store.topic_stats(topic_name, partition_count)
        committed_offsets = self.offset_store.get_topic_offsets(group_id, topic_name, partition_count)
        partitions: list[PartitionLagResponse] = []
        total_lag = 0
        for stats in partition_stats:
            committed_offset = committed_offsets[stats.partition]
            lag = max(0, stats.end_offset - committed_offset)
            total_lag += lag
            partitions.append(
                PartitionLagResponse(
                    partition=stats.partition,
                    committed_offset=committed_offset,
                    end_offset=stats.end_offset,
                    lag=lag,
                )
            )
        return ConsumerLagResponse(
            group_id=group_id,
            topic_name=topic_name,
            total_lag=total_lag,
            partitions=partitions,
        )

    def get_partition_lag(self, group_id: str, topic_name: str, partition: int) -> PartitionLagResponse:
        self.topic_manager.validate_partition(topic_name, partition)
        end_offset = self.log_store.partition_end_offset(topic_name, partition)
        committed_offset = self.offset_store.get_offset(group_id, topic_name, partition).offset
        lag = max(0, end_offset - committed_offset)
        return PartitionLagResponse(
            partition=partition,
            committed_offset=committed_offset,
            end_offset=end_offset,
            lag=lag,
        )

    # Group coordination wrappers
    def register_consumer(self, group_id: str, consumer_id: str, topic_name: str) -> dict:
        if not self.group_coordinator:
            raise RuntimeError("Group coordinator not configured")
        return self.group_coordinator.register_consumer(group_id, consumer_id, topic_name)

    def heartbeat_consumer(self, group_id: str, consumer_id: str) -> dict:
        if not self.group_coordinator:
            raise RuntimeError("Group coordinator not configured")
        return self.group_coordinator.heartbeat(group_id, consumer_id)

    def unregister_consumer(self, group_id: str, consumer_id: str) -> dict:
        if not self.group_coordinator:
            raise RuntimeError("Group coordinator not configured")
        return self.group_coordinator.unregister_consumer(group_id, consumer_id)

    def get_group_state(self, group_id: str) -> dict | None:
        if not self.group_coordinator:
            raise RuntimeError("Group coordinator not configured")
        return self.group_coordinator.get_group_state(group_id)

    def get_assignments(self, group_id: str) -> dict | None:
        if not self.group_coordinator:
            raise RuntimeError("Group coordinator not configured")
        return self.group_coordinator.get_assignments(group_id)

    def _partition_stats_response(self, stats) -> PartitionStatsResponse:
        return PartitionStatsResponse(
            topic_name=stats.topic_name,
            partition=stats.partition,
            earliest_offset=stats.earliest_offset,
            end_offset=stats.end_offset,
            record_count=stats.record_count,
            segment_count=stats.segment_count,
            size_bytes=stats.size_bytes,
            segments=[
                SegmentStatsResponse(
                    file_name=segment.file_name,
                    start_offset=segment.start_offset,
                    end_offset=segment.end_offset,
                    record_count=segment.record_count,
                    size_bytes=segment.size_bytes,
                )
                for segment in stats.segments
            ],
        )


__all__ = ["BrokerService"]
