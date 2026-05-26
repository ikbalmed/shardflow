from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request, status

from app.broker.service import BrokerService
from app.broker.topic_manager import InvalidPartitionError, TopicAlreadyExistsError, TopicError, TopicNotFoundError
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
    TopicStatsResponse,
    TopicInfoResponse,
    GroupRegisterRequest,
    HeartbeatRequest,
    LeaveRequest,
    AssignmentsResponse,
    GroupStateResponse,
    PartitionAssignment,
)
from app.storage.log_store import InvalidOffsetError, LogStoreError, PartitionLogMissingError
from app.broker.cluster_coordinator import ClusterError, BrokerNotRegisteredError, NotLeaderError
from app.models.schemas import BrokerRegisterRequest, BrokerInfo, PartitionReplicationInfo
from app.models.schemas import BrokerStatusRequest, BrokerStatusResponse, BrokerStatusChange


router = APIRouter(tags=["broker"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "mini-kafka", "version": "0.1.0"}


def _service(request: Request) -> BrokerService:
    return request.app.state.broker_service


def _map_error(exc: Exception) -> HTTPException:
    if isinstance(exc, TopicAlreadyExistsError):
        return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    if isinstance(exc, TopicNotFoundError):
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if isinstance(exc, InvalidPartitionError):
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    if isinstance(exc, InvalidOffsetError):
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    if isinstance(exc, PartitionLogMissingError):
        return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
    if isinstance(exc, TopicError):
        return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
    if isinstance(exc, LogStoreError):
        return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
    if isinstance(exc, BrokerNotRegisteredError):
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if isinstance(exc, NotLeaderError):
        return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    if isinstance(exc, ClusterError):
        return HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))
    return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unexpected broker error")


@router.post("/topics", response_model=TopicInfoResponse, status_code=status.HTTP_201_CREATED)
async def create_topic(payload: CreateTopicRequest, request: Request) -> TopicInfoResponse:
    try:
        return _service(request).create_topic(payload)
    except Exception as exc:  # pragma: no cover - translated into HTTP response
        raise _map_error(exc) from exc


@router.get("/topics/{topic_name}", response_model=TopicInfoResponse)
async def get_topic(topic_name: str, request: Request) -> TopicInfoResponse:
    try:
        return _service(request).get_topic(topic_name)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/topics/{topic_name}/stats", response_model=TopicStatsResponse)
async def topic_stats(topic_name: str, request: Request) -> TopicStatsResponse:
    try:
        return _service(request).get_topic_stats(topic_name)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/topics/{topic_name}/partitions/{partition}/stats", response_model=PartitionStatsResponse)
async def partition_stats(topic_name: str, partition: int, request: Request) -> PartitionStatsResponse:
    try:
        return _service(request).get_partition_stats(topic_name, partition)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/topics/{topic_name}/partitions/{partition}/end-offset", response_model=EndOffsetResponse)
async def end_offset(topic_name: str, partition: int, request: Request) -> EndOffsetResponse:
    try:
        return _service(request).get_end_offset(topic_name, partition)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/produce", response_model=ProduceResponse, status_code=status.HTTP_201_CREATED)
async def produce(payload: ProduceRequest, request: Request) -> ProduceResponse:
    try:
        return _service(request).produce(payload)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/consume", response_model=ConsumeResponse)
async def consume(
    request: Request,
    topic_name: str = Query(..., min_length=1),
    partition: int = Query(..., ge=0),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
) -> ConsumeResponse:
    try:
        return _service(request).consume(topic_name, partition, offset, limit)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/commit", response_model=OffsetResponse)
async def commit_offset(payload: CommitOffsetRequest, request: Request) -> OffsetResponse:
    try:
        return _service(request).commit_offset(payload)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/offsets/{group_id}/{topic_name}/{partition}", response_model=OffsetResponse)
async def get_offset(group_id: str, topic_name: str, partition: int, request: Request) -> OffsetResponse:
    try:
        return _service(request).get_committed_offset(group_id, topic_name, partition)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/lag/{group_id}/{topic_name}", response_model=ConsumerLagResponse)
async def consumer_lag(group_id: str, topic_name: str, request: Request) -> ConsumerLagResponse:
    try:
        return _service(request).get_consumer_lag(group_id, topic_name)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/lag/{group_id}/{topic_name}/{partition}", response_model=PartitionLagResponse)
async def partition_lag(group_id: str, topic_name: str, partition: int, request: Request) -> PartitionLagResponse:
    try:
        return _service(request).get_partition_lag(group_id, topic_name, partition)
    except Exception as exc:
        raise _map_error(exc) from exc


# Group coordination endpoints
@router.post("/groups/{group_id}/register", response_model=AssignmentsResponse)
async def register_group_consumer(group_id: str, payload: GroupRegisterRequest, request: Request) -> AssignmentsResponse:
    try:
        result = _service(request).register_consumer(group_id, payload.consumer_id, payload.topic_name)
        assignments = result.get("assignments", {})
        topic_assign = assignments.get(payload.topic_name, {})
        as_list = [PartitionAssignment(partition=int(p), consumer_id=cid) for p, cid in topic_assign.items()]
        return AssignmentsResponse(group_id=group_id, topic_name=payload.topic_name, assignments=as_list)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/groups/{group_id}/heartbeat")
async def heartbeat_group_consumer(group_id: str, payload: HeartbeatRequest, request: Request) -> dict:
    try:
        return _service(request).heartbeat_consumer(group_id, payload.consumer_id)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/groups/{group_id}/leave")
async def leave_group_consumer(group_id: str, payload: LeaveRequest, request: Request) -> dict:
    try:
        return _service(request).unregister_consumer(group_id, payload.consumer_id)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/groups/{group_id}", response_model=GroupStateResponse)
async def get_group_state(group_id: str, request: Request) -> GroupStateResponse:
    try:
        state = _service(request).get_group_state(group_id)
        if state is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="group not found")
        members = []
        for cid, info in state.get("members", {}).items():
            members.append(PartitionAssignment)  # placeholder to satisfy type
        # rebuild proper members list
        members = [
            {"consumer_id": cid, "topics": info.get("topics", []), "last_seen": info.get("last_seen", 0)}
            for cid, info in state.get("members", {}).items()
        ]
        return GroupStateResponse(group_id=group_id, members=members, assignments=state.get("assignments", {}))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/groups/{group_id}/assignments/{topic_name}", response_model=AssignmentsResponse)
async def get_group_assignments(group_id: str, topic_name: str, request: Request) -> AssignmentsResponse:
    try:
        assignments = _service(request).get_assignments(group_id)
        if assignments is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="group not found")
        topic_assign = assignments.get(topic_name, {})
        as_list = [PartitionAssignment(partition=int(p), consumer_id=cid) for p, cid in topic_assign.items()]
        return AssignmentsResponse(group_id=group_id, topic_name=topic_name, assignments=as_list)
    except Exception as exc:
        raise _map_error(exc) from exc


# Cluster admin endpoints (Phase 4)
@router.post("/cluster/brokers/register", response_model=BrokerInfo)
async def register_broker(payload: BrokerRegisterRequest, request: Request) -> BrokerInfo:
    try:
        coord = request.app.state.broker_service.cluster_coordinator
        info = coord.register_broker(payload.broker_id)
        return BrokerInfo(**info)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/cluster/brokers/{broker_id}/heartbeat", response_model=BrokerInfo)
async def broker_heartbeat(broker_id: str, request: Request) -> BrokerInfo:
    try:
        coord = request.app.state.broker_service.cluster_coordinator
        info = coord.heartbeat(broker_id)
        return BrokerInfo(**info)
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/cluster/brokers/{broker_id}/status", response_model=BrokerStatusResponse)
async def set_broker_status(broker_id: str, payload: BrokerStatusRequest, request: Request) -> BrokerStatusResponse:
    try:
        coord = request.app.state.broker_service.cluster_coordinator
        result = coord.set_broker_status(broker_id, payload.status)
        broker = result.get("broker")
        affected = result.get("affected", [])
        return BrokerStatusResponse(broker=BrokerInfo(**broker), affected=[BrokerStatusChange(**a) for a in affected])
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/cluster/brokers", response_model=list[BrokerInfo])
async def list_brokers(request: Request) -> list[BrokerInfo]:
    try:
        coord = request.app.state.broker_service.cluster_coordinator
        return [BrokerInfo(**b) for b in coord.list_brokers()]
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/cluster/partitions/{topic_name}/{partition}", response_model=PartitionReplicationInfo)
async def partition_info(topic_name: str, partition: int, request: Request) -> PartitionReplicationInfo:
    try:
        coord = request.app.state.broker_service.cluster_coordinator
        info = coord.get_partition_info(topic_name, partition)
        return PartitionReplicationInfo(
            topic_name=topic_name,
            partition=partition,
            leader=info.get("leader"),
            replicas=info.get("replicas", {}),
            in_sync_replicas=info.get("in_sync_replicas", []),
            leader_epoch=info.get("leader_epoch", 0),
            replica_lags=info.get("replica_lags", {}),
        )
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/cluster/topics/{topic_name}")
async def topic_info(topic_name: str, request: Request) -> dict:
    try:
        coord = request.app.state.broker_service.cluster_coordinator
        info = coord.get_topic_info(topic_name)
        return info
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/cluster/failover/{topic_name}/{partition}")
async def failover_partition(topic_name: str, partition: int, request: Request) -> dict:
    try:
        coord = request.app.state.broker_service.cluster_coordinator
        new_leader = coord.failover_partition(topic_name, partition)
        return {"topic": topic_name, "partition": partition, "new_leader": new_leader}
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/cluster/replication/{topic_name}/{partition}")
async def replication_status(topic_name: str, partition: int, request: Request) -> dict:
    try:
        coord = request.app.state.broker_service.cluster_coordinator
        info = coord.get_partition_info(topic_name, partition)
        return {
            "leader": info.get("leader"),
            "replicas": info.get("replicas", []),
            "in_sync_replicas": info.get("in_sync_replicas", []),
            "replica_lags": info.get("replica_lag_smoothed", {}),
            "replica_offsets": info.get("replica_offsets", {}),
            "leader_epoch": info.get("leader_epoch", 0),
        }
    except Exception as exc:
        raise _map_error(exc) from exc
