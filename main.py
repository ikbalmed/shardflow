from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI

from app.api.routes import router
from app.broker.consumer_offsets import ConsumerOffsetStore
from app.broker.partitioner import Partitioner
from app.broker.service import BrokerService
from app.broker.topic_manager import TopicManager
from app.core.config import build_app_paths
from app.storage.log_store import LogStore
from app.broker.group_coordinator import GroupCoordinator
from app.broker.cluster_coordinator import ClusterCoordinator


def create_app(
    data_dir: Path | None = None,
    segment_max_records: int = 3,
    retention_segments: int = 4,
    group_expiration_seconds: int = 10,
    broker_id: str = "broker-1",
    replication_factor: int = 2,
) -> FastAPI:
    paths = build_app_paths(data_dir)
    paths.data_dir.mkdir(parents=True, exist_ok=True)

    partitioner = Partitioner()
    topic_manager = TopicManager(paths.metadata_file, paths.data_dir, partitioner)
    log_store = LogStore(
        paths.data_dir,
        max_records_per_segment=segment_max_records,
        retention_segments=retention_segments,
    )
    offset_store = ConsumerOffsetStore(paths.offsets_file)
    group_coord = GroupCoordinator(paths.groups_file, topic_manager, expiration_seconds=group_expiration_seconds)
    cluster_coord = ClusterCoordinator(paths.cluster_file, paths.data_dir, replication_factor=replication_factor)
    # register this local broker in the cluster
    cluster_coord.register_broker(broker_id)
    broker_service = BrokerService(
        topic_manager=topic_manager,
        log_store=log_store,
        offset_store=offset_store,
        group_coordinator=group_coord,
        cluster_coordinator=cluster_coord,
        local_broker_id=broker_id,
    )

    app = FastAPI(title="Mini Kafka MVP", version="0.1.0")
    app.state.broker_service = broker_service
    app.include_router(router)
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
