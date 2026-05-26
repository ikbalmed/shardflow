from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class AppPaths:
    root_dir: Path
    data_dir: Path
    metadata_file: Path
    offsets_file: Path
    groups_file: Path
    cluster_file: Path


def build_app_paths(base_dir: Path | None = None) -> AppPaths:
    root_dir = base_dir or Path(__file__).resolve().parents[2]
    data_dir = root_dir / "data"
    return AppPaths(
        root_dir=root_dir,
        data_dir=data_dir,
        metadata_file=data_dir / "topics.json",
        offsets_file=data_dir / "consumer_offsets.json",
        groups_file=data_dir / "groups.json",
        cluster_file=data_dir / "cluster.json",
    )
