from __future__ import annotations

import hashlib


class Partitioner:
    def partition_for_key(self, key: str, partitions: int) -> int:
        if partitions < 1:
            raise ValueError("partitions must be greater than zero")

        digest = hashlib.sha256(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], byteorder="big", signed=False) % partitions

    def next_round_robin(self, current_partition: int, partitions: int) -> tuple[int, int]:
        if partitions < 1:
            raise ValueError("partitions must be greater than zero")

        partition = current_partition % partitions
        return partition, (partition + 1) % partitions
