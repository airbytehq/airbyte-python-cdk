#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# ruff: noqa: A005  # Shadows built-in 'types' module

from __future__ import annotations

from typing import Union

from airbyte_cdk.sources.concurrent_source.partition_generation_completed_sentinel import (
    PartitionGenerationCompletedSentinel,
)
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.record import Record


class PartitionCompleteSentinel:  # noqa: PLW1641  # missing __hash__ method
    """A sentinel object indicating all records for a partition were produced.
    Includes a pointer to the partition that was processed.
    """

    def __init__(
        self,
        partition: Partition,
        is_successful: bool = True,  # noqa: FBT001, FBT002  (bool positional arg)
    ) -> None:
        """:param partition: The partition that was processed"""
        self.partition = partition
        self.is_successful = is_successful

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PartitionCompleteSentinel):
            return self.partition == other.partition
        return False


"""
Typedef representing the items that can be added to the ThreadBasedConcurrentStream
"""
QueueItem = Union[  # noqa: UP007  (deprecated Union type)
    Record, Partition, PartitionCompleteSentinel, PartitionGenerationCompletedSentinel, Exception
]
