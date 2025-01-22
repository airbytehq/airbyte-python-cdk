#  # noqa: A005
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Union

from airbyte_cdk.sources.concurrent_source.partition_generation_completed_sentinel import (
    PartitionGenerationCompletedSentinel,
)
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.types import Record


class PartitionCompleteSentinel:  # noqa: PLW1641
    """
    A sentinel object indicating all records for a partition were produced.
    Includes a pointer to the partition that was processed.
    """

    def __init__(self, partition: Partition, is_successful: bool = True):  # noqa: ANN204, FBT001, FBT002
        """
        :param partition: The partition that was processed
        """
        self.partition = partition
        self.is_successful = is_successful

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PartitionCompleteSentinel):
            return self.partition == other.partition
        return False


"""
Typedef representing the items that can be added to the ThreadBasedConcurrentStream
"""
QueueItem = Union[  # noqa: UP007
    Record, Partition, PartitionCompleteSentinel, PartitionGenerationCompletedSentinel, Exception
]
