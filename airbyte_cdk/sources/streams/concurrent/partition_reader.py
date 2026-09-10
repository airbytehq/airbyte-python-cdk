# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import logging
from queue import Queue
from typing import Optional

from airbyte_cdk.sources.concurrent_source.stream_abort_registry import StreamAbortRegistry
from airbyte_cdk.sources.concurrent_source.stream_thread_exception import StreamThreadException
from airbyte_cdk.sources.message.repository import MessageRepository
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.types import (
    PartitionCompleteSentinel,
    QueueItem,
)
from airbyte_cdk.sources.utils.slice_logger import SliceLogger


# Since moving all the connector builder workflow to the concurrent CDK which required correct ordering
# of grouping log messages onto the main write thread using the ConcurrentMessageRepository, this
# separate flow and class that was used to log slices onto this partition's message_repository
# should just be replaced by emitting messages directly onto the repository instead of an intermediary.
class PartitionLogger:
    """
    Helper class that provides a mechanism for passing a log message onto the current
    partitions message repository
    """

    def __init__(
        self,
        slice_logger: SliceLogger,
        logger: logging.Logger,
        message_repository: MessageRepository,
    ):
        self._slice_logger = slice_logger
        self._logger = logger
        self._message_repository = message_repository

    def log(self, partition: Partition) -> None:
        if self._slice_logger.should_log_slice_message(self._logger):
            self._message_repository.emit_message(
                self._slice_logger.create_slice_log_message(partition.to_slice())
            )


class PartitionReader:
    """
    Generates records from a partition and puts them in a queue.
    """

    _IS_SUCCESSFUL = True

    def __init__(
        self,
        queue: Queue[QueueItem],
        partition_logger: Optional[PartitionLogger] = None,
        stream_abort_registry: Optional[StreamAbortRegistry] = None,
    ) -> None:
        """
        :param queue: The queue to put the records in.
        :param partition_logger: Optional logger used to emit a slice log message per partition.
        :param stream_abort_registry: Optional registry of streams that failed fatally. Partitions
            belonging to an aborted stream are skipped instead of being read.
        """
        self._queue = queue
        self._partition_logger = partition_logger
        self._stream_abort_registry = stream_abort_registry

    def process_partition(self, partition: Partition, cursor: Cursor) -> None:
        """
        Process a partition and put the records in the output queue.
        When all the partitions are added to the queue, a sentinel is added to the queue to indicate that all the partitions have been generated.

        If an exception is encountered, the exception will be caught and put in the queue. This is very important because if we don't, the
        main thread will have no way to know that something when wrong and will wait until the timeout is reached

        This method is meant to be called from a thread.
        :param partition: The partition to read data from
        :return: None
        """
        # If the stream already failed fatally on another partition, skip this one instead of
        # repeating the same doomed request. We do not close the partition on the cursor so that
        # state is not advanced past the failure.
        if self._stream_abort_registry and self._stream_abort_registry.is_aborted(
            partition.stream_name()
        ):
            self._queue.put(PartitionCompleteSentinel(partition, not self._IS_SUCCESSFUL))
            return

        try:
            if self._partition_logger:
                self._partition_logger.log(partition)

            for record in partition.read():
                self._queue.put(record)
                cursor.observe(record)
            cursor.close_partition(partition)
            self._queue.put(PartitionCompleteSentinel(partition, self._IS_SUCCESSFUL))
        except Exception as e:
            self._queue.put(StreamThreadException(e, partition.stream_name()))
            self._queue.put(PartitionCompleteSentinel(partition, not self._IS_SUCCESSFUL))
