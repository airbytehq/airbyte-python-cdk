#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging
from queue import Queue
from typing import Optional

from airbyte_cdk.sources.concurrent_source.stream_thread_exception import StreamThreadException
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.types import (
    PartitionCompleteSentinel,
    QueueItem,
)
from airbyte_cdk.sources.utils.slice_logger import SliceLogger


class PartitionLogger:
    def __init__(self, slice_logger: SliceLogger, logger: logging.Logger, message_repository: MessageRepository):
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

    def __init__(self, queue: Queue[QueueItem], partition_logger: Optional[PartitionLogger]) -> None:
        """
        :param queue: The queue to put the records in.
        """
        self._queue = queue
        self._partition_logger = partition_logger

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
        if self._partition_logger:
            self._partition_logger.log(partition)

        try:
            for record in partition.read():
                self._queue.put(record)
                cursor.observe(record)

            # this assumes the cursor will put a state message on the queue. It also needs to be before the completion sentinel else the concurrent_read_processor might end the sync before consuming the state
            cursor.close_partition(partition)
            self._queue.put(PartitionCompleteSentinel(partition, self._IS_SUCCESSFUL))
        except Exception as e:
            self._queue.put(StreamThreadException(e, partition.stream_name()))
            self._queue.put(PartitionCompleteSentinel(partition, not self._IS_SUCCESSFUL))
