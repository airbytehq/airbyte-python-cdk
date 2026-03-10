# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import logging
import time
from queue import Full, Queue
from typing import Optional

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
    # Maximum time (seconds) to block on queue.put() before raising.
    # Prevents silent deadlocks when the bounded queue is full and the
    # main-thread consumer cannot drain it.
    _QUEUE_PUT_TIMEOUT = 300.0  # 5 minutes

    def __init__(
        self,
        queue: Queue[QueueItem],
        partition_logger: Optional[PartitionLogger] = None,
    ) -> None:
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
        partition_start = time.monotonic()
        stream_name = partition.stream_name()
        slice_info = partition.to_slice()
        logger = logging.getLogger(f"airbyte.partition_reader.{stream_name}")
        logger.info(
            "Partition read STARTED for stream=%s, slice=%s",
            stream_name,
            slice_info,
        )
        try:
            if self._partition_logger:
                self._partition_logger.log(partition)

            record_count = 0
            last_progress_time = partition_start
            for record in partition.read():
                self._put_with_timeout(record, stream_name, logger)
                cursor.observe(record)
                record_count += 1
                now = time.monotonic()
                if now - last_progress_time >= 30.0:
                    logger.info(
                        "Partition read PROGRESS for stream=%s: %d records read so far (%.0fs elapsed), slice=%s",
                        stream_name,
                        record_count,
                        now - partition_start,
                        slice_info,
                    )
                    last_progress_time = now
            cursor.close_partition(partition)
            elapsed = time.monotonic() - partition_start
            logger.info(
                "Partition read COMPLETED for stream=%s: %d records in %.1fs, slice=%s",
                stream_name,
                record_count,
                elapsed,
                slice_info,
            )
            self._put_with_timeout(
                PartitionCompleteSentinel(partition, self._IS_SUCCESSFUL),
                stream_name,
                logger,
            )
        except Exception as e:
            elapsed = time.monotonic() - partition_start
            logger.info(
                "Partition read FAILED for stream=%s after %.1fs: %s, slice=%s",
                stream_name,
                elapsed,
                str(e)[:200],
                slice_info,
            )
            self._queue.put(StreamThreadException(e, stream_name))
            self._queue.put(PartitionCompleteSentinel(partition, not self._IS_SUCCESSFUL))

    def _put_with_timeout(
        self,
        item: QueueItem,
        stream_name: str,
        logger: logging.Logger,
    ) -> None:
        """Put an item on the queue, raising if blocked longer than the timeout.

        This prevents a deadlock where all worker threads are blocked on
        ``queue.put()`` while the main thread is unable to drain the queue.
        """
        put_start = time.monotonic()
        while True:
            try:
                self._queue.put(item, timeout=self._QUEUE_PUT_TIMEOUT)
                return
            except Full:
                blocked_secs = time.monotonic() - put_start
                logger.warning(
                    "queue.put() blocked for %.0fs for stream=%s "
                    "(queue_size=%d). Possible deadlock.",
                    blocked_secs,
                    stream_name,
                    self._queue.qsize(),
                )
                raise RuntimeError(
                    f"Timed out putting item on the queue after "
                    f"{blocked_secs:.0f}s for stream {stream_name}. "
                    f"This indicates a deadlock in the concurrent read pipeline."
                )
