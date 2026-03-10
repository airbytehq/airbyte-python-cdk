#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging
import time
from queue import Full, Queue

from airbyte_cdk.sources.concurrent_source.partition_generation_completed_sentinel import (
    PartitionGenerationCompletedSentinel,
)
from airbyte_cdk.sources.concurrent_source.stream_thread_exception import StreamThreadException
from airbyte_cdk.sources.concurrent_source.thread_pool_manager import ThreadPoolManager
from airbyte_cdk.sources.streams.concurrent.abstract_stream import AbstractStream
from airbyte_cdk.sources.streams.concurrent.partitions.types import QueueItem


class PartitionEnqueuer:
    """
    Generates partitions from a partition generator and puts them in a queue.
    """

    # Maximum time (seconds) to block on queue.put() before raising.
    # Prevents silent deadlocks when the bounded queue is full and the
    # main-thread consumer cannot drain it.
    _QUEUE_PUT_TIMEOUT = 300.0  # 5 minutes

    def __init__(
        self,
        queue: Queue[QueueItem],
        thread_pool_manager: ThreadPoolManager,
        sleep_time_in_seconds: float = 0.1,
    ) -> None:
        """
        :param queue:  The queue to put the partitions in.
        :param throttler: The throttler to use to throttle the partition generation.
        """
        self._queue = queue
        self._thread_pool_manager = thread_pool_manager
        self._sleep_time_in_seconds = sleep_time_in_seconds

    def generate_partitions(self, stream: AbstractStream) -> None:
        """Generate partitions from a partition generator and put them in a queue.

        When all the partitions are added to the queue, a sentinel is added to the queue to indicate
        that all the partitions have been generated.

        If an exception is encountered, the exception will be caught and put in the queue. This is
        very important because if we don't, the main thread will have no way to know that something
        went wrong and will wait until the timeout is reached.

        This method is meant to be called in a separate thread.
        """
        logger = logging.getLogger(f"airbyte.partition_enqueuer.{stream.name}")
        logger.info("Partition generation STARTED for stream=%s", stream.name)
        partition_count = 0
        start_time = time.monotonic()
        try:
            for partition in stream.generate_partitions():
                partition_count += 1
                logger.info(
                    "Partition generation: enqueuing partition #%d for stream=%s, slice=%s",
                    partition_count,
                    stream.name,
                    partition.to_slice(),
                )
                # Adding partitions to the queue generates futures. To avoid having too many futures, we throttle here. We understand that
                # we might add more futures than the limit by throttling in the threads while it is the main thread that actual adds the
                # future but we expect the delta between the max futures length and the actual to be small enough that it would not be an
                # issue. We do this in the threads because we want the main thread to always be processing QueueItems as if it does not, the
                # queue size could grow and generating OOM issues.
                #
                # Also note that we do not expect this to create deadlocks where all worker threads wait because we have less
                # PartitionEnqueuer threads than worker threads.
                #
                # Also note that prune_to_validate_has_reached_futures_limit has a lock while pruning which might create a bottleneck in
                # terms of performance.
                while self._thread_pool_manager.prune_to_validate_has_reached_futures_limit():
                    time.sleep(self._sleep_time_in_seconds)
                self._put_with_timeout(partition, stream.name, logger)
            elapsed = time.monotonic() - start_time
            logger.info(
                "Partition generation COMPLETED for stream=%s: %d partitions in %.1fs",
                stream.name,
                partition_count,
                elapsed,
            )
            self._put_with_timeout(
                PartitionGenerationCompletedSentinel(stream), stream.name, logger
            )
        except Exception as e:
            elapsed = time.monotonic() - start_time
            logger.info(
                "Partition generation FAILED for stream=%s after %.1fs with %d partitions: %s",
                stream.name,
                elapsed,
                partition_count,
                str(e)[:200],
            )
            self._queue.put(StreamThreadException(e, stream.name))
            self._queue.put(PartitionGenerationCompletedSentinel(stream))

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
