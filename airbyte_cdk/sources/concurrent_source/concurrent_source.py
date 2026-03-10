#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import concurrent
import logging
import os
import sys
import threading
import time
from queue import Empty, Queue
from typing import Iterable, Iterator, List, Optional

from airbyte_cdk.models import AirbyteMessage
from airbyte_cdk.sources.concurrent_source.concurrent_read_processor import ConcurrentReadProcessor
from airbyte_cdk.sources.concurrent_source.partition_generation_completed_sentinel import (
    PartitionGenerationCompletedSentinel,
)
from airbyte_cdk.sources.concurrent_source.stream_thread_exception import StreamThreadException
from airbyte_cdk.sources.concurrent_source.thread_pool_manager import ThreadPoolManager
from airbyte_cdk.sources.message import InMemoryMessageRepository, MessageRepository
from airbyte_cdk.sources.streams.concurrent.abstract_stream import AbstractStream
from airbyte_cdk.sources.streams.concurrent.partition_enqueuer import PartitionEnqueuer
from airbyte_cdk.sources.streams.concurrent.partition_reader import PartitionLogger, PartitionReader
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.types import (
    PartitionCompleteSentinel,
    QueueItem,
)
from airbyte_cdk.sources.types import Record
from airbyte_cdk.sources.utils.slice_logger import DebugSliceLogger, SliceLogger


class ConcurrentSource:
    """
    A Source that reads data from multiple AbstractStreams concurrently.
    It does so by submitting partition generation, and partition read tasks to a thread pool.
    The tasks asynchronously add their output to a shared queue.
    The read is done when all partitions for all streams w ere generated and read.
    """

    DEFAULT_TIMEOUT_SECONDS = 900
    # If the main thread makes no progress for this long, the watchdog
    # terminates the process.  This breaks deadlocks caused by stdout/stderr
    # pipe blockage where no in-process timeout can fire because I/O itself
    # is blocked at the OS level.
    _WATCHDOG_TIMEOUT_SECONDS = 600.0  # 10 minutes

    @staticmethod
    def create(
        num_workers: int,
        initial_number_of_partitions_to_generate: int,
        logger: logging.Logger,
        slice_logger: SliceLogger,
        message_repository: MessageRepository,
        queue: Optional[Queue[QueueItem]] = None,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> "ConcurrentSource":
        is_single_threaded = initial_number_of_partitions_to_generate == 1 and num_workers == 1
        too_many_generator = (
            not is_single_threaded and initial_number_of_partitions_to_generate >= num_workers
        )
        assert not too_many_generator, (
            "It is required to have more workers than threads generating partitions"
        )
        threadpool = ThreadPoolManager(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=num_workers, thread_name_prefix="workerpool"
            ),
            logger,
        )
        return ConcurrentSource(
            threadpool=threadpool,
            logger=logger,
            slice_logger=slice_logger,
            queue=queue,
            message_repository=message_repository,
            initial_number_partitions_to_generate=initial_number_of_partitions_to_generate,
            timeout_seconds=timeout_seconds,
        )

    def __init__(
        self,
        threadpool: ThreadPoolManager,
        logger: logging.Logger,
        slice_logger: SliceLogger = DebugSliceLogger(),
        queue: Optional[Queue[QueueItem]] = None,
        message_repository: MessageRepository = InMemoryMessageRepository(),
        initial_number_partitions_to_generate: int = 1,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        """
        :param threadpool: The threadpool to submit tasks to
        :param logger: The logger to log to
        :param slice_logger: The slice logger used to create messages on new slices
        :param message_repository: The repository to emit messages to
        :param initial_number_partitions_to_generate: The initial number of concurrent partition generation tasks. Limiting this number ensures will limit the latency of the first records emitted. While the latency is not critical, emitting the records early allows the platform and the destination to process them as early as possible.
        :param timeout_seconds: The maximum number of seconds to wait for a record to be read from the queue. If no record is read within this time, the source will stop reading and return.
        """
        self._threadpool = threadpool
        self._logger = logger
        self._slice_logger = slice_logger
        self._message_repository = message_repository
        self._initial_number_partitions_to_generate = initial_number_partitions_to_generate
        self._timeout_seconds = timeout_seconds

        # We set a maxsize to for the main thread to process record items when the queue size grows. This assumes that there are less
        # threads generating partitions that than are max number of workers. If it weren't the case, we could have threads only generating
        # partitions which would fill the queue. This number is arbitrarily set to 10_000 but will probably need to be changed given more
        # information and might even need to be configurable depending on the source
        self._queue = queue or Queue(maxsize=10_000)

    def read(
        self,
        streams: List[AbstractStream],
    ) -> Iterator[AirbyteMessage]:
        self._logger.info("Starting syncing")
        # Shared timestamp updated every time the main thread makes progress
        # (consumes an item from the queue).  The watchdog reads this to
        # detect when the main thread is stuck.
        self._last_progress_time = time.monotonic()
        self._watchdog_should_run = True
        watchdog = threading.Thread(
            target=self._watchdog_loop,
            daemon=True,
            name="progress-watchdog",
        )
        watchdog.start()

        try:
            concurrent_stream_processor = ConcurrentReadProcessor(
                streams,
                PartitionEnqueuer(self._queue, self._threadpool),
                self._threadpool,
                self._logger,
                self._slice_logger,
                self._message_repository,
                PartitionReader(
                    self._queue,
                    PartitionLogger(self._slice_logger, self._logger, self._message_repository),
                ),
            )

            # Enqueue initial partition generation tasks
            yield from self._submit_initial_partition_generators(concurrent_stream_processor)

            # Read from the queue until all partitions were generated and read
            yield from self._consume_from_queue(
                self._queue,
                concurrent_stream_processor,
            )
            self._threadpool.check_for_errors_and_shutdown()
            self._logger.info("Finished syncing")
        finally:
            self._watchdog_should_run = False

    def _submit_initial_partition_generators(
        self, concurrent_stream_processor: ConcurrentReadProcessor
    ) -> Iterable[AirbyteMessage]:
        for _ in range(self._initial_number_partitions_to_generate):
            status_message = concurrent_stream_processor.start_next_partition_generator()
            if status_message:
                yield status_message

    def _consume_from_queue(
        self,
        queue: Queue[QueueItem],
        concurrent_stream_processor: ConcurrentReadProcessor,
    ) -> Iterable[AirbyteMessage]:
        last_item_time = time.monotonic()
        heartbeat_interval = 60.0  # Log heartbeat every 60 seconds
        items_since_last_heartbeat = 0

        while True:
            try:
                airbyte_message_or_record_or_exception = queue.get(timeout=heartbeat_interval)
            except Empty:
                elapsed = time.monotonic() - last_item_time
                self._logger.info(
                    "Queue heartbeat: no items received for %.0fs. "
                    "queue_size=%d, threadpool_done=%s, active_threads=%d",
                    elapsed,
                    queue.qsize(),
                    self._threadpool.is_done(),
                    threading.active_count(),
                )
                continue

            if not airbyte_message_or_record_or_exception:
                break

            now = time.monotonic()
            items_since_last_heartbeat += 1
            if now - last_item_time >= heartbeat_interval:
                self._logger.info(
                    "Queue heartbeat: processed %d items in last %.0fs. "
                    "queue_size=%d, item_type=%s",
                    items_since_last_heartbeat,
                    now - last_item_time,
                    queue.qsize(),
                    type(airbyte_message_or_record_or_exception).__name__,
                )
                items_since_last_heartbeat = 0
            self._last_progress_time = now
            last_item_time = now

            yield from self._handle_item(
                airbyte_message_or_record_or_exception,
                concurrent_stream_processor,
            )
            # In the event that a partition raises an exception, anything remaining in
            # the queue will be missed because is_done() can raise an exception and exit
            # out of this loop before remaining items are consumed
            if queue.empty() and concurrent_stream_processor.is_done():
                # all partitions were generated and processed. we're done here
                break

    def _watchdog_loop(self) -> None:
        """Daemon thread that terminates the process when the main thread stalls.

        In Airbyte Cloud the source container's stdout and stderr are read by
        the platform (replication-orchestrator).  If the platform stops reading
        (e.g. destination backpressure), both pipes fill up and *all* threads
        block on I/O — including the main thread's ``yield`` and every worker
        thread's ``logger.*()`` call.  No in-process timeout can fire because
        the timeout's own log/write call also blocks.

        This watchdog does **not** perform any I/O.  It simply checks a shared
        monotonic timestamp that the main thread updates whenever it consumes a
        queue item.  If no progress is observed for ``_WATCHDOG_TIMEOUT_SECONDS``,
        it calls ``os._exit(1)`` which is a raw syscall that terminates the
        process immediately regardless of I/O state.
        """
        while self._watchdog_should_run:
            time.sleep(30)  # check every 30 seconds
            if not self._watchdog_should_run:
                return
            elapsed = time.monotonic() - self._last_progress_time
            if elapsed >= self._WATCHDOG_TIMEOUT_SECONDS:
                # Write directly to stderr fd to bypass Python buffering
                # which may be blocked.  This is best-effort; if the fd is
                # blocked the write will simply fail and we still exit.
                try:
                    msg = (
                        f"WATCHDOG: Main thread made no progress for "
                        f"{elapsed:.0f}s (threshold={self._WATCHDOG_TIMEOUT_SECONDS:.0f}s). "
                        f"Terminating process to prevent indefinite hang.\n"
                    )
                    os.write(sys.stderr.fileno(), msg.encode())
                except Exception:
                    # Intentionally ignored: logging is best-effort and must
                    # not prevent process termination via os._exit() below.
                    pass
                os._exit(1)

    def _handle_item(
        self,
        queue_item: QueueItem,
        concurrent_stream_processor: ConcurrentReadProcessor,
    ) -> Iterable[AirbyteMessage]:
        # handle queue item and call the appropriate handler depending on the type of the queue item
        if isinstance(queue_item, StreamThreadException):
            yield from concurrent_stream_processor.on_exception(queue_item)
        elif isinstance(queue_item, PartitionGenerationCompletedSentinel):
            yield from concurrent_stream_processor.on_partition_generation_completed(queue_item)
        elif isinstance(queue_item, Partition):
            concurrent_stream_processor.on_partition(queue_item)
        elif isinstance(queue_item, PartitionCompleteSentinel):
            yield from concurrent_stream_processor.on_partition_complete_sentinel(queue_item)
        elif isinstance(queue_item, Record):
            yield from concurrent_stream_processor.on_record(queue_item)
        elif isinstance(queue_item, AirbyteMessage):
            yield queue_item
        else:
            raise ValueError(f"Unknown queue item type: {type(queue_item)}")
