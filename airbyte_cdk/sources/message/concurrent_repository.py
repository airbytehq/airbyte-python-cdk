# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
import logging
import threading
from collections import deque
from queue import Full, Queue
from typing import Callable, Iterable

from airbyte_cdk.models import AirbyteMessage, Level
from airbyte_cdk.sources.message.repository import LogMessage, MessageRepository
from airbyte_cdk.sources.streams.concurrent.partitions.types import QueueItem

logger = logging.getLogger("airbyte")


class ConcurrentMessageRepository(MessageRepository):
    """
    Message repository that immediately loads messages onto the queue processed on the
    main thread. This ensures that messages are processed in the correct order they are
    received. The InMemoryMessageRepository implementation does not have guaranteed
    ordering since whether to process the main thread vs. partitions is non-deterministic
    and there can be a lag between reading the main-thread and consuming messages on the
    MessageRepository.

    This is particularly important for the connector builder which relies on grouping
    of messages to organize request/response, pages, and partitions.

    DEADLOCK PREVENTION:
    The main thread is the sole consumer of the shared queue. If it calls queue.put()
    while the queue is full, it deadlocks — nobody else will drain the queue.
    This happens in 3 code paths from _handle_item:
      1. PartitionCompleteSentinel → _on_stream_is_done → ensure_at_least_one_state_emitted → emit_message → queue.put(state)
      2. PartitionGenerationCompletedSentinel → _on_stream_is_done → same path
      3. Partition → on_partition → emit_message(slice_log) → queue.put(log)
    To prevent this, the main thread uses non-blocking put(block=False). If the queue
    is full, messages are buffered in _pending and drained via consume_queue(), which
    the main thread calls after processing every queue item.
    Worker threads continue using blocking put() for normal backpressure.
    """

    def __init__(self, queue: Queue[QueueItem], message_repository: MessageRepository):
        self._queue = queue
        self._decorated_message_repository = message_repository
        # Capture the thread ID of the consumer (main thread) at construction time.
        # This is always the main thread because ConcurrentSource.__init__ (and the
        # declarative source that creates this repository) runs on the main thread.
        self._consumer_thread_id = threading.get_ident()
        # Buffer for messages that couldn't be put on the queue from the main thread
        # because the queue was full. Drained by consume_queue().
        # deque.append() and deque.popleft() are atomic in CPython (GIL-protected).
        self._pending: deque[AirbyteMessage] = deque()

    def _put_on_queue(self, message: AirbyteMessage) -> None:
        """Put a message on the shared queue, with deadlock prevention for the main thread."""
        if threading.get_ident() == self._consumer_thread_id:
            # Main thread (consumer): non-blocking to prevent self-deadlock.
            # If queue is full, buffer the message — it will be drained via consume_queue().
            try:
                self._queue.put(message, block=False)
            except Full:
                self._pending.append(message)
        else:
            # Worker thread: blocking put for normal backpressure.
            self._queue.put(message)

    def emit_message(self, message: AirbyteMessage) -> None:
        self._decorated_message_repository.emit_message(message)
        for msg in self._decorated_message_repository.consume_queue():
            self._put_on_queue(msg)

    def log_message(self, level: Level, message_provider: Callable[[], LogMessage]) -> None:
        self._decorated_message_repository.log_message(level, message_provider)
        for msg in self._decorated_message_repository.consume_queue():
            self._put_on_queue(msg)

    def consume_queue(self) -> Iterable[AirbyteMessage]:
        """
        Drain any messages that were buffered because the queue was full when the
        main thread tried to put them. This is called by the main thread after
        processing every queue item (in on_record, on_partition_complete_sentinel,
        _on_stream_is_done), ensuring buffered messages are yielded promptly.
        """
        while self._pending:
            yield self._pending.popleft()
