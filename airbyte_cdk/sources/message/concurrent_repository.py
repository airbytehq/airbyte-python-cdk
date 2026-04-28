# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
import logging
import threading
from collections import deque
from queue import Full, Queue
from typing import Callable, Iterable

from airbyte_cdk.models import AirbyteMessage, Level
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.message.repository import LogMessage, MessageRepository
from airbyte_cdk.sources.streams.concurrent.partitions.types import QueueItem

logger = logging.getLogger("airbyte")


class ConcurrentMessageRepository(MessageRepository):
    """Message repository that loads messages onto the shared concurrent queue.

    Messages are placed directly onto the queue consumed by the main thread.
    This ensures correct ordering, which is especially important for the
    connector builder's grouping of request/response, pages, and partitions.

    Deadlock prevention: the main thread is the sole consumer of the queue.
    If it also calls ``emit_message`` or ``log_message`` (e.g. when emitting
    a final state via ``ensure_at_least_one_state_emitted``), a blocking
    ``put`` on a full queue would deadlock because no other thread can drain
    it.  To avoid this, the repository captures the consumer (main) thread
    ID at construction time.  Calls from the main thread use non-blocking
    ``put``; overflow is buffered in ``_pending`` and drained on the next
    ``consume_queue`` call, which the main thread already invokes after
    processing every queue item.  Worker threads continue to use blocking
    ``put`` for normal back-pressure.
    """

    def __init__(self, queue: Queue[QueueItem], message_repository: MessageRepository):
        self._queue = queue
        self._decorated_message_repository = message_repository
        self._consumer_thread_id: int = threading.get_ident()
        self._pending: deque[AirbyteMessage] = deque()

    def emit_message(self, message: AirbyteMessage) -> None:
        self._decorated_message_repository.emit_message(message)
        for queued_message in self._decorated_message_repository.consume_queue():
            self._put(queued_message)

    def log_message(self, level: Level, message_provider: Callable[[], LogMessage]) -> None:
        self._decorated_message_repository.log_message(level, message_provider)
        for queued_message in self._decorated_message_repository.consume_queue():
            self._put(queued_message)

    def consume_queue(self) -> Iterable[AirbyteMessage]:
        """Drain any messages buffered because the queue was full.

        The main thread calls this after processing every queue item, so
        buffered messages are delivered promptly without risking deadlock.
        """
        while self._pending:
            yield self._pending.popleft()

    def _put(self, message: AirbyteMessage) -> None:
        """Place a message on the shared queue.

        On the consumer (main) thread a non-blocking put is used; if the
        queue is full the message is appended to ``_pending`` instead of
        blocking.  Worker threads use a normal blocking put so that
        back-pressure is preserved.
        """
        if threading.get_ident() == self._consumer_thread_id:
            try:
                self._queue.put(message, block=False)
            except Full:
                self._pending.append(message)
        else:
            self._queue.put(message)
