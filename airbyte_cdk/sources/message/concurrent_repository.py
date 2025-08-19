# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
import logging
import os
from queue import Queue
from typing import Callable, Iterable

from airbyte_cdk.models import AirbyteMessage, Level
from airbyte_cdk.models import Type as MessageType
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
    """

    def __init__(self, queue: Queue[QueueItem], message_repository: MessageRepository):
        self._queue = queue
        self._decorated_message_repository = message_repository

        test_env = os.getenv("PYTEST_CURRENT_TEST")
        self._log_messages_for_testing = (
            test_env and "test_concurrent_declarative_source.py" in test_env
        )

    def emit_message(self, message: AirbyteMessage) -> None:
        if self._log_messages_for_testing:
            self._log_message(message)
        self._decorated_message_repository.emit_message(message)
        for message in self._decorated_message_repository.consume_queue():
            if self._log_messages_for_testing:
                self._log_message(message)
            self._queue.put(message)

    def log_message(self, level: Level, message_provider: Callable[[], LogMessage]) -> None:
        self._decorated_message_repository.log_message(level, message_provider)
        for message in self._decorated_message_repository.consume_queue():
            if self._log_messages_for_testing:
                self._log_message(message)
            self._queue.put(message)

    def consume_queue(self) -> Iterable[AirbyteMessage]:
        """
        This method shouldn't need to be called because as part of emit_message() we are already
        loading messages onto the queue processed on the main thread.
        """
        yield from []

    @staticmethod
    def _log_message(message: AirbyteMessage) -> None:
        if message.type == MessageType.STATE:
            if message.state and message.state.stream:
                state = message.state.stream.stream_state.__dict__
                logger.info(
                    f"Processing and emitting message of type {message.type} with contents: {message.state.stream.stream_state.__dict__}"
                )
        else:
            logger.info(
                f"Processing and emitting message of type {message.type} with contents: {message.__dict__}"
            )
