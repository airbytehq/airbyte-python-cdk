#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import threading
from queue import Queue

import pytest

from airbyte_cdk.models import (
    AirbyteControlConnectorConfigMessage,
    AirbyteControlMessage,
    AirbyteMessage,
    AirbyteStateMessage,
    AirbyteStateType,
    Level,
    OrchestratorType,
    Type,
)
from airbyte_cdk.sources.message.concurrent_repository import ConcurrentMessageRepository
from airbyte_cdk.sources.message.repository import InMemoryMessageRepository


def _make_state_message(stream_name: str = "test_stream") -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.STATE,
        state=AirbyteStateMessage(type=AirbyteStateType.STREAM, data={"stream_name": stream_name}),
    )


def _make_control_message() -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.CONTROL,
        control=AirbyteControlMessage(
            type=OrchestratorType.CONNECTOR_CONFIG,
            emitted_at=0,
            connectorConfig=AirbyteControlConnectorConfigMessage(config={"key": "value"}),
        ),
    )


@pytest.fixture()
def small_queue() -> Queue:
    return Queue(maxsize=2)


@pytest.fixture()
def repo(small_queue: Queue) -> ConcurrentMessageRepository:
    return ConcurrentMessageRepository(small_queue, InMemoryMessageRepository())


def test_emit_message_puts_on_queue_when_space_available() -> None:
    """When the queue has space, emit_message places the message directly on it."""
    queue: Queue = Queue(maxsize=100)
    repo = ConcurrentMessageRepository(queue, InMemoryMessageRepository())

    msg = _make_control_message()
    repo.emit_message(msg)

    assert not queue.empty()
    assert queue.get_nowait() == msg


def test_emit_message_buffers_when_queue_full_on_consumer_thread(
    small_queue: Queue, repo: ConcurrentMessageRepository
) -> None:
    """When called from the consumer thread with a full queue, the message goes to _pending."""
    small_queue.put("filler_1")
    small_queue.put("filler_2")
    assert small_queue.full()

    msg = _make_state_message()
    repo.emit_message(msg)

    assert len(repo._pending) == 1
    assert repo._pending[0] == msg


def test_consume_queue_drains_pending_buffer(
    small_queue: Queue, repo: ConcurrentMessageRepository
) -> None:
    """consume_queue yields messages that were buffered due to a full queue."""
    small_queue.put("filler_1")
    small_queue.put("filler_2")

    msg1 = _make_state_message("stream_1")
    msg2 = _make_state_message("stream_2")
    repo.emit_message(msg1)
    repo.emit_message(msg2)

    drained = list(repo.consume_queue())
    assert drained == [msg1, msg2]
    assert len(repo._pending) == 0


def test_consume_queue_empty_when_no_pending(repo: ConcurrentMessageRepository) -> None:
    """consume_queue yields nothing when there are no pending messages."""
    assert list(repo.consume_queue()) == []


def test_log_message_buffers_when_queue_full_on_consumer_thread(
    small_queue: Queue, repo: ConcurrentMessageRepository
) -> None:
    """log_message also uses non-blocking put on the consumer thread."""
    small_queue.put("filler_1")
    small_queue.put("filler_2")

    repo.log_message(Level.INFO, lambda: {"message": "test log"})

    assert len(repo._pending) == 1


def test_worker_thread_uses_blocking_put() -> None:
    """Worker threads (non-consumer) should use blocking put for back-pressure."""
    queue: Queue = Queue(maxsize=1)
    repo = ConcurrentMessageRepository(queue, InMemoryMessageRepository())

    queue.put("filler")

    worker_started = threading.Event()
    worker_done = threading.Event()

    def worker_emit() -> None:
        worker_started.set()
        repo.emit_message(_make_state_message())
        worker_done.set()

    t = threading.Thread(target=worker_emit, daemon=True)
    t.start()

    worker_started.wait(timeout=1.0)
    assert not worker_done.wait(timeout=0.5), "Worker should be blocked on full queue"

    queue.get()
    assert worker_done.wait(timeout=2.0), "Worker should complete after queue space freed"
    t.join(timeout=2.0)


def test_main_thread_does_not_deadlock_on_full_queue(
    small_queue: Queue, repo: ConcurrentMessageRepository
) -> None:
    """Simulate the deadlock: main thread emits on a full queue.

    Without the fix this would hang forever because the main thread
    (sole consumer) blocks on queue.put() and nobody drains the queue.
    """
    small_queue.put("record_1")
    small_queue.put("record_2")
    assert small_queue.full()

    state_msg = _make_state_message("contact_lists")
    repo.emit_message(state_msg)  # Must return immediately

    pending = list(repo.consume_queue())
    assert len(pending) == 1
    assert pending[0] == state_msg


def test_ordering_preserved_across_queue_and_pending(
    small_queue: Queue, repo: ConcurrentMessageRepository
) -> None:
    """Messages maintain order: first queued directly, overflow to pending."""
    msg1 = _make_state_message("stream_1")
    msg2 = _make_state_message("stream_2")
    msg3 = _make_state_message("stream_3")

    repo.emit_message(msg1)
    repo.emit_message(msg2)
    assert small_queue.qsize() == 2

    repo.emit_message(msg3)
    assert len(repo._pending) == 1

    from_queue = [small_queue.get_nowait(), small_queue.get_nowait()]
    from_pending = list(repo.consume_queue())
    all_messages = from_queue + from_pending

    assert all_messages == [msg1, msg2, msg3]
