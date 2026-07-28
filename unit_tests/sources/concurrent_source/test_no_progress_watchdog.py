#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import logging
from queue import Empty, Queue
from unittest.mock import Mock

import pytest

from airbyte_cdk.models import AirbyteMessage, AirbyteRecordMessage, FailureType
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.concurrent_source.concurrent_source import ConcurrentSource
from airbyte_cdk.sources.concurrent_source.thread_pool_manager import ThreadPoolManager
from airbyte_cdk.sources.message import InMemoryMessageRepository
from airbyte_cdk.sources.utils.slice_logger import DebugSliceLogger
from airbyte_cdk.utils import AirbyteTracedException


def _message() -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.RECORD,
        record=AirbyteRecordMessage(stream="stream", data={}, emitted_at=0),
    )


def _source(queue, **kwargs) -> ConcurrentSource:
    return ConcurrentSource(
        threadpool=Mock(spec=ThreadPoolManager),
        logger=Mock(spec=logging.Logger),
        slice_logger=DebugSliceLogger(),
        queue=queue,
        message_repository=InMemoryMessageRepository(),
        timeout_seconds=0.05,
        **kwargs,
    )


def _processor(done: bool = True) -> Mock:
    processor = Mock()
    processor.is_done.return_value = done
    processor.get_in_flight_streams_description.return_value = (
        "Streams generating partitions: ['stream']; streams with running partitions: {'stream': 1}"
    )
    return processor


def test_stalled_queue_logs_warning_without_raising(monkeypatch) -> None:
    queue = Mock()
    queue.get.side_effect = [Empty, Empty, _message()]
    queue.empty.return_value = True
    processor = _processor()
    source = _source(queue)
    monkeypatch.setattr(
        "airbyte_cdk.sources.concurrent_source.concurrent_source.time.monotonic",
        Mock(side_effect=[0, 0.06, 0.12, 0.12]),
    )

    assert list(source._consume_from_queue(queue, processor))
    assert source._logger.warning.call_count >= 1


def test_stalled_queue_raises_with_constructor_timeout(monkeypatch) -> None:
    queue = Mock()
    queue.get.side_effect = Empty
    source = _source(queue, no_progress_timeout_seconds=1)
    processor = _processor()
    monkeypatch.setattr(
        "airbyte_cdk.sources.concurrent_source.concurrent_source.time.monotonic",
        Mock(side_effect=[0, 1]),
    )

    with pytest.raises(AirbyteTracedException) as exc_info:
        list(source._consume_from_queue(queue, processor))

    assert exc_info.value.failure_type == FailureType.system_error
    assert exc_info.value.message == "Source made no progress for 1 seconds and was stopped."
    source._threadpool.shutdown.assert_called_once_with()


def test_stalled_queue_raises_with_environment_timeout(monkeypatch) -> None:
    monkeypatch.setenv("AIRBYTE_NO_PROGRESS_TIMEOUT_SECONDS", "1")
    queue = Mock()
    queue.get.side_effect = Empty
    source = _source(queue)
    processor = _processor()
    monkeypatch.setattr(
        "airbyte_cdk.sources.concurrent_source.concurrent_source.time.monotonic",
        Mock(side_effect=[0, 1]),
    )

    with pytest.raises(AirbyteTracedException) as exc_info:
        list(source._consume_from_queue(queue, processor))

    assert source._no_progress_timeout_seconds == 1
    assert exc_info.value.failure_type == FailureType.system_error


def test_normal_read_and_slow_progressing_read_complete() -> None:
    queue = Queue()
    queue.put(_message())
    processor = _processor()
    source = _source(queue, no_progress_timeout_seconds=1)

    assert list(source._consume_from_queue(queue, processor))

    queue = Mock()
    queue.get.side_effect = [_message(), Empty, _message()]
    queue.empty.side_effect = [True, True]
    processor = _processor()
    processor.is_done.side_effect = [False, True]
    source = _source(queue, no_progress_timeout_seconds=1)
    source._logger = Mock(spec=logging.Logger)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "airbyte_cdk.sources.concurrent_source.concurrent_source.time.monotonic",
            Mock(side_effect=[0, 0, 0.06, 0.06]),
        )
        assert len(list(source._consume_from_queue(queue, processor))) == 2
    source._threadpool.shutdown.assert_not_called()
