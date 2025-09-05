#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
from datetime import timedelta
from unittest.mock import Mock

import pytest

from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.declarative.async_job.job_orchestrator import (
    AsyncJobOrchestrator,
)
from airbyte_cdk.sources.declarative.async_job.job_tracker import JobTracker
from airbyte_cdk.sources.declarative.incremental import (
    ConcurrentCursorFactory,
    ConcurrentPerPartitionCursor,
)
from airbyte_cdk.sources.declarative.models import (
    CustomRetriever,
    DeclarativeStream,
    ParentStreamConfig,
)
from airbyte_cdk.sources.declarative.partition_routers import (
    AsyncJobPartitionRouter,
    SubstreamPartitionRouter,
)
from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.declarative.partition_routers.single_partition_router import (
    SinglePartitionRouter,
)
from airbyte_cdk.sources.declarative.stream_slicers import (
    StreamSlicer,
    StreamSlicerTestReadDecorator,
)
from airbyte_cdk.sources.message import MessageRepository, NoopMessageRepository
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor, Cursor, CursorField
from airbyte_cdk.sources.streams.concurrent.state_converters.datetime_stream_state_converter import (
    CustomFormatConcurrentStreamStateConverter,
)
from airbyte_cdk.sources.types import StreamSlice
from airbyte_cdk.utils.datetime_helpers import ab_datetime_parse
from unit_tests.sources.declarative.async_job.test_integration import MockAsyncJobRepository

CURSOR_SLICE_FIELD = "cursor slice field"
_NO_LIMIT = 10000
DATE_FORMAT = "%Y-%m-%d"


class MockedCursorBuilder:
    def __init__(self):
        self._stream_slices = []
        self._stream_state = {}

    def with_stream_slices(self, stream_slices):
        self._stream_slices = stream_slices
        return self

    def with_stream_state(self, stream_state):
        self._stream_state = stream_state
        return self

    def build(self):
        cursor = Mock(spec=Cursor)
        cursor.get_stream_state.return_value = self._stream_state
        cursor.stream_slices.return_value = self._stream_slices
        return cursor


def mocked_partition_router():
    return Mock(spec=PartitionRouter)


def mocked_message_repository() -> MessageRepository:
    return Mock(spec=MessageRepository)


def mocked_connector_state_manager() -> ConnectorStateManager:
    return Mock(spec=ConnectorStateManager)


def concurrent_cursor_factory() -> ConcurrentCursor:
    state_converter = CustomFormatConcurrentStreamStateConverter(
        datetime_format=DATE_FORMAT,
        is_sequential_state=False,
    )

    return ConcurrentCursor(
        stream_name="test",
        stream_namespace="",
        stream_state={},
        message_repository=mocked_message_repository(),
        connector_state_manager=mocked_connector_state_manager(),
        connector_state_converter=state_converter,
        cursor_field=CursorField("created_at"),
        slice_boundary_fields=None,
        start=ab_datetime_parse("2021-01-01"),
        end_provider=state_converter.get_end_provider(),
    )


def create_substream_partition_router():
    return SubstreamPartitionRouter(
        config={},
        parameters={},
        parent_stream_configs=[
            ParentStreamConfig(
                type="ParentStreamConfig",
                parent_key="id",
                partition_field="id",
                stream=DeclarativeStream(
                    type="DeclarativeStream",
                    retriever=CustomRetriever(type="CustomRetriever", class_name="a_class_name"),
                ),
            )
        ],
    )


def test_isinstance_global_cursor_async_job_partition_router():
    async_job_partition_router = AsyncJobPartitionRouter(
        stream_slicer=SinglePartitionRouter(parameters={}),
        job_orchestrator_factory=lambda stream_slices: AsyncJobOrchestrator(
            MockAsyncJobRepository(),
            stream_slices,
            JobTracker(_NO_LIMIT),
            NoopMessageRepository(),
        ),
        config={},
        parameters={},
    )

    wrapped_slicer = StreamSlicerTestReadDecorator(
        wrapped_slicer=async_job_partition_router,
        maximum_number_of_slices=5,
    )
    assert isinstance(wrapped_slicer, AsyncJobPartitionRouter)
    assert isinstance(wrapped_slicer.wrapped_slicer, AsyncJobPartitionRouter)
    assert isinstance(wrapped_slicer, StreamSlicerTestReadDecorator)

    assert not isinstance(wrapped_slicer.wrapped_slicer, StreamSlicerTestReadDecorator)
    assert not isinstance(wrapped_slicer, ConcurrentPerPartitionCursor)
    assert not isinstance(wrapped_slicer.wrapped_slicer, ConcurrentPerPartitionCursor)
    assert not isinstance(wrapped_slicer, SubstreamPartitionRouter)
    assert not isinstance(wrapped_slicer.wrapped_slicer, SubstreamPartitionRouter)

    assert isinstance(async_job_partition_router, AsyncJobPartitionRouter)
    assert not isinstance(async_job_partition_router, StreamSlicerTestReadDecorator)
    assert not isinstance(async_job_partition_router, ConcurrentPerPartitionCursor)
    assert not isinstance(async_job_partition_router, SubstreamPartitionRouter)


def test_isinstance_substream_partition_router():
    partition_router = create_substream_partition_router()

    wrapped_slicer = StreamSlicerTestReadDecorator(
        wrapped_slicer=partition_router,
        maximum_number_of_slices=5,
    )

    assert isinstance(wrapped_slicer, SubstreamPartitionRouter)
    assert isinstance(wrapped_slicer.wrapped_slicer, SubstreamPartitionRouter)
    assert isinstance(wrapped_slicer, StreamSlicerTestReadDecorator)

    assert not isinstance(wrapped_slicer.wrapped_slicer, StreamSlicerTestReadDecorator)
    assert not isinstance(wrapped_slicer, ConcurrentPerPartitionCursor)
    assert not isinstance(wrapped_slicer.wrapped_slicer, ConcurrentPerPartitionCursor)
    assert not isinstance(wrapped_slicer, AsyncJobPartitionRouter)
    assert not isinstance(wrapped_slicer.wrapped_slicer, AsyncJobPartitionRouter)

    assert isinstance(partition_router, SubstreamPartitionRouter)
    assert not isinstance(partition_router, StreamSlicerTestReadDecorator)
    assert not isinstance(partition_router, ConcurrentPerPartitionCursor)
    assert not isinstance(partition_router, AsyncJobPartitionRouter)


@pytest.mark.parametrize(
    "use_global_cursor",
    [
        pytest.param(True, id="test_with_global_cursor"),
        pytest.param(False, id="test_with_no_global_cursor"),
    ],
)
def test_isinstance_concurrent_per_partition_cursor(use_global_cursor):
    partition_router = create_substream_partition_router()
    cursor_factory = ConcurrentCursorFactory(concurrent_cursor_factory)
    connector_state_converter = CustomFormatConcurrentStreamStateConverter(
        datetime_format="%Y-%m-%dT%H:%M:%SZ",
        input_datetime_formats=["%Y-%m-%dT%H:%M:%SZ"],
        is_sequential_state=True,
        cursor_granularity=timedelta(0),
    )

    substream_cursor = ConcurrentPerPartitionCursor(
        cursor_factory=cursor_factory,
        partition_router=partition_router,
        stream_name="test",
        stream_namespace="",
        stream_state={},
        message_repository=mocked_message_repository(),
        connector_state_manager=mocked_connector_state_manager(),
        connector_state_converter=connector_state_converter,
        cursor_field=CursorField(cursor_field_key="updated_at"),
        use_global_cursor=use_global_cursor,
    )

    wrapped_slicer = StreamSlicerTestReadDecorator(
        wrapped_slicer=substream_cursor,
        maximum_number_of_slices=5,
    )

    assert isinstance(wrapped_slicer, ConcurrentPerPartitionCursor)
    assert isinstance(wrapped_slicer.wrapped_slicer, ConcurrentPerPartitionCursor)
    assert isinstance(wrapped_slicer, StreamSlicerTestReadDecorator)

    assert not isinstance(wrapped_slicer.wrapped_slicer, StreamSlicerTestReadDecorator)
    assert not isinstance(wrapped_slicer, AsyncJobPartitionRouter)
    assert not isinstance(wrapped_slicer.wrapped_slicer, AsyncJobPartitionRouter)
    assert not isinstance(wrapped_slicer, SubstreamPartitionRouter)
    assert not isinstance(wrapped_slicer.wrapped_slicer, SubstreamPartitionRouter)

    assert wrapped_slicer._cursor_factory == cursor_factory
    assert wrapped_slicer._partition_router == partition_router
    assert wrapped_slicer._use_global_cursor == use_global_cursor

    assert isinstance(substream_cursor, ConcurrentPerPartitionCursor)
    assert not isinstance(substream_cursor, StreamSlicerTestReadDecorator)
    assert not isinstance(substream_cursor, AsyncJobPartitionRouter)
    assert not isinstance(substream_cursor, SubstreamPartitionRouter)

    assert substream_cursor._cursor_factory == cursor_factory
    assert substream_cursor._partition_router == partition_router
    assert wrapped_slicer._use_global_cursor == use_global_cursor

    assert substream_cursor._use_global_cursor == wrapped_slicer._use_global_cursor


def test_slice_limiting_functionality():
    # Create a slicer that returns many slices
    mock_slicer = Mock(spec=StreamSlicer)
    mock_slicer.stream_slices.return_value = [
        StreamSlice(partition={f"key_{i}": f"value_{i}"}, cursor_slice={}) for i in range(10)
    ]

    # Wrap with decorator limiting to 3 slices
    wrapped_slicer = StreamSlicerTestReadDecorator(
        wrapped_slicer=mock_slicer,
        maximum_number_of_slices=3,
    )

    # Verify only 3 slices are returned
    slices = list(wrapped_slicer.stream_slices())
    assert len(slices) == 3
    assert slices == mock_slicer.stream_slices.return_value[:3]
