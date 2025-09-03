#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from unittest.mock import Mock

from airbyte_cdk.legacy.sources.declarative.incremental import (
    CursorFactory,
    DatetimeBasedCursor,
    GlobalSubstreamCursor,
    PerPartitionWithGlobalCursor,
)
from airbyte_cdk.legacy.sources.declarative.incremental.declarative_cursor import DeclarativeCursor
from airbyte_cdk.sources.declarative.async_job.job_orchestrator import (
    AsyncJobOrchestrator,
)
from airbyte_cdk.sources.declarative.async_job.job_tracker import JobTracker
from airbyte_cdk.sources.declarative.datetime.min_max_datetime import MinMaxDatetime
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
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
from airbyte_cdk.sources.message import NoopMessageRepository
from airbyte_cdk.sources.types import StreamSlice
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
        cursor = Mock(spec=DeclarativeCursor)
        cursor.get_stream_state.return_value = self._stream_state
        cursor.stream_slices.return_value = self._stream_slices
        return cursor


def mocked_partition_router():
    return Mock(spec=PartitionRouter)


def date_time_based_cursor_factory() -> DatetimeBasedCursor:
    return DatetimeBasedCursor(
        start_datetime=MinMaxDatetime(
            datetime="2021-01-01", datetime_format=DATE_FORMAT, parameters={}
        ),
        end_datetime=MinMaxDatetime(
            datetime="2021-01-05", datetime_format=DATE_FORMAT, parameters={}
        ),
        step="P10Y",
        cursor_field=InterpolatedString.create("created_at", parameters={}),
        datetime_format=DATE_FORMAT,
        cursor_granularity="P1D",
        config={},
        parameters={},
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


def test_isinstance_global_cursor():
    first_partition = {"first_partition_key": "first_partition_value"}
    partition_router = mocked_partition_router()
    partition_router.stream_slices.return_value = [
        StreamSlice(
            partition=first_partition, cursor_slice={}, extra_fields={"extra_field": "extra_value"}
        ),
    ]
    cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )

    global_cursor = GlobalSubstreamCursor(cursor, partition_router)
    wrapped_slicer = StreamSlicerTestReadDecorator(
        wrapped_slicer=global_cursor,
        maximum_number_of_slices=5,
    )
    assert isinstance(wrapped_slicer, GlobalSubstreamCursor)
    assert isinstance(wrapped_slicer.wrapped_slicer, GlobalSubstreamCursor)
    assert isinstance(wrapped_slicer, StreamSlicerTestReadDecorator)

    assert not isinstance(wrapped_slicer.wrapped_slicer, StreamSlicerTestReadDecorator)
    assert not isinstance(wrapped_slicer, AsyncJobPartitionRouter)
    assert not isinstance(wrapped_slicer.wrapped_slicer, AsyncJobPartitionRouter)
    assert not isinstance(wrapped_slicer, PerPartitionWithGlobalCursor)
    assert not isinstance(wrapped_slicer.wrapped_slicer, PerPartitionWithGlobalCursor)
    assert not isinstance(wrapped_slicer, SubstreamPartitionRouter)
    assert not isinstance(wrapped_slicer.wrapped_slicer, SubstreamPartitionRouter)

    assert isinstance(global_cursor, GlobalSubstreamCursor)
    assert not isinstance(global_cursor, StreamSlicerTestReadDecorator)
    assert not isinstance(global_cursor, AsyncJobPartitionRouter)
    assert not isinstance(global_cursor, PerPartitionWithGlobalCursor)
    assert not isinstance(global_cursor, SubstreamPartitionRouter)


def test_isinstance_global_cursor_aysnc_job_partition_router():
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
    assert not isinstance(wrapped_slicer, GlobalSubstreamCursor)
    assert not isinstance(wrapped_slicer.wrapped_slicer, GlobalSubstreamCursor)
    assert not isinstance(wrapped_slicer, PerPartitionWithGlobalCursor)
    assert not isinstance(wrapped_slicer.wrapped_slicer, PerPartitionWithGlobalCursor)
    assert not isinstance(wrapped_slicer, SubstreamPartitionRouter)
    assert not isinstance(wrapped_slicer.wrapped_slicer, SubstreamPartitionRouter)

    assert isinstance(async_job_partition_router, AsyncJobPartitionRouter)
    assert not isinstance(async_job_partition_router, StreamSlicerTestReadDecorator)
    assert not isinstance(async_job_partition_router, GlobalSubstreamCursor)
    assert not isinstance(async_job_partition_router, PerPartitionWithGlobalCursor)
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
    assert not isinstance(wrapped_slicer, GlobalSubstreamCursor)
    assert not isinstance(wrapped_slicer.wrapped_slicer, GlobalSubstreamCursor)
    assert not isinstance(wrapped_slicer, AsyncJobPartitionRouter)
    assert not isinstance(wrapped_slicer.wrapped_slicer, AsyncJobPartitionRouter)
    assert not isinstance(wrapped_slicer, PerPartitionWithGlobalCursor)
    assert not isinstance(wrapped_slicer.wrapped_slicer, PerPartitionWithGlobalCursor)

    assert isinstance(partition_router, SubstreamPartitionRouter)
    assert not isinstance(partition_router, StreamSlicerTestReadDecorator)
    assert not isinstance(partition_router, GlobalSubstreamCursor)
    assert not isinstance(partition_router, AsyncJobPartitionRouter)
    assert not isinstance(partition_router, PerPartitionWithGlobalCursor)


def test_isinstance_perpartition_with_global_cursor():
    partition_router = create_substream_partition_router()
    date_time_based_cursor = date_time_based_cursor_factory()

    cursor_factory = CursorFactory(date_time_based_cursor_factory)
    substream_cursor = PerPartitionWithGlobalCursor(
        cursor_factory=cursor_factory,
        partition_router=partition_router,
        stream_cursor=date_time_based_cursor,
    )

    wrapped_slicer = StreamSlicerTestReadDecorator(
        wrapped_slicer=substream_cursor,
        maximum_number_of_slices=5,
    )

    assert isinstance(wrapped_slicer, PerPartitionWithGlobalCursor)
    assert isinstance(wrapped_slicer.wrapped_slicer, PerPartitionWithGlobalCursor)
    assert isinstance(wrapped_slicer, StreamSlicerTestReadDecorator)

    assert not isinstance(wrapped_slicer.wrapped_slicer, StreamSlicerTestReadDecorator)
    assert not isinstance(wrapped_slicer, GlobalSubstreamCursor)
    assert not isinstance(wrapped_slicer.wrapped_slicer, GlobalSubstreamCursor)
    assert not isinstance(wrapped_slicer, AsyncJobPartitionRouter)
    assert not isinstance(wrapped_slicer.wrapped_slicer, AsyncJobPartitionRouter)
    assert not isinstance(wrapped_slicer, SubstreamPartitionRouter)
    assert not isinstance(wrapped_slicer.wrapped_slicer, SubstreamPartitionRouter)

    assert wrapped_slicer._per_partition_cursor._cursor_factory == cursor_factory
    assert wrapped_slicer._partition_router == partition_router
    assert wrapped_slicer._global_cursor._stream_cursor == date_time_based_cursor

    assert isinstance(substream_cursor, PerPartitionWithGlobalCursor)
    assert not isinstance(substream_cursor, StreamSlicerTestReadDecorator)
    assert not isinstance(substream_cursor, GlobalSubstreamCursor)
    assert not isinstance(substream_cursor, AsyncJobPartitionRouter)
    assert not isinstance(substream_cursor, SubstreamPartitionRouter)

    assert substream_cursor._per_partition_cursor._cursor_factory == cursor_factory
    assert substream_cursor._partition_router == partition_router
    assert substream_cursor._global_cursor._stream_cursor == date_time_based_cursor

    assert substream_cursor._get_active_cursor() == wrapped_slicer._get_active_cursor()


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
