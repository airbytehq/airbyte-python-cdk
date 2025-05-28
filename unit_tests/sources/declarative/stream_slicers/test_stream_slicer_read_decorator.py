#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from unittest.mock import Mock

from airbyte_cdk.sources.declarative.incremental import PerPartitionWithGlobalCursor
from airbyte_cdk.sources.declarative.incremental.declarative_cursor import DeclarativeCursor
from airbyte_cdk.sources.declarative.incremental.global_substream_cursor import (
    GlobalSubstreamCursor,
)
from airbyte_cdk.sources.declarative.incremental.per_partition_cursor import (
    StreamSlice,
)
from airbyte_cdk.sources.declarative.partition_routers import AsyncJobPartitionRouter
from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.declarative.stream_slicers import StreamSlicerTestReadDecorator

CURSOR_SLICE_FIELD = "cursor slice field"


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


def test_show_as_wrapped_instance():
    first_partition = {"first_partition_key": "first_partition_value"}
    mocked_partition_router().stream_slices.return_value = [
        StreamSlice(
            partition=first_partition, cursor_slice={}, extra_fields={"extra_field": "extra_value"}
        ),
    ]
    cursor = (
        MockedCursorBuilder()
        .with_stream_slices([{CURSOR_SLICE_FIELD: "first slice cursor value"}])
        .build()
    )

    global_cursor = GlobalSubstreamCursor(cursor, mocked_partition_router)
    wrapped_slicer = StreamSlicerTestReadDecorator(
        wrapped_slicer=global_cursor,
        maximum_number_of_slices=5,
    )
    assert isinstance(wrapped_slicer, GlobalSubstreamCursor)
    assert not isinstance(wrapped_slicer, AsyncJobPartitionRouter)
    assert not isinstance(wrapped_slicer, PerPartitionWithGlobalCursor)

    assert isinstance(global_cursor, GlobalSubstreamCursor)
    assert not isinstance(global_cursor, AsyncJobPartitionRouter)
    assert not isinstance(global_cursor, PerPartitionWithGlobalCursor)
