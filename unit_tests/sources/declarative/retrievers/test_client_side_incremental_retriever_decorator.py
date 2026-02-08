#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock

import pytest

from airbyte_cdk.sources.declarative.retrievers import (
    ClientSideIncrementalRetrieverDecorator,
    Retriever,
)
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor, CursorField
from airbyte_cdk.sources.streams.concurrent.state_converters.datetime_stream_state_converter import (
    CustomFormatConcurrentStreamStateConverter,
)
from airbyte_cdk.sources.types import Record, StreamSlice

DATE_FORMAT = "%Y-%m-%d"


class MockRetriever(Retriever):
    """Mock retriever that yields predefined records."""

    def __init__(self, records: list[dict[str, Any]]):
        self._records = records

    def read_records(
        self,
        records_schema: dict[str, Any],
        stream_slice: StreamSlice | None = None,
    ):
        for record in self._records:
            yield record


@pytest.fixture
def cursor_with_state():
    """Create a cursor with state set to 2021-01-03."""
    return ConcurrentCursor(
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={"created_at": "2021-01-03"},
        message_repository=Mock(),
        connector_state_manager=Mock(),
        connector_state_converter=CustomFormatConcurrentStreamStateConverter(
            datetime_format=DATE_FORMAT
        ),
        cursor_field=CursorField("created_at"),
        slice_boundary_fields=("start", "end"),
        start=datetime(2021, 1, 1, tzinfo=timezone.utc),
        end_provider=lambda: datetime(2021, 1, 10, tzinfo=timezone.utc),
        slice_range=timedelta(days=365 * 10),
    )


@pytest.fixture
def cursor_without_state():
    """Create a cursor without state."""
    return ConcurrentCursor(
        stream_name="test_stream",
        stream_namespace=None,
        stream_state={},
        message_repository=Mock(),
        connector_state_manager=Mock(),
        connector_state_converter=CustomFormatConcurrentStreamStateConverter(
            datetime_format=DATE_FORMAT
        ),
        cursor_field=CursorField("created_at"),
        slice_boundary_fields=("start", "end"),
        start=datetime(2021, 1, 1, tzinfo=timezone.utc),
        end_provider=lambda: datetime(2021, 1, 10, tzinfo=timezone.utc),
        slice_range=timedelta(days=365 * 10),
    )


@pytest.mark.parametrize(
    "records,cursor_state,expected_ids",
    [
        pytest.param(
            [
                {"id": 1, "created_at": "2020-01-03"},
                {"id": 2, "created_at": "2021-01-03"},
                {"id": 3, "created_at": "2021-01-04"},
                {"id": 4, "created_at": "2021-02-01"},
            ],
            {"created_at": "2021-01-03"},
            [2, 3, 4],
            id="filters_records_older_than_cursor_state",
        ),
        pytest.param(
            [
                {"id": 1, "created_at": "2020-01-03"},
                {"id": 2, "created_at": "2021-01-03"},
                {"id": 3, "created_at": "2021-01-04"},
            ],
            {},
            [2, 3],
            id="no_state_uses_start_date_for_filtering",
        ),
        pytest.param(
            [],
            {"created_at": "2021-01-03"},
            [],
            id="empty_records_returns_empty",
        ),
    ],
)
def test_client_side_incremental_retriever_decorator_with_dict_records(
    records: list[dict[str, Any]],
    cursor_state: dict[str, Any],
    expected_ids: list[int],
):
    """Test filtering with dict records."""
    cursor = ConcurrentCursor(
        stream_name="test_stream",
        stream_namespace=None,
        stream_state=cursor_state,
        message_repository=Mock(),
        connector_state_manager=Mock(),
        connector_state_converter=CustomFormatConcurrentStreamStateConverter(
            datetime_format=DATE_FORMAT
        ),
        cursor_field=CursorField("created_at"),
        slice_boundary_fields=("start", "end"),
        start=datetime(2021, 1, 1, tzinfo=timezone.utc),
        end_provider=lambda: datetime(2021, 12, 31, tzinfo=timezone.utc),
        slice_range=timedelta(days=365 * 10),
    )

    mock_retriever = MockRetriever(records)
    decorator = ClientSideIncrementalRetrieverDecorator(
        retriever=mock_retriever,
        cursor=cursor,
    )

    stream_slice = StreamSlice(partition={}, cursor_slice={})
    result = list(decorator.read_records(records_schema={}, stream_slice=stream_slice))

    assert [r["id"] for r in result] == expected_ids


def test_client_side_incremental_retriever_decorator_with_record_objects(
    cursor_with_state,
):
    """Test filtering with Record objects."""
    stream_slice = StreamSlice(partition={}, cursor_slice={})
    records = [
        Record(
            data={"id": 1, "created_at": "2020-01-03"},
            associated_slice=stream_slice,
            stream_name="test_stream",
        ),
        Record(
            data={"id": 2, "created_at": "2021-01-03"},
            associated_slice=stream_slice,
            stream_name="test_stream",
        ),
        Record(
            data={"id": 3, "created_at": "2021-01-04"},
            associated_slice=stream_slice,
            stream_name="test_stream",
        ),
    ]

    class MockRetrieverWithRecords(Retriever):
        def read_records(self, records_schema, stream_slice=None):
            yield from records

    mock_retriever = MockRetrieverWithRecords()
    decorator = ClientSideIncrementalRetrieverDecorator(
        retriever=mock_retriever,
        cursor=cursor_with_state,
    )

    result = list(decorator.read_records(records_schema={}, stream_slice=stream_slice))

    assert [r["id"] for r in result] == [2, 3]


def test_client_side_incremental_retriever_decorator_passes_through_non_record_data(
    cursor_with_state,
):
    """Test that non-dict/non-Record data is passed through unchanged."""
    stream_slice = StreamSlice(partition={}, cursor_slice={})

    class MockRetrieverWithMixedData(Retriever):
        def read_records(self, records_schema, stream_slice=None):
            yield "some_string"
            yield 123
            yield {"id": 1, "created_at": "2021-01-04"}

    mock_retriever = MockRetrieverWithMixedData()
    decorator = ClientSideIncrementalRetrieverDecorator(
        retriever=mock_retriever,
        cursor=cursor_with_state,
    )

    result = list(decorator.read_records(records_schema={}, stream_slice=stream_slice))

    assert result == ["some_string", 123, {"id": 1, "created_at": "2021-01-04"}]
