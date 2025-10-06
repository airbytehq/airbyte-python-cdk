#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
from unittest import TestCase
from unittest.mock import Mock

import pytest

from airbyte_cdk.sources.declarative.models import FailureType
from airbyte_cdk.sources.declarative.retrievers.pagination_tracker import PaginationTracker
from airbyte_cdk.sources.declarative.types import Record, StreamSlice
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

_A_RECORD = Record(
    data={"id": 1},
    associated_slice=StreamSlice(partition={"id": 11}, cursor_slice={}),
    stream_name="a_stream_name",
)
_A_STREAM_SLICE = StreamSlice(cursor_slice={"stream slice": "slice value"}, partition={})


class TestPaginationTracker(TestCase):
    def setUp(self) -> None:
        self._cursor = Mock(spec=ConcurrentCursor)

    def test_given_cursor_when_observe_then_forward_to_cursor(self):
        tracker = PaginationTracker(cursor=self._cursor)

        tracker.observe(_A_RECORD)

        self._cursor.observe.assert_called_once_with(_A_RECORD)

    def test_given_not_enough_records_when_has_reached_limit_return_false(self):
        tracker = PaginationTracker(max_number_of_records=100)
        tracker.observe(_A_RECORD)
        assert not tracker.has_reached_limit()

    def test_given_enough_records_when_has_reached_limit_return_true(self):
        tracker = PaginationTracker(max_number_of_records=2)

        tracker.observe(_A_RECORD)
        tracker.observe(_A_RECORD)

        assert tracker.has_reached_limit()

    def test_given_reset_before_limit_reached_when_has_reached_limit_return_true(self):
        tracker = PaginationTracker(max_number_of_records=2)

        tracker.observe(_A_RECORD)
        tracker.reset()
        tracker.observe(_A_RECORD)

        assert not tracker.has_reached_limit()

    def test_given_no_cursor_when_reduce_slice_range_then_return_same_slice(self):
        tracker = PaginationTracker()
        original_slice = StreamSlice(partition={}, cursor_slice={})

        result_slice = tracker.reduce_slice_range_if_possible(original_slice)

        assert result_slice == original_slice

    def test_given_no_cursor_when_reduce_slice_range_multiple_times_then_raise(self):
        tracker = PaginationTracker()
        original_slice = StreamSlice(partition={}, cursor_slice={})

        tracker.reduce_slice_range_if_possible(original_slice)
        with pytest.raises(AirbyteTracedException):
            tracker.reduce_slice_range_if_possible(original_slice)

    def test_given_cursor_when_reduce_slice_range_then_return_cursor_stream_slice(self):
        tracker = PaginationTracker(cursor=self._cursor)
        self._cursor.reduce_slice_range.return_value = _A_STREAM_SLICE

        new_slice = tracker.reduce_slice_range_if_possible(
            StreamSlice(partition={}, cursor_slice={})
        )

        assert new_slice == _A_STREAM_SLICE

    def test_given_cursor_cant_reduce_slice_when_reduce_slice_range_then_raise(self):
        tracker = PaginationTracker(cursor=self._cursor)
        self._cursor.reduce_slice_range.return_value = _A_STREAM_SLICE

        with pytest.raises(AirbyteTracedException):
            tracker.reduce_slice_range_if_possible(_A_STREAM_SLICE)
