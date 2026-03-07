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


@pytest.mark.parametrize(
    "pages_per_interval, total_pages, expected_checkpoint_calls",
    [
        pytest.param(5, 4, 0, id="below_interval_no_checkpoint"),
        pytest.param(5, 5, 1, id="exactly_one_interval"),
        pytest.param(5, 10, 2, id="two_intervals"),
        pytest.param(5, 12, 2, id="past_second_interval_but_not_third"),
        pytest.param(3, 9, 3, id="three_intervals_with_smaller_page_size"),
        pytest.param(1, 3, 3, id="checkpoint_every_page"),
    ],
)
def test_on_page_complete_triggers_checkpoint_at_interval(
    pages_per_interval: int, total_pages: int, expected_checkpoint_calls: int
) -> None:
    checkpoint_cursor = Mock(spec=ConcurrentCursor)
    tracker = PaginationTracker(
        checkpoint_cursor=checkpoint_cursor,
        pages_per_checkpoint_interval=pages_per_interval,
    )

    for _ in range(total_pages):
        tracker.on_page_complete(_A_STREAM_SLICE)

    assert checkpoint_cursor.emit_intermediate_state.call_count == expected_checkpoint_calls


def test_on_page_complete_without_checkpoint_cursor_is_noop() -> None:
    tracker = PaginationTracker()
    tracker.on_page_complete(_A_STREAM_SLICE)


def test_on_page_complete_without_interval_is_noop() -> None:
    checkpoint_cursor = Mock(spec=ConcurrentCursor)
    tracker = PaginationTracker(checkpoint_cursor=checkpoint_cursor)
    tracker.on_page_complete(_A_STREAM_SLICE)
    checkpoint_cursor.emit_intermediate_state.assert_not_called()


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

    def test_given_reduce_slice_before_limit_reached_when_has_reached_limit_return_true(self):
        tracker = PaginationTracker(max_number_of_records=2)

        tracker.observe(_A_RECORD)
        tracker.reduce_slice_range_if_possible(_A_STREAM_SLICE, _A_STREAM_SLICE)
        tracker.observe(_A_RECORD)

        assert not tracker.has_reached_limit()

    def test_given_no_cursor_when_reduce_slice_range_then_return_same_slice(self):
        tracker = PaginationTracker()
        original_slice = StreamSlice(partition={}, cursor_slice={})

        result_slice = tracker.reduce_slice_range_if_possible(original_slice, original_slice)

        assert result_slice == original_slice

    def test_given_no_cursor_when_reduce_slice_range_multiple_times_then_raise(self):
        tracker = PaginationTracker()
        original_slice = StreamSlice(partition={}, cursor_slice={})

        tracker.reduce_slice_range_if_possible(original_slice, original_slice)
        with pytest.raises(AirbyteTracedException):
            tracker.reduce_slice_range_if_possible(original_slice, original_slice)

    def test_given_cursor_when_reduce_slice_range_then_return_cursor_stream_slice(self):
        tracker = PaginationTracker(cursor=self._cursor)
        self._cursor.reduce_slice_range.return_value = _A_STREAM_SLICE

        new_slice = tracker.reduce_slice_range_if_possible(
            StreamSlice(partition={}, cursor_slice={}), StreamSlice(partition={}, cursor_slice={})
        )

        assert new_slice == _A_STREAM_SLICE

    def test_given_cursor_cant_reduce_slice_when_reduce_slice_range_then_raise(self):
        tracker = PaginationTracker(cursor=self._cursor)
        original_slice = StreamSlice(partition={}, cursor_slice={})
        self._cursor.reduce_slice_range.return_value = _A_STREAM_SLICE

        with pytest.raises(AirbyteTracedException):
            tracker.reduce_slice_range_if_possible(_A_STREAM_SLICE, original_slice)

    def test_cursor_called_with_original_slice_when_reduce_slice_range(self):
        tracker = PaginationTracker(cursor=self._cursor)
        original_slice = StreamSlice(partition={}, cursor_slice={})

        tracker.reduce_slice_range_if_possible(_A_STREAM_SLICE, original_slice)

        self._cursor.reduce_slice_range.assert_called_once_with(original_slice)
