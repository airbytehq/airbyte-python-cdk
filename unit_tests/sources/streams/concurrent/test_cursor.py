#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, Mapping, Optional
from unittest import TestCase
from unittest.mock import Mock

import freezegun
import pytest
from isodate import parse_duration

from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.declarative.datetime.min_max_datetime import MinMaxDatetime
from airbyte_cdk.sources.declarative.incremental.datetime_based_cursor import DatetimeBasedCursor
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.concurrent.clamping import (
    ClampingEndProvider,
    ClampingStrategy,
    MonthClampingStrategy,
    WeekClampingStrategy,
    Weekday,
)
from airbyte_cdk.sources.streams.concurrent.cursor import (
    ConcurrentCursor,
    CursorField,
    CursorValueType,
)
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter import (
    ConcurrencyCompatibleStateType,
)
from airbyte_cdk.sources.streams.concurrent.state_converters.datetime_stream_state_converter import (
    CustomFormatConcurrentStreamStateConverter,
    EpochValueConcurrentStreamStateConverter,
    IsoMillisConcurrentStreamStateConverter,
)
from airbyte_cdk.sources.types import Record, StreamSlice

_A_STREAM_NAME = "a stream name"
_A_STREAM_NAMESPACE = "a stream namespace"
_A_CURSOR_FIELD_KEY = "a_cursor_field_key"
_NO_STATE = {}
_NO_PARTITION_IDENTIFIER = None
_NO_SLICE = None
_NO_SLICE_BOUNDARIES = None
_NOT_SEQUENTIAL = False
_LOWER_SLICE_BOUNDARY_FIELD = "lower_boundary"
_UPPER_SLICE_BOUNDARY_FIELD = "upper_boundary"
_SLICE_BOUNDARY_FIELDS = (_LOWER_SLICE_BOUNDARY_FIELD, _UPPER_SLICE_BOUNDARY_FIELD)
_A_VERY_HIGH_CURSOR_VALUE = 1000000000
_NO_LOOKBACK_WINDOW = timedelta(seconds=0)


def _partition(
    _slice: Optional[Mapping[str, Any]], _stream_name: Optional[str] = Mock()
) -> Partition:
    partition = Mock(spec=Partition)
    partition.to_slice.return_value = _slice
    partition.stream_name.return_value = _stream_name
    return partition


def _record(
    cursor_value: CursorValueType, partition: Optional[Partition] = Mock(spec=Partition)
) -> Record:
    return Record(
        data={_A_CURSOR_FIELD_KEY: cursor_value},
        associated_slice=partition.to_slice(),
        stream_name=_A_STREAM_NAME,
    )


class ConcurrentCursorStateTest(TestCase):
    def setUp(self) -> None:
        self._message_repository = Mock(spec=MessageRepository)
        self._state_manager = Mock(spec=ConnectorStateManager)

    def _cursor_with_slice_boundary_fields(
        self, is_sequential_state: bool = True
    ) -> ConcurrentCursor:
        return ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            deepcopy(_NO_STATE),
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            None,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
        )

    def _cursor_without_slice_boundary_fields(self) -> ConcurrentCursor:
        return ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            deepcopy(_NO_STATE),
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=True),
            CursorField(_A_CURSOR_FIELD_KEY),
            None,
            None,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
        )

    def test_given_no_cursor_value_when_observe_then_do_not_raise(self) -> None:
        cursor = self._cursor_with_slice_boundary_fields()
        partition = _partition(_NO_SLICE)

        cursor.observe(
            Record(
                data={"record_with_A_CURSOR_FIELD_KEY": "any value"},
                associated_slice=partition.to_slice(),
                stream_name=_A_STREAM_NAME,
            )
        )

        # did not raise

    def test_given_boundary_fields_when_close_partition_then_emit_state(self) -> None:
        cursor = self._cursor_with_slice_boundary_fields()
        cursor.close_partition(
            _partition(
                StreamSlice(
                    partition={_LOWER_SLICE_BOUNDARY_FIELD: 12, _UPPER_SLICE_BOUNDARY_FIELD: 30},
                    cursor_slice={},
                ),
            )
        )

        self._message_repository.emit_message.assert_called_once_with(
            self._state_manager.create_state_message.return_value
        )
        self._state_manager.update_state_for_stream.assert_called_once_with(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                _A_CURSOR_FIELD_KEY: 0
            },  # State message is updated to the legacy format before being emitted
        )

    def test_given_state_not_sequential_when_close_partition_then_emit_state(self) -> None:
        cursor = self._cursor_with_slice_boundary_fields(is_sequential_state=False)
        cursor.close_partition(
            _partition(
                StreamSlice(
                    partition={_LOWER_SLICE_BOUNDARY_FIELD: 12, _UPPER_SLICE_BOUNDARY_FIELD: 30},
                    cursor_slice={},
                ),
            )
        )

        self._message_repository.emit_message.assert_called_once_with(
            self._state_manager.create_state_message.return_value
        )
        self._state_manager.update_state_for_stream.assert_called_once_with(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "slices": [
                    {"end": 0, "most_recent_cursor_value": 0, "start": 0},
                    {"end": 30, "start": 12},
                ],
                "state_type": "date-range",
            },
        )

    def test_close_partition_emits_message_to_lower_boundary_when_no_prior_state_exists(
        self,
    ) -> None:
        self._cursor_with_slice_boundary_fields().close_partition(
            _partition(
                StreamSlice(
                    partition={_LOWER_SLICE_BOUNDARY_FIELD: 0, _UPPER_SLICE_BOUNDARY_FIELD: 30},
                    cursor_slice={},
                ),
            )
        )

        self._message_repository.emit_message.assert_called_once_with(
            self._state_manager.create_state_message.return_value
        )
        self._state_manager.update_state_for_stream.assert_called_once_with(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {_A_CURSOR_FIELD_KEY: 0},  # State message is updated to the lower slice boundary
        )

    def test_given_boundary_fields_and_record_observed_when_close_partition_then_ignore_records(
        self,
    ) -> None:
        cursor = self._cursor_with_slice_boundary_fields()
        cursor.observe(_record(_A_VERY_HIGH_CURSOR_VALUE))

        cursor.close_partition(
            _partition(
                StreamSlice(
                    partition={_LOWER_SLICE_BOUNDARY_FIELD: 12, _UPPER_SLICE_BOUNDARY_FIELD: 30},
                    cursor_slice={},
                )
            )
        )

        assert (
            self._state_manager.update_state_for_stream.call_args_list[0].args[2][
                _A_CURSOR_FIELD_KEY
            ]
            != _A_VERY_HIGH_CURSOR_VALUE
        )

    def test_given_no_boundary_fields_when_close_partition_then_emit_state(self) -> None:
        cursor = self._cursor_without_slice_boundary_fields()
        partition = _partition(_NO_SLICE)
        cursor.observe(_record(10, partition=partition))
        cursor.close_partition(partition)

        self._state_manager.update_state_for_stream.assert_called_once_with(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {"a_cursor_field_key": 10},
        )

    def test_given_no_boundary_fields_when_close_multiple_partitions_then_raise_exception(
        self,
    ) -> None:
        cursor = self._cursor_without_slice_boundary_fields()
        partition = _partition(_NO_SLICE)
        cursor.observe(_record(10, partition=partition))
        cursor.close_partition(partition)

        with pytest.raises(ValueError):
            cursor.close_partition(partition)

    def test_given_no_records_observed_when_close_partition_then_do_not_emit_state(self) -> None:
        cursor = self._cursor_without_slice_boundary_fields()
        cursor.close_partition(_partition(_NO_SLICE))
        assert self._message_repository.emit_message.call_count == 0

    def test_given_slice_boundaries_and_no_slice_when_close_partition_then_raise_error(
        self,
    ) -> None:
        cursor = self._cursor_with_slice_boundary_fields()
        with pytest.raises(KeyError):
            cursor.close_partition(_partition(_NO_SLICE))

    def test_given_slice_boundaries_not_matching_slice_when_close_partition_then_raise_error(
        self,
    ) -> None:
        cursor = self._cursor_with_slice_boundary_fields()
        with pytest.raises(KeyError):
            cursor.close_partition(
                _partition(StreamSlice(partition={"not_matching_key": "value"}, cursor_slice={}))
            )

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_no_state_when_generate_slices_then_create_slice_from_start_to_end(self):
        start = datetime.fromtimestamp(10, timezone.utc)
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            deepcopy(_NO_STATE),
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 10,
                    _SLICE_BOUNDARY_FIELDS[1]: 50,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_one_slice_when_generate_slices_then_create_slice_from_slice_upper_boundary_to_end(
        self,
    ):
        start = datetime.fromtimestamp(0, timezone.utc)
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 0,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 20,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 20,
                    _SLICE_BOUNDARY_FIELDS[1]: 50,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_start_after_slices_when_generate_slices_then_generate_from_start(self):
        start = datetime.fromtimestamp(30, timezone.utc)
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 0,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 20,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 30,
                    _SLICE_BOUNDARY_FIELDS[1]: 50,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_state_with_gap_and_start_after_slices_when_generate_slices_then_generate_from_start(
        self,
    ):
        start = datetime.fromtimestamp(30, timezone.utc)
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 0,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 10,
                    },
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 15,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 20,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 30,
                    _SLICE_BOUNDARY_FIELDS[1]: 50,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_small_slice_range_when_generate_slices_then_create_many_slices(self):
        start = datetime.fromtimestamp(0, timezone.utc)
        small_slice_range = timedelta(seconds=10)
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 0,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 20,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
            small_slice_range,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 20,
                    _SLICE_BOUNDARY_FIELDS[1]: 30,
                },
            ),
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 30,
                    _SLICE_BOUNDARY_FIELDS[1]: 40,
                },
            ),
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 40,
                    _SLICE_BOUNDARY_FIELDS[1]: 50,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_difference_between_slices_match_slice_range_when_generate_slices_then_create_one_slice(
        self,
    ):
        start = datetime.fromtimestamp(0, timezone.utc)
        small_slice_range = timedelta(seconds=10)
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 0,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 30,
                    },
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 40,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 50,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
            small_slice_range,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 30,
                    _SLICE_BOUNDARY_FIELDS[1]: 40,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_small_slice_range_with_granularity_when_generate_slices_then_create_many_slices(
        self,
    ):
        start = datetime.fromtimestamp(1, timezone.utc)
        small_slice_range = timedelta(seconds=10)
        granularity = timedelta(seconds=1)
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 1,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 20,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
            small_slice_range,
            granularity,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 20,
                    _SLICE_BOUNDARY_FIELDS[1]: 29,
                },
            ),
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 30,
                    _SLICE_BOUNDARY_FIELDS[1]: 39,
                },
            ),
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 40,
                    _SLICE_BOUNDARY_FIELDS[1]: 50,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_difference_between_slices_match_slice_range_and_cursor_granularity_when_generate_slices_then_create_one_slice(
        self,
    ):
        start = datetime.fromtimestamp(1, timezone.utc)
        small_slice_range = timedelta(seconds=10)
        granularity = timedelta(seconds=1)
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 1,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 30,
                    },
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 41,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 50,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
            small_slice_range,
            granularity,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 31,
                    _SLICE_BOUNDARY_FIELDS[1]: 40,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_non_continuous_state_when_generate_slices_then_create_slices_between_gaps_and_after(
        self,
    ):
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 0,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 10,
                    },
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 20,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 25,
                    },
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 30,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 40,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            None,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 10,
                    _SLICE_BOUNDARY_FIELDS[1]: 20,
                },
            ),
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 25,
                    _SLICE_BOUNDARY_FIELDS[1]: 30,
                },
            ),
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 40,
                    _SLICE_BOUNDARY_FIELDS[1]: 50,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_lookback_window_when_generate_slices_then_apply_lookback_on_most_recent_slice(
        self,
    ):
        start = datetime.fromtimestamp(0, timezone.utc)
        lookback_window = timedelta(seconds=10)
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 0,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 20,
                    },
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 30,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 40,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            lookback_window,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 20,
                    _SLICE_BOUNDARY_FIELDS[1]: 30,
                },
            ),
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 30,
                    _SLICE_BOUNDARY_FIELDS[1]: 50,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_start_is_before_first_slice_lower_boundary_when_generate_slices_then_generate_slice_before(
        self,
    ):
        start = datetime.fromtimestamp(0, timezone.utc)
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 10,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 20,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 0,
                    _SLICE_BOUNDARY_FIELDS[1]: 10,
                },
            ),
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 20,
                    _SLICE_BOUNDARY_FIELDS[1]: 50,
                },
            ),
        ]

    def test_slices_with_records_when_close_then_most_recent_cursor_value_from_most_recent_slice(
        self,
    ) -> None:
        cursor = self._cursor_with_slice_boundary_fields(is_sequential_state=False)
        first_partition = _partition(
            StreamSlice(
                partition={_LOWER_SLICE_BOUNDARY_FIELD: 0, _UPPER_SLICE_BOUNDARY_FIELD: 10},
                cursor_slice={},
            )
        )
        second_partition = _partition(
            StreamSlice(
                partition={_LOWER_SLICE_BOUNDARY_FIELD: 10, _UPPER_SLICE_BOUNDARY_FIELD: 20},
                cursor_slice={},
            )
        )
        cursor.observe(_record(5, partition=first_partition))
        cursor.close_partition(first_partition)

        cursor.observe(_record(15, partition=second_partition))
        cursor.close_partition(second_partition)

        assert self._state_manager.update_state_for_stream.call_args_list[-1].args[2] == {
            "slices": [{"end": 20, "start": 0, "most_recent_cursor_value": 15}],
            "state_type": "date-range",
        }

    def test_last_slice_without_records_when_close_then_most_recent_cursor_value_is_from_previous_slice(
        self,
    ) -> None:
        cursor = self._cursor_with_slice_boundary_fields(is_sequential_state=False)
        first_partition = _partition(
            StreamSlice(
                partition={_LOWER_SLICE_BOUNDARY_FIELD: 0, _UPPER_SLICE_BOUNDARY_FIELD: 10},
                cursor_slice={},
            )
        )
        second_partition = _partition(
            StreamSlice(
                partition={_LOWER_SLICE_BOUNDARY_FIELD: 10, _UPPER_SLICE_BOUNDARY_FIELD: 20},
                cursor_slice={},
            )
        )
        cursor.observe(_record(5, partition=first_partition))
        cursor.close_partition(first_partition)

        cursor.close_partition(second_partition)

        assert self._state_manager.update_state_for_stream.call_args_list[-1].args[2] == {
            "slices": [{"end": 20, "start": 0, "most_recent_cursor_value": 5}],
            "state_type": "date-range",
        }

    def test_most_recent_cursor_value_outside_of_boundaries_when_close_then_most_recent_cursor_value_still_considered(
        self,
    ) -> None:
        """
        Not sure what is the value of this behavior but I'm simply documenting how it is today
        """
        cursor = self._cursor_with_slice_boundary_fields(is_sequential_state=False)
        partition = _partition(
            StreamSlice(
                partition={_LOWER_SLICE_BOUNDARY_FIELD: 0, _UPPER_SLICE_BOUNDARY_FIELD: 10},
                cursor_slice={},
            )
        )
        cursor.observe(_record(15, partition=partition))
        cursor.close_partition(partition)

        assert self._state_manager.update_state_for_stream.call_args_list[-1].args[2] == {
            "slices": [{"end": 10, "start": 0, "most_recent_cursor_value": 15}],
            "state_type": "date-range",
        }

    def test_most_recent_cursor_value_on_sequential_state_when_close_then_cursor_value_is_most_recent_cursor_value(
        self,
    ) -> None:
        cursor = self._cursor_with_slice_boundary_fields(is_sequential_state=True)
        partition = _partition(
            StreamSlice(
                partition={_LOWER_SLICE_BOUNDARY_FIELD: 0, _UPPER_SLICE_BOUNDARY_FIELD: 10},
                cursor_slice={},
            )
        )
        cursor.observe(_record(7, partition=partition))
        cursor.close_partition(partition)

        assert self._state_manager.update_state_for_stream.call_args_list[-1].args[2] == {
            _A_CURSOR_FIELD_KEY: 7
        }

    def test_non_continuous_slices_on_sequential_state_when_close_then_cursor_value_is_most_recent_cursor_value_of_first_slice(
        self,
    ) -> None:
        cursor = self._cursor_with_slice_boundary_fields(is_sequential_state=True)
        first_partition = _partition(
            StreamSlice(
                partition={_LOWER_SLICE_BOUNDARY_FIELD: 0, _UPPER_SLICE_BOUNDARY_FIELD: 10},
                cursor_slice={},
            )
        )
        third_partition = _partition(
            StreamSlice(
                partition={_LOWER_SLICE_BOUNDARY_FIELD: 20, _UPPER_SLICE_BOUNDARY_FIELD: 30},
                cursor_slice={},
            )
        )  # second partition has failed
        cursor.observe(_record(7, partition=first_partition))
        cursor.close_partition(first_partition)

        cursor.close_partition(third_partition)

        assert self._state_manager.update_state_for_stream.call_args_list[-1].args[2] == {
            _A_CURSOR_FIELD_KEY: 7
        }

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(10, timezone.utc))
    def test_given_overflowing_slice_gap_when_generate_slices_then_cap_upper_bound_to_end_provider(
        self,
    ) -> None:
        a_very_big_slice_range = timedelta.max
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {_A_CURSOR_FIELD_KEY: 0},
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            None,
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
            slice_range=a_very_big_slice_range,
        )

        slices = list(cursor.stream_slices())

        assert slices == [
            StreamSlice(
                partition={},
                cursor_slice={
                    _SLICE_BOUNDARY_FIELDS[0]: 0,
                    _SLICE_BOUNDARY_FIELDS[1]: 10,
                },
            ),
        ]

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_initial_state_is_sequential_and_start_provided_when_generate_slices_then_state_emitted_is_initial_state(
        self,
    ) -> None:
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {_A_CURSOR_FIELD_KEY: 10},
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=True),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            datetime.fromtimestamp(0, timezone.utc),
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
        )

        # simulate the case where at least the first slice fails but others succeed
        cursor.close_partition(
            _partition(
                StreamSlice(
                    partition={_LOWER_SLICE_BOUNDARY_FIELD: 40, _UPPER_SLICE_BOUNDARY_FIELD: 50},
                    cursor_slice={},
                )
            )
        )

        self._state_manager.update_state_for_stream.assert_called_once_with(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                _A_CURSOR_FIELD_KEY: 10
            },  # State message is updated to the legacy format before being emitted
        )

    @freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(50, timezone.utc))
    def test_given_most_recent_cursor_value_in_input_state_when_emit_state_then_serialize_state_properly(
        self,
    ) -> None:
        cursor = ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {
                "state_type": ConcurrencyCompatibleStateType.date_range.value,
                "slices": [
                    {
                        EpochValueConcurrentStreamStateConverter.START_KEY: 0,
                        EpochValueConcurrentStreamStateConverter.END_KEY: 20,
                        EpochValueConcurrentStreamStateConverter.MOST_RECENT_RECORD_KEY: 15,
                    },
                ],
            },
            self._message_repository,
            self._state_manager,
            EpochValueConcurrentStreamStateConverter(is_sequential_state=False),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            datetime.fromtimestamp(0, timezone.utc),
            EpochValueConcurrentStreamStateConverter.get_end_provider(),
            _NO_LOOKBACK_WINDOW,
        )

        cursor.close_partition(
            _partition(
                StreamSlice(
                    partition={},
                    cursor_slice={
                        _LOWER_SLICE_BOUNDARY_FIELD: 20,
                        _UPPER_SLICE_BOUNDARY_FIELD: 50,
                    },
                ),
                _stream_name=_A_STREAM_NAME,
            )
        )

        expected_state = {
            "state_type": ConcurrencyCompatibleStateType.date_range.value,
            "slices": [
                {
                    EpochValueConcurrentStreamStateConverter.START_KEY: 0,
                    EpochValueConcurrentStreamStateConverter.END_KEY: 50,
                    EpochValueConcurrentStreamStateConverter.MOST_RECENT_RECORD_KEY: 15,
                },
            ],
        }
        self._state_manager.update_state_for_stream.assert_called_once_with(
            _A_STREAM_NAME, _A_STREAM_NAMESPACE, expected_state
        )


class ClampingIntegrationTest(TestCase):
    def setUp(self) -> None:
        self._message_repository = Mock(spec=MessageRepository)
        self._state_manager = Mock(spec=ConnectorStateManager)

    def _cursor(
        self,
        start: datetime,
        end_provider,
        slice_range: timedelta,
        granularity: Optional[timedelta],
        clamping_strategy: ClampingStrategy,
    ) -> ConcurrentCursor:
        return ConcurrentCursor(
            _A_STREAM_NAME,
            _A_STREAM_NAMESPACE,
            {},
            self._message_repository,
            self._state_manager,
            CustomFormatConcurrentStreamStateConverter(
                "%Y-%m-%dT%H:%M:%SZ", is_sequential_state=_NOT_SEQUENTIAL
            ),
            CursorField(_A_CURSOR_FIELD_KEY),
            _SLICE_BOUNDARY_FIELDS,
            start,
            end_provider,
            slice_range=slice_range,
            cursor_granularity=granularity,
            clamping_strategy=clamping_strategy,
        )

    @freezegun.freeze_time(time_to_freeze=datetime(2025, 1, 3, tzinfo=timezone.utc))
    def test_given_monthly_clamp_without_granularity_when_stream_slices_then_upper_boundaries_equals_next_lower_boundary(
        self,
    ) -> None:
        cursor = self._cursor(
            start=datetime(2023, 12, 31, tzinfo=timezone.utc),
            end_provider=ClampingEndProvider(
                MonthClampingStrategy(is_ceiling=False),
                CustomFormatConcurrentStreamStateConverter.get_end_provider(),
                granularity=timedelta(days=1),
            ),
            slice_range=timedelta(days=27),
            granularity=None,
            clamping_strategy=MonthClampingStrategy(),
        )
        stream_slices = list(cursor.stream_slices())
        assert stream_slices == [
            {"lower_boundary": "2024-01-01T00:00:00Z", "upper_boundary": "2024-02-01T00:00:00Z"},
            {"lower_boundary": "2024-02-01T00:00:00Z", "upper_boundary": "2024-03-01T00:00:00Z"},
            {"lower_boundary": "2024-03-01T00:00:00Z", "upper_boundary": "2024-04-01T00:00:00Z"},
            {"lower_boundary": "2024-04-01T00:00:00Z", "upper_boundary": "2024-05-01T00:00:00Z"},
            {"lower_boundary": "2024-05-01T00:00:00Z", "upper_boundary": "2024-06-01T00:00:00Z"},
            {"lower_boundary": "2024-06-01T00:00:00Z", "upper_boundary": "2024-07-01T00:00:00Z"},
            {"lower_boundary": "2024-07-01T00:00:00Z", "upper_boundary": "2024-08-01T00:00:00Z"},
            {"lower_boundary": "2024-08-01T00:00:00Z", "upper_boundary": "2024-09-01T00:00:00Z"},
            {"lower_boundary": "2024-09-01T00:00:00Z", "upper_boundary": "2024-10-01T00:00:00Z"},
            {"lower_boundary": "2024-10-01T00:00:00Z", "upper_boundary": "2024-11-01T00:00:00Z"},
            {"lower_boundary": "2024-11-01T00:00:00Z", "upper_boundary": "2024-12-01T00:00:00Z"},
            {"lower_boundary": "2024-12-01T00:00:00Z", "upper_boundary": "2025-01-01T00:00:00Z"},
        ]

    @freezegun.freeze_time(time_to_freeze=datetime(2025, 1, 3, tzinfo=timezone.utc))
    def test_given_monthly_clamp_and_granularity_when_stream_slices_then_consider_number_of_days_per_month(
        self,
    ) -> None:
        cursor = self._cursor(
            start=datetime(2023, 12, 31, tzinfo=timezone.utc),
            end_provider=ClampingEndProvider(
                MonthClampingStrategy(is_ceiling=False),
                CustomFormatConcurrentStreamStateConverter.get_end_provider(),
                granularity=timedelta(days=1),
            ),
            slice_range=timedelta(days=27),
            granularity=timedelta(days=1),
            clamping_strategy=MonthClampingStrategy(),
        )
        stream_slices = list(cursor.stream_slices())
        assert stream_slices == [
            {"lower_boundary": "2024-01-01T00:00:00Z", "upper_boundary": "2024-01-31T00:00:00Z"},
            {"lower_boundary": "2024-02-01T00:00:00Z", "upper_boundary": "2024-02-29T00:00:00Z"},
            {"lower_boundary": "2024-03-01T00:00:00Z", "upper_boundary": "2024-03-31T00:00:00Z"},
            {"lower_boundary": "2024-04-01T00:00:00Z", "upper_boundary": "2024-04-30T00:00:00Z"},
            {"lower_boundary": "2024-05-01T00:00:00Z", "upper_boundary": "2024-05-31T00:00:00Z"},
            {"lower_boundary": "2024-06-01T00:00:00Z", "upper_boundary": "2024-06-30T00:00:00Z"},
            {"lower_boundary": "2024-07-01T00:00:00Z", "upper_boundary": "2024-07-31T00:00:00Z"},
            {"lower_boundary": "2024-08-01T00:00:00Z", "upper_boundary": "2024-08-31T00:00:00Z"},
            {"lower_boundary": "2024-09-01T00:00:00Z", "upper_boundary": "2024-09-30T00:00:00Z"},
            {"lower_boundary": "2024-10-01T00:00:00Z", "upper_boundary": "2024-10-31T00:00:00Z"},
            {"lower_boundary": "2024-11-01T00:00:00Z", "upper_boundary": "2024-11-30T00:00:00Z"},
            {"lower_boundary": "2024-12-01T00:00:00Z", "upper_boundary": "2024-12-31T00:00:00Z"},
        ]

    @freezegun.freeze_time(time_to_freeze=datetime(2024, 1, 31, tzinfo=timezone.utc))
    def test_given_weekly_clamp_and_granularity_when_stream_slices_then_slice_per_week(
        self,
    ) -> None:
        cursor = self._cursor(
            start=datetime(
                2023, 12, 31, tzinfo=timezone.utc
            ),  # this is Sunday so we expect start to be 2 days after
            end_provider=ClampingEndProvider(
                WeekClampingStrategy(Weekday.TUESDAY, is_ceiling=False),
                CustomFormatConcurrentStreamStateConverter.get_end_provider(),
                granularity=timedelta(days=1),
            ),
            slice_range=timedelta(days=7),
            granularity=timedelta(days=1),
            clamping_strategy=WeekClampingStrategy(Weekday.TUESDAY),
        )
        stream_slices = list(cursor.stream_slices())
        assert stream_slices == [
            {"lower_boundary": "2024-01-02T00:00:00Z", "upper_boundary": "2024-01-08T00:00:00Z"},
            {"lower_boundary": "2024-01-09T00:00:00Z", "upper_boundary": "2024-01-15T00:00:00Z"},
            {"lower_boundary": "2024-01-16T00:00:00Z", "upper_boundary": "2024-01-22T00:00:00Z"},
            {"lower_boundary": "2024-01-23T00:00:00Z", "upper_boundary": "2024-01-29T00:00:00Z"},
        ]


@freezegun.freeze_time(time_to_freeze=datetime(2024, 4, 1, 0, 0, 0, 0, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "start_datetime,end_datetime,step,cursor_field,lookback_window,state,expected_slices",
    [
        pytest.param(
            "{{ config.start_time }}",
            "{{ config.end_time or now_utc() }}",
            "P10D",
            "updated_at",
            "P5D",
            {},
            [
                {
                    "start": "2024-01-01T00:00:00.000Z",
                    "end": "2024-01-10T23:59:59.000Z",
                },
                {
                    "start": "2024-01-11T00:00:00.000Z",
                    "end": "2024-01-20T23:59:59.000Z",
                },
                {
                    "start": "2024-01-21T00:00:00.000Z",
                    "end": "2024-01-30T23:59:59.000Z",
                },
                {
                    "start": "2024-01-31T00:00:00.000Z",
                    "end": "2024-02-09T23:59:59.000Z",
                },
                {
                    "start": "2024-02-10T00:00:00.000Z",
                    "end": "2024-02-19T23:59:59.000Z",
                },
                {
                    "start": "2024-02-20T00:00:00.000Z",
                    "end": "2024-03-01T00:00:00.000Z",
                },
            ],
            id="test_datetime_based_cursor_all_fields",
        ),
        pytest.param(
            "{{ config.start_time }}",
            "{{ config.end_time or '2024-01-01T00:00:00.000000+0000' }}",
            "P10D",
            "updated_at",
            "P5D",
            {
                "slices": [
                    {
                        "start": "2024-01-01T00:00:00.000000+0000",
                        "end": "2024-02-10T00:00:00.000000+0000",
                    }
                ],
                "state_type": "date-range",
            },
            [
                {
                    "start": "2024-02-05T00:00:00.000Z",
                    "end": "2024-02-14T23:59:59.000Z",
                },
                {
                    "start": "2024-02-15T00:00:00.000Z",
                    "end": "2024-02-24T23:59:59.000Z",
                },
                {
                    "start": "2024-02-25T00:00:00.000Z",
                    "end": "2024-03-01T00:00:00.000Z",
                },
            ],
            id="test_datetime_based_cursor_with_state",
        ),
        pytest.param(
            "{{ config.start_time }}",
            "{{ config.missing or now_utc().strftime('%Y-%m-%dT%H:%M:%S.%fZ') }}",
            "P20D",
            "updated_at",
            "P1D",
            {
                "slices": [
                    {
                        "start": "2024-01-01T00:00:00.000000+0000",
                        "end": "2024-01-21T00:00:00.000000+0000",
                    }
                ],
                "state_type": "date-range",
            },
            [
                {
                    "start": "2024-01-20T00:00:00.000Z",
                    "end": "2024-02-08T23:59:59.000Z",
                },
                {
                    "start": "2024-02-09T00:00:00.000Z",
                    "end": "2024-02-28T23:59:59.000Z",
                },
                {
                    "start": "2024-02-29T00:00:00.000Z",
                    "end": "2024-03-19T23:59:59.000Z",
                },
                {
                    "start": "2024-03-20T00:00:00.000Z",
                    "end": "2024-04-01T00:00:00.000Z",
                },
            ],
            id="test_datetime_based_cursor_with_state_and_end_date",
        ),
        pytest.param(
            "{{ config.start_time }}",
            "{{ config.end_time }}",
            "P1M",
            "updated_at",
            "P5D",
            {},
            [
                {
                    "start": "2024-01-01T00:00:00.000Z",
                    "end": "2024-01-31T23:59:59.000Z",
                },
                {
                    "start": "2024-02-01T00:00:00.000Z",
                    "end": "2024-03-01T00:00:00.000Z",
                },
            ],
            id="test_datetime_based_cursor_using_large_step_duration",
        ),
    ],
)
def test_generate_slices_concurrent_cursor_from_datetime_based_cursor(
    start_datetime,
    end_datetime,
    step,
    cursor_field,
    lookback_window,
    state,
    expected_slices,
):
    message_repository = Mock(spec=MessageRepository)
    state_manager = Mock(spec=ConnectorStateManager)

    config = {
        "start_time": "2024-01-01T00:00:00.000000+0000",
        "end_time": "2024-03-01T00:00:00.000000+0000",
    }

    datetime_based_cursor = DatetimeBasedCursor(
        start_datetime=MinMaxDatetime(datetime=start_datetime, parameters={}),
        end_datetime=MinMaxDatetime(datetime=end_datetime, parameters={}),
        step=step,
        cursor_field=cursor_field,
        partition_field_start="start",
        partition_field_end="end",
        datetime_format="%Y-%m-%dT%H:%M:%S.%f%z",
        cursor_granularity="PT1S",
        lookback_window=lookback_window,
        is_compare_strictly=True,
        config=config,
        parameters={},
    )

    # I don't love that we're back to this inching close to interpolation at parse time instead of runtime
    # We also might need to add a wrapped class that exposes these fields publicly or live with ugly private access
    interpolated_state_date = datetime_based_cursor._start_datetime
    start_date = interpolated_state_date.get_datetime(config=config)

    interpolated_end_date = datetime_based_cursor._end_datetime
    interpolated_end_date_provider = partial(interpolated_end_date.get_datetime, config)

    interpolated_cursor_field = datetime_based_cursor.cursor_field
    cursor_field = CursorField(cursor_field_key=interpolated_cursor_field.eval(config=config))

    lower_slice_boundary = datetime_based_cursor._partition_field_start.eval(config=config)
    upper_slice_boundary = datetime_based_cursor._partition_field_end.eval(config=config)
    slice_boundary_fields = (lower_slice_boundary, upper_slice_boundary)

    # DatetimeBasedCursor returns an isodate.Duration if step uses month or year precision. This still works in our
    # code, but mypy may complain when we actually implement this in the concurrent low-code source. To fix this, we
    # may need to convert a Duration to timedelta by multiplying month by 30 (but could lose precision).
    step_length = datetime_based_cursor._step

    lookback_window = (
        parse_duration(datetime_based_cursor.lookback_window)
        if datetime_based_cursor.lookback_window
        else None
    )

    cursor_granularity = (
        parse_duration(datetime_based_cursor.cursor_granularity)
        if datetime_based_cursor.cursor_granularity
        else None
    )

    cursor = ConcurrentCursor(
        stream_name=_A_STREAM_NAME,
        stream_namespace=_A_STREAM_NAMESPACE,
        stream_state=state,
        message_repository=message_repository,
        connector_state_manager=state_manager,
        connector_state_converter=IsoMillisConcurrentStreamStateConverter(is_sequential_state=True),
        cursor_field=cursor_field,
        slice_boundary_fields=slice_boundary_fields,
        start=start_date,
        end_provider=interpolated_end_date_provider,
        lookback_window=lookback_window,
        slice_range=step_length,
        cursor_granularity=cursor_granularity,
    )

    actual_slices = list(cursor.stream_slices())
    assert actual_slices == expected_slices


@freezegun.freeze_time(time_to_freeze=datetime(2024, 9, 1, 0, 0, 0, 0, tzinfo=timezone.utc))
def test_observe_concurrent_cursor_from_datetime_based_cursor():
    message_repository = Mock(spec=MessageRepository)
    state_manager = Mock(spec=ConnectorStateManager)

    config = {"start_time": "2024-08-01T00:00:00.000000+0000", "dynamic_cursor_key": "updated_at"}

    datetime_based_cursor = DatetimeBasedCursor(
        start_datetime=MinMaxDatetime(datetime="{{ config.start_time }}", parameters={}),
        cursor_field="{{ config.dynamic_cursor_key }}",
        datetime_format="%Y-%m-%dT%H:%M:%S.%f%z",
        config=config,
        parameters={},
    )

    interpolated_state_date = datetime_based_cursor._start_datetime
    start_date = interpolated_state_date.get_datetime(config=config)

    interpolated_cursor_field = datetime_based_cursor.cursor_field
    cursor_field = CursorField(cursor_field_key=interpolated_cursor_field.eval(config=config))

    step_length = datetime_based_cursor._step

    concurrent_cursor = ConcurrentCursor(
        stream_name="gods",
        stream_namespace=_A_STREAM_NAMESPACE,
        stream_state={},
        message_repository=message_repository,
        connector_state_manager=state_manager,
        connector_state_converter=IsoMillisConcurrentStreamStateConverter(is_sequential_state=True),
        cursor_field=cursor_field,
        slice_boundary_fields=None,
        start=start_date,
        end_provider=IsoMillisConcurrentStreamStateConverter.get_end_provider(),
        slice_range=step_length,
    )

    partition = _partition(
        StreamSlice(
            partition={
                _LOWER_SLICE_BOUNDARY_FIELD: "2024-08-01T00:00:00.000000+0000",
                _UPPER_SLICE_BOUNDARY_FIELD: "2024-09-01T00:00:00.000000+0000",
            },
            cursor_slice={},
        ),
        _stream_name="gods",
    )

    record_1 = Record(
        associated_slice=partition.to_slice(),
        data={
            "id": "999",
            "updated_at": "2024-08-23T00:00:00.000000+0000",
            "name": "kratos",
            "mythology": "greek",
        },
        stream_name="gods",
    )
    record_2 = Record(
        associated_slice=partition.to_slice(),
        data={
            "id": "1000",
            "updated_at": "2024-08-22T00:00:00.000000+0000",
            "name": "odin",
            "mythology": "norse",
        },
        stream_name="gods",
    )
    record_3 = Record(
        associated_slice=partition.to_slice(),
        data={
            "id": "500",
            "updated_at": "2024-08-24T00:00:00.000000+0000",
            "name": "freya",
            "mythology": "norse",
        },
        stream_name="gods",
    )

    concurrent_cursor.observe(record_1)
    actual_most_recent_record = concurrent_cursor._most_recent_cursor_value_per_partition[
        partition.to_slice()
    ]
    assert actual_most_recent_record == concurrent_cursor._extract_cursor_value(record_1)

    concurrent_cursor.observe(record_2)
    actual_most_recent_record = concurrent_cursor._most_recent_cursor_value_per_partition[
        partition.to_slice()
    ]
    assert actual_most_recent_record == concurrent_cursor._extract_cursor_value(record_1)

    concurrent_cursor.observe(record_3)
    actual_most_recent_record = concurrent_cursor._most_recent_cursor_value_per_partition[
        partition.to_slice()
    ]
    assert actual_most_recent_record == concurrent_cursor._extract_cursor_value(record_3)


@freezegun.freeze_time(time_to_freeze=datetime(2024, 9, 1, 0, 0, 0, 0, tzinfo=timezone.utc))
def test_close_partition_concurrent_cursor_from_datetime_based_cursor():
    message_repository = Mock(spec=MessageRepository)
    state_manager = Mock(spec=ConnectorStateManager)

    config = {"start_time": "2024-08-01T00:00:00.000000+0000", "dynamic_cursor_key": "updated_at"}

    datetime_based_cursor = DatetimeBasedCursor(
        start_datetime=MinMaxDatetime(datetime="{{ config.start_time }}", parameters={}),
        cursor_field="{{ config.dynamic_cursor_key }}",
        datetime_format="%Y-%m-%dT%H:%M:%S.%f%z",
        config=config,
        parameters={},
    )

    interpolated_state_date = datetime_based_cursor._start_datetime
    start_date = interpolated_state_date.get_datetime(config=config)

    interpolated_cursor_field = datetime_based_cursor.cursor_field
    cursor_field = CursorField(cursor_field_key=interpolated_cursor_field.eval(config=config))

    step_length = datetime_based_cursor._step

    concurrent_cursor = ConcurrentCursor(
        stream_name="gods",
        stream_namespace=_A_STREAM_NAMESPACE,
        stream_state={},
        message_repository=message_repository,
        connector_state_manager=state_manager,
        connector_state_converter=IsoMillisConcurrentStreamStateConverter(
            is_sequential_state=False
        ),
        cursor_field=cursor_field,
        slice_boundary_fields=None,
        start=start_date,
        end_provider=IsoMillisConcurrentStreamStateConverter.get_end_provider(),
        slice_range=step_length,
    )

    partition = _partition(
        StreamSlice(
            partition={
                _LOWER_SLICE_BOUNDARY_FIELD: "2024-08-01T00:00:00.000000+0000",
                _UPPER_SLICE_BOUNDARY_FIELD: "2024-09-01T00:00:00.000000+0000",
            },
            cursor_slice={},
        ),
        _stream_name="gods",
    )

    record_1 = Record(
        associated_slice=partition.to_slice(),
        data={
            "id": "999",
            "updated_at": "2024-08-23T00:00:00.000000+0000",
            "name": "kratos",
            "mythology": "greek",
        },
        stream_name="gods",
    )
    concurrent_cursor.observe(record_1)

    concurrent_cursor.close_partition(partition)

    message_repository.emit_message.assert_called_once_with(
        state_manager.create_state_message.return_value
    )
    state_manager.update_state_for_stream.assert_called_once_with(
        "gods",
        _A_STREAM_NAMESPACE,
        {
            "slices": [
                {
                    "end": "2024-08-23T00:00:00.000Z",
                    "start": "2024-08-01T00:00:00.000Z",
                    "most_recent_cursor_value": "2024-08-23T00:00:00.000Z",
                }
            ],
            "state_type": "date-range",
        },
    )


@freezegun.freeze_time(time_to_freeze=datetime(2024, 9, 1, 0, 0, 0, 0, tzinfo=timezone.utc))
def test_close_partition_with_slice_range_concurrent_cursor_from_datetime_based_cursor():
    message_repository = Mock(spec=MessageRepository)
    state_manager = Mock(spec=ConnectorStateManager)

    config = {"start_time": "2024-07-01T00:00:00.000000+0000", "dynamic_cursor_key": "updated_at"}

    datetime_based_cursor = DatetimeBasedCursor(
        start_datetime=MinMaxDatetime(datetime="{{ config.start_time }}", parameters={}),
        cursor_field="{{ config.dynamic_cursor_key }}",
        datetime_format="%Y-%m-%dT%H:%M:%S.%f%z",
        step="P15D",
        cursor_granularity="P1D",
        config=config,
        parameters={},
    )

    interpolated_state_date = datetime_based_cursor._start_datetime
    start_date = interpolated_state_date.get_datetime(config=config)

    interpolated_cursor_field = datetime_based_cursor.cursor_field
    cursor_field = CursorField(cursor_field_key=interpolated_cursor_field.eval(config=config))

    lower_slice_boundary = datetime_based_cursor._partition_field_start.eval(config=config)
    upper_slice_boundary = datetime_based_cursor._partition_field_end.eval(config=config)
    slice_boundary_fields = (lower_slice_boundary, upper_slice_boundary)

    step_length = datetime_based_cursor._step

    concurrent_cursor = ConcurrentCursor(
        stream_name="gods",
        stream_namespace=_A_STREAM_NAMESPACE,
        stream_state={},
        message_repository=message_repository,
        connector_state_manager=state_manager,
        connector_state_converter=IsoMillisConcurrentStreamStateConverter(
            is_sequential_state=False, cursor_granularity=None
        ),
        cursor_field=cursor_field,
        slice_boundary_fields=slice_boundary_fields,
        start=start_date,
        slice_range=step_length,
        cursor_granularity=None,
        end_provider=IsoMillisConcurrentStreamStateConverter.get_end_provider(),
    )

    partition_0 = _partition(
        StreamSlice(
            partition={
                "start_time": "2024-07-01T00:00:00.000000+0000",
                "end_time": "2024-07-16T00:00:00.000000+0000",
            },
            cursor_slice={},
        ),
        _stream_name="gods",
    )
    partition_3 = _partition(
        StreamSlice(
            partition={
                "start_time": "2024-08-15T00:00:00.000000+0000",
                "end_time": "2024-08-30T00:00:00.000000+0000",
            },
            cursor_slice={},
        ),
        _stream_name="gods",
    )
    record_1 = Record(
        associated_slice=partition_0.to_slice(),
        data={
            "id": "1000",
            "updated_at": "2024-07-05T00:00:00.000000+0000",
            "name": "loki",
            "mythology": "norse",
        },
        stream_name="gods",
    )
    record_2 = Record(
        associated_slice=partition_3.to_slice(),
        data={
            "id": "999",
            "updated_at": "2024-08-20T00:00:00.000000+0000",
            "name": "kratos",
            "mythology": "greek",
        },
        stream_name="gods",
    )

    concurrent_cursor.observe(record_1)
    concurrent_cursor.close_partition(partition_0)
    concurrent_cursor.observe(record_2)
    concurrent_cursor.close_partition(partition_3)

    message_repository.emit_message.assert_called_with(
        state_manager.create_state_message.return_value
    )
    assert message_repository.emit_message.call_count == 2
    state_manager.update_state_for_stream.assert_called_with(
        "gods",
        _A_STREAM_NAMESPACE,
        {
            "slices": [
                {
                    "start": "2024-07-01T00:00:00.000Z",
                    "end": "2024-07-16T00:00:00.000Z",
                    "most_recent_cursor_value": "2024-07-05T00:00:00.000Z",
                },
                {
                    "start": "2024-08-15T00:00:00.000Z",
                    "end": "2024-08-30T00:00:00.000Z",
                    "most_recent_cursor_value": "2024-08-20T00:00:00.000Z",
                },
            ],
            "state_type": "date-range",
        },
    )
    assert state_manager.update_state_for_stream.call_count == 2


@freezegun.freeze_time(time_to_freeze=datetime(2024, 9, 1, 0, 0, 0, 0, tzinfo=timezone.utc))
def test_close_partition_with_slice_range_granularity_concurrent_cursor_from_datetime_based_cursor():
    message_repository = Mock(spec=MessageRepository)
    state_manager = Mock(spec=ConnectorStateManager)

    config = {"start_time": "2024-07-01T00:00:00.000000+0000", "dynamic_cursor_key": "updated_at"}

    datetime_based_cursor = DatetimeBasedCursor(
        start_datetime=MinMaxDatetime(datetime="{{ config.start_time }}", parameters={}),
        cursor_field="{{ config.dynamic_cursor_key }}",
        datetime_format="%Y-%m-%dT%H:%M:%S.%f%z",
        step="P15D",
        cursor_granularity="P1D",
        config=config,
        parameters={},
    )

    interpolated_state_date = datetime_based_cursor._start_datetime
    start_date = interpolated_state_date.get_datetime(config=config)

    interpolated_cursor_field = datetime_based_cursor.cursor_field
    cursor_field = CursorField(cursor_field_key=interpolated_cursor_field.eval(config=config))

    lower_slice_boundary = datetime_based_cursor._partition_field_start.eval(config=config)
    upper_slice_boundary = datetime_based_cursor._partition_field_end.eval(config=config)
    slice_boundary_fields = (lower_slice_boundary, upper_slice_boundary)

    step_length = datetime_based_cursor._step

    cursor_granularity = (
        parse_duration(datetime_based_cursor.cursor_granularity)
        if datetime_based_cursor.cursor_granularity
        else None
    )

    concurrent_cursor = ConcurrentCursor(
        stream_name="gods",
        stream_namespace=_A_STREAM_NAMESPACE,
        stream_state={},
        message_repository=message_repository,
        connector_state_manager=state_manager,
        connector_state_converter=IsoMillisConcurrentStreamStateConverter(
            is_sequential_state=False, cursor_granularity=cursor_granularity
        ),
        cursor_field=cursor_field,
        slice_boundary_fields=slice_boundary_fields,
        start=start_date,
        slice_range=step_length,
        cursor_granularity=cursor_granularity,
        end_provider=IsoMillisConcurrentStreamStateConverter.get_end_provider(),
    )

    partition_0 = _partition(
        StreamSlice(
            partition={
                "start_time": "2024-07-01T00:00:00.000000+0000",
                "end_time": "2024-07-15T00:00:00.000000+0000",
            },
            cursor_slice={},
        ),
        _stream_name="gods",
    )
    partition_1 = _partition(
        StreamSlice(
            partition={
                "start_time": "2024-07-16T00:00:00.000000+0000",
                "end_time": "2024-07-31T00:00:00.000000+0000",
            },
            cursor_slice={},
        ),
        _stream_name="gods",
    )
    partition_3 = _partition(
        StreamSlice(
            partition={
                "start_time": "2024-08-15T00:00:00.000000+0000",
                "end_time": "2024-08-29T00:00:00.000000+0000",
            },
            cursor_slice={},
        ),
        _stream_name="gods",
    )
    record_1 = Record(
        associated_slice=partition_0.to_slice(),
        data={
            "id": "1000",
            "updated_at": "2024-07-05T00:00:00.000000+0000",
            "name": "loki",
            "mythology": "norse",
        },
        stream_name="gods",
    )
    record_2 = Record(
        associated_slice=partition_1.to_slice(),
        data={
            "id": "2000",
            "updated_at": "2024-07-25T00:00:00.000000+0000",
            "name": "freya",
            "mythology": "norse",
        },
        stream_name="gods",
    )
    record_3 = Record(
        associated_slice=partition_3.to_slice(),
        data={
            "id": "999",
            "updated_at": "2024-08-20T00:00:00.000000+0000",
            "name": "kratos",
            "mythology": "greek",
        },
        stream_name="gods",
    )

    concurrent_cursor.observe(record_1)
    concurrent_cursor.close_partition(partition_0)
    concurrent_cursor.observe(record_2)
    concurrent_cursor.close_partition(partition_1)
    concurrent_cursor.observe(record_3)
    concurrent_cursor.close_partition(partition_3)

    message_repository.emit_message.assert_called_with(
        state_manager.create_state_message.return_value
    )
    assert message_repository.emit_message.call_count == 3
    state_manager.update_state_for_stream.assert_called_with(
        "gods",
        _A_STREAM_NAMESPACE,
        {
            "slices": [
                {
                    "start": "2024-07-01T00:00:00.000Z",
                    "end": "2024-07-31T00:00:00.000Z",
                    "most_recent_cursor_value": "2024-07-25T00:00:00.000Z",
                },
                {
                    "start": "2024-08-15T00:00:00.000Z",
                    "end": "2024-08-29T00:00:00.000Z",
                    "most_recent_cursor_value": "2024-08-20T00:00:00.000Z",
                },
            ],
            "state_type": "date-range",
        },
    )
    assert state_manager.update_state_for_stream.call_count == 3


_SHOULD_BE_SYNCED_START = 10


@pytest.mark.parametrize(
    "record, should_be_synced",
    [
        [
            Record(
                data={_A_CURSOR_FIELD_KEY: _SHOULD_BE_SYNCED_START},
                stream_name="test_stream",
            ),
            True,
        ],
        [
            Record(
                data={_A_CURSOR_FIELD_KEY: _SHOULD_BE_SYNCED_START - 1},
                stream_name="test_stream",
            ),
            False,
        ],
        [
            Record(
                data={_A_CURSOR_FIELD_KEY: _SHOULD_BE_SYNCED_START + 1},
                stream_name="test_stream",
            ),
            True,
        ],
        [
            Record(
                data={"not_a_cursor_field": "some_data"},
                stream_name="test_stream",
            ),
            True,
        ],
    ],
    ids=[
        "with_cursor_field_inside_range",
        "with_cursor_field_lower_than_start",
        "with_cursor_field_higher_than_end",
        "no_cursor",
    ],
)
@freezegun.freeze_time(time_to_freeze=datetime.fromtimestamp(20, timezone.utc))
def test_should_be_synced_non_partitioned_state_no_state(record: Record, should_be_synced: bool):
    cursor = ConcurrentCursor(
        _A_STREAM_NAME,
        _A_STREAM_NAMESPACE,
        {},
        Mock(spec=MessageRepository),
        Mock(spec=ConnectorStateManager),
        EpochValueConcurrentStreamStateConverter(True),
        CursorField(_A_CURSOR_FIELD_KEY),
        _SLICE_BOUNDARY_FIELDS,
        datetime.fromtimestamp(_SHOULD_BE_SYNCED_START, timezone.utc),
        EpochValueConcurrentStreamStateConverter.get_end_provider(),
        _NO_LOOKBACK_WINDOW,
    )
    assert cursor.should_be_synced(record) == should_be_synced


def test_given_state_when_should_be_synced_then_use_cursor_value_to_filter():
    state_value = _SHOULD_BE_SYNCED_START + 5
    cursor = ConcurrentCursor(
        _A_STREAM_NAME,
        _A_STREAM_NAMESPACE,
        {_A_CURSOR_FIELD_KEY: state_value},
        Mock(spec=MessageRepository),
        Mock(spec=ConnectorStateManager),
        EpochValueConcurrentStreamStateConverter(True),
        CursorField(_A_CURSOR_FIELD_KEY),
        _SLICE_BOUNDARY_FIELDS,
        datetime.fromtimestamp(_SHOULD_BE_SYNCED_START, timezone.utc),
        EpochValueConcurrentStreamStateConverter.get_end_provider(),
        _NO_LOOKBACK_WINDOW,
    )

    assert (
        cursor.should_be_synced(
            Record(data={_A_CURSOR_FIELD_KEY: state_value - 1}, stream_name="test_stream")
        )
        == False
    )
    assert (
        cursor.should_be_synced(
            Record(data={_A_CURSOR_FIELD_KEY: state_value}, stream_name="test_stream")
        )
        == True
    )


def test_given_partitioned_state_without_slices_nor_start_when_should_be_synced_then_use_zero_value_to_filter():
    cursor = ConcurrentCursor(
        _A_STREAM_NAME,
        _A_STREAM_NAMESPACE,
        {
            "slices": [],
            "state_type": "date-range",
        },
        Mock(spec=MessageRepository),
        Mock(spec=ConnectorStateManager),
        EpochValueConcurrentStreamStateConverter(True),
        CursorField(_A_CURSOR_FIELD_KEY),
        _SLICE_BOUNDARY_FIELDS,
        None,
        EpochValueConcurrentStreamStateConverter.get_end_provider(),
        _NO_LOOKBACK_WINDOW,
    )

    assert (
        cursor.should_be_synced(Record(data={_A_CURSOR_FIELD_KEY: -1}, stream_name="test_stream"))
        == False
    )
    assert (
        cursor.should_be_synced(Record(data={_A_CURSOR_FIELD_KEY: 0}, stream_name="test_stream"))
        == True
    )


def test_given_partitioned_state_without_slices_but_start_when_should_be_synced_then_use_start_value_to_filter():
    cursor = ConcurrentCursor(
        _A_STREAM_NAME,
        _A_STREAM_NAMESPACE,
        {
            "slices": [],
            "state_type": "date-range",
        },
        Mock(spec=MessageRepository),
        Mock(spec=ConnectorStateManager),
        EpochValueConcurrentStreamStateConverter(True),
        CursorField(_A_CURSOR_FIELD_KEY),
        _SLICE_BOUNDARY_FIELDS,
        datetime.fromtimestamp(_SHOULD_BE_SYNCED_START, timezone.utc),
        EpochValueConcurrentStreamStateConverter.get_end_provider(),
        _NO_LOOKBACK_WINDOW,
    )

    assert (
        cursor.should_be_synced(
            Record(
                data={_A_CURSOR_FIELD_KEY: _SHOULD_BE_SYNCED_START - 1}, stream_name="test_stream"
            )
        )
        == False
    )
    assert (
        cursor.should_be_synced(
            Record(data={_A_CURSOR_FIELD_KEY: _SHOULD_BE_SYNCED_START}, stream_name="test_stream")
        )
        == True
    )


def test_given_partitioned_state_with_one_slice_and_most_recent_cursor_value_when_should_be_synced_then_use_most_recent_cursor_value_of_slice_to_filter():
    most_recent_cursor_value = 5
    cursor = ConcurrentCursor(
        _A_STREAM_NAME,
        _A_STREAM_NAMESPACE,
        {
            "slices": [
                {"end": 10, "most_recent_cursor_value": most_recent_cursor_value, "start": 0},
            ],
            "state_type": "date-range",
        },
        Mock(spec=MessageRepository),
        Mock(spec=ConnectorStateManager),
        EpochValueConcurrentStreamStateConverter(True),
        CursorField(_A_CURSOR_FIELD_KEY),
        _SLICE_BOUNDARY_FIELDS,
        datetime.fromtimestamp(_SHOULD_BE_SYNCED_START, timezone.utc),
        EpochValueConcurrentStreamStateConverter.get_end_provider(),
        _NO_LOOKBACK_WINDOW,
    )

    assert (
        cursor.should_be_synced(
            Record(
                data={_A_CURSOR_FIELD_KEY: most_recent_cursor_value - 1}, stream_name="test_stream"
            )
        )
        == False
    )
    assert (
        cursor.should_be_synced(
            Record(data={_A_CURSOR_FIELD_KEY: most_recent_cursor_value}, stream_name="test_stream")
        )
        == True
    )


def test_given_partitioned_state_with_one_slice_without_most_recent_cursor_value_when_should_be_synced_then_use_upper_boundary_of_slice_to_filter():
    slice_end = 5
    cursor = ConcurrentCursor(
        _A_STREAM_NAME,
        _A_STREAM_NAMESPACE,
        {
            "slices": [
                {"end": slice_end, "start": 0},
            ],
            "state_type": "date-range",
        },
        Mock(spec=MessageRepository),
        Mock(spec=ConnectorStateManager),
        EpochValueConcurrentStreamStateConverter(True),
        CursorField(_A_CURSOR_FIELD_KEY),
        _SLICE_BOUNDARY_FIELDS,
        datetime.fromtimestamp(_SHOULD_BE_SYNCED_START, timezone.utc),
        EpochValueConcurrentStreamStateConverter.get_end_provider(),
        _NO_LOOKBACK_WINDOW,
    )

    assert (
        cursor.should_be_synced(
            Record(data={_A_CURSOR_FIELD_KEY: slice_end - 1}, stream_name="test_stream")
        )
        == False
    )
    assert (
        cursor.should_be_synced(
            Record(data={_A_CURSOR_FIELD_KEY: slice_end}, stream_name="test_stream")
        )
        == True
    )


def test_given_partitioned_state_with_multiple_slices_when_should_be_synced_then_use_upper_boundary_of_first_slice_to_filter():
    first_slice_end = 5
    second_slice_start = first_slice_end + 10
    cursor = ConcurrentCursor(
        _A_STREAM_NAME,
        _A_STREAM_NAMESPACE,
        {
            "slices": [
                {"end": first_slice_end, "start": 0},
                {"end": first_slice_end + 100, "start": second_slice_start},
            ],
            "state_type": "date-range",
        },
        Mock(spec=MessageRepository),
        Mock(spec=ConnectorStateManager),
        EpochValueConcurrentStreamStateConverter(True),
        CursorField(_A_CURSOR_FIELD_KEY),
        _SLICE_BOUNDARY_FIELDS,
        datetime.fromtimestamp(_SHOULD_BE_SYNCED_START, timezone.utc),
        EpochValueConcurrentStreamStateConverter.get_end_provider(),
        _NO_LOOKBACK_WINDOW,
    )

    assert (
        cursor.should_be_synced(
            Record(data={_A_CURSOR_FIELD_KEY: first_slice_end - 1}, stream_name="test_stream")
        )
        == False
    )
    assert (
        cursor.should_be_synced(
            Record(data={_A_CURSOR_FIELD_KEY: first_slice_end}, stream_name="test_stream")
        )
        == True
    )
    # even if this is within a boundary that has been synced, we don't take any chance and we sync it
    # anyway in most cases, it shouldn't be pulled because we query for specific slice boundaries to the API
    assert (
        cursor.should_be_synced(
            Record(data={_A_CURSOR_FIELD_KEY: second_slice_start}, stream_name="test_stream")
        )
        == True
    )
