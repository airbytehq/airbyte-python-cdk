#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from datetime import datetime, timedelta, timezone
from typing import List, Mapping, Optional
from unittest.mock import Mock

import pytest

from airbyte_cdk.sources.declarative.datetime.min_max_datetime import MinMaxDatetime
from airbyte_cdk.sources.declarative.extractors.record_filter import (
    ClientSideIncrementalRecordFilterDecorator,
    RecordFilter,
)
from airbyte_cdk.sources.declarative.incremental import (
    ConcurrentCursorFactory,
    ConcurrentPerPartitionCursor,
)
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.models import (
    CustomRetriever,
    DeclarativeStream,
    ParentStreamConfig,
)
from airbyte_cdk.sources.declarative.partition_routers import SubstreamPartitionRouter
from airbyte_cdk.sources.declarative.types import StreamSlice
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor, CursorField
from airbyte_cdk.sources.streams.concurrent.state_converters.datetime_stream_state_converter import (
    CustomFormatConcurrentStreamStateConverter,
)
from airbyte_cdk.sources.types import Record
from airbyte_cdk.utils.datetime_helpers import ab_datetime_now, ab_datetime_parse

DATE_FORMAT = "%Y-%m-%d"
RECORDS_TO_FILTER_DATE_FORMAT = [
    {"id": 1, "created_at": "2020-01-03"},
    {"id": 2, "created_at": "2021-01-03"},
    {"id": 3, "created_at": "2021-01-04"},
    {"id": 4, "created_at": "2021-02-01"},
    {"id": 5, "created_at": "2021-01-02"},
]

DATE_TIME_WITH_TZ_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
RECORDS_TO_FILTER_DATE_TIME_WITH_TZ_FORMAT = [
    {"id": 1, "created_at": "2020-01-03T00:00:00+00:00"},
    {"id": 2, "created_at": "2021-01-03T00:00:00+00:00"},
    {"id": 3, "created_at": "2021-01-04T00:00:00+00:00"},
    {"id": 4, "created_at": "2021-02-01T00:00:00+00:00"},
]

DATE_TIME_WITHOUT_TZ_FORMAT = "%Y-%m-%dT%H:%M:%S"
RECORDS_TO_FILTER_DATE_TIME_WITHOUT_TZ_FORMAT = [
    {"id": 1, "created_at": "2020-01-03T00:00:00"},
    {"id": 2, "created_at": "2021-01-03T00:00:00"},
    {"id": 3, "created_at": "2021-01-04T00:00:00"},
    {"id": 4, "created_at": "2021-02-01T00:00:00"},
]


@pytest.mark.parametrize(
    "filter_template, records, expected_records",
    [
        (
            "{{ record['created_at'] >= stream_interval.extra_fields['created_at'] }}",
            [
                {"id": 1, "created_at": "06-06-21"},
                {"id": 2, "created_at": "06-07-21"},
                {"id": 3, "created_at": "06-08-21"},
            ],
            [{"id": 2, "created_at": "06-07-21"}, {"id": 3, "created_at": "06-08-21"}],
        ),
        (
            "{{ record['last_seen'] >= stream_slice['last_seen'] }}",
            [
                {"id": 1, "last_seen": "06-06-21"},
                {"id": 2, "last_seen": "06-07-21"},
                {"id": 3, "last_seen": "06-10-21"},
            ],
            [{"id": 3, "last_seen": "06-10-21"}],
        ),
        (
            "{{ record['id'] >= next_page_token['last_seen_id'] }}",
            [{"id": 11}, {"id": 12}, {"id": 13}, {"id": 14}, {"id": 15}],
            [{"id": 14}, {"id": 15}],
        ),
        (
            "{{ record['id'] >= next_page_token['path_to_nowhere'] }}",
            [{"id": 11}, {"id": 12}, {"id": 13}, {"id": 14}, {"id": 15}],
            [],
        ),
        (
            "{{ record['created_at'] > parameters['created_at'] }}",
            [
                {"id": 1, "created_at": "06-06-21"},
                {"id": 2, "created_at": "06-07-21"},
                {"id": 3, "created_at": "06-08-21"},
            ],
            [{"id": 3, "created_at": "06-08-21"}],
        ),
        (
            "{{ record['created_at'] > stream_slice.extra_fields['created_at'] }}",
            [
                {"id": 1, "created_at": "06-06-21"},
                {"id": 2, "created_at": "06-07-21"},
                {"id": 3, "created_at": "06-08-21"},
            ],
            [{"id": 3, "created_at": "06-08-21"}],
        ),
    ],
    ids=[
        "test_using_state_filter",
        "test_with_slice_filter",
        "test_with_next_page_token_filter",
        "test_missing_filter_fields_return_no_results",
        "test_using_parameters_filter",
        "test_using_extra_fields_filter",
    ],
)
def test_record_filter(
    filter_template: str, records: List[Mapping], expected_records: List[Mapping]
):
    config = {"response_override": "stop_if_you_see_me"}
    parameters = {"created_at": "06-07-21"}
    stream_state = {"created_at": "06-06-21"}
    stream_slice = StreamSlice(
        partition={},
        cursor_slice={"last_seen": "06-10-21"},
        extra_fields={"created_at": "06-07-21"},
    )
    next_page_token = {"last_seen_id": 14}
    record_filter = RecordFilter(config=config, condition=filter_template, parameters=parameters)

    actual_records = list(
        record_filter.filter_records(
            records,
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )
    )
    assert actual_records == expected_records


@pytest.mark.parametrize(
    "datetime_format, stream_state, record_filter_expression, end_datetime,  records_to_filter, expected_record_ids",
    [
        (DATE_FORMAT, {}, None, "2021-01-05", RECORDS_TO_FILTER_DATE_FORMAT, [2, 3, 5]),
        (DATE_FORMAT, {}, None, None, RECORDS_TO_FILTER_DATE_FORMAT, [2, 3, 4, 5]),
        (
            DATE_FORMAT,
            {"created_at": "2021-01-04"},
            None,
            "2021-01-05",
            RECORDS_TO_FILTER_DATE_FORMAT,
            [3],
        ),
        (
            DATE_FORMAT,
            {"created_at": "2021-01-04"},
            None,
            None,
            RECORDS_TO_FILTER_DATE_FORMAT,
            [3, 4],
        ),
        (
            DATE_FORMAT,
            {},
            "{{ record['id'] % 2 == 1 }}",
            "2021-01-05",
            RECORDS_TO_FILTER_DATE_FORMAT,
            [3, 5],
        ),
        (
            DATE_TIME_WITH_TZ_FORMAT,
            {},
            None,
            "2021-01-05T00:00:00+00:00",
            RECORDS_TO_FILTER_DATE_TIME_WITH_TZ_FORMAT,
            [2, 3],
        ),
        (
            DATE_TIME_WITH_TZ_FORMAT,
            {},
            None,
            None,
            RECORDS_TO_FILTER_DATE_TIME_WITH_TZ_FORMAT,
            [2, 3, 4],
        ),
        (
            DATE_TIME_WITH_TZ_FORMAT,
            {"created_at": "2021-01-04T00:00:00+00:00"},
            None,
            "2021-01-05T00:00:00+00:00",
            RECORDS_TO_FILTER_DATE_TIME_WITH_TZ_FORMAT,
            [3],
        ),
        (
            DATE_TIME_WITH_TZ_FORMAT,
            {"created_at": "2021-01-04T00:00:00+00:00"},
            None,
            None,
            RECORDS_TO_FILTER_DATE_TIME_WITH_TZ_FORMAT,
            [3, 4],
        ),
        (
            DATE_TIME_WITH_TZ_FORMAT,
            {},
            "{{ record['id'] % 2 == 1 }}",
            "2021-01-05T00:00:00+00:00",
            RECORDS_TO_FILTER_DATE_TIME_WITH_TZ_FORMAT,
            [3],
        ),
        (
            DATE_TIME_WITHOUT_TZ_FORMAT,
            {},
            None,
            "2021-01-05T00:00:00",
            RECORDS_TO_FILTER_DATE_TIME_WITHOUT_TZ_FORMAT,
            [2, 3],
        ),
        (
            DATE_TIME_WITHOUT_TZ_FORMAT,
            {},
            None,
            None,
            RECORDS_TO_FILTER_DATE_TIME_WITHOUT_TZ_FORMAT,
            [2, 3, 4],
        ),
        (
            DATE_TIME_WITHOUT_TZ_FORMAT,
            {"created_at": "2021-01-04T00:00:00"},
            None,
            "2021-01-05T00:00:00",
            RECORDS_TO_FILTER_DATE_TIME_WITHOUT_TZ_FORMAT,
            [3],
        ),
        (
            DATE_TIME_WITHOUT_TZ_FORMAT,
            {"created_at": "2021-01-04T00:00:00"},
            None,
            None,
            RECORDS_TO_FILTER_DATE_TIME_WITHOUT_TZ_FORMAT,
            [3, 4],
        ),
        (
            DATE_TIME_WITHOUT_TZ_FORMAT,
            {},
            "{{ record['id'] % 2 == 1 }}",
            "2021-01-05T00:00:00",
            RECORDS_TO_FILTER_DATE_TIME_WITHOUT_TZ_FORMAT,
            [3],
        ),
    ],
    ids=[
        "date_format_no_stream_state_no_record_filter",
        "date_format_no_stream_state_no_end_date_no_record_filter",
        "date_format_with_stream_state_no_record_filter",
        "date_format_with_stream_state_no_end_date_no_record_filter",
        "date_format_no_stream_state_with_record_filter",
        "date_time_with_tz_format_no_stream_state_no_record_filter",
        "date_time_with_tz_format_no_stream_state_no_end_date_no_record_filter",
        "date_time_with_tz_format_with_stream_state_no_record_filter",
        "date_time_with_tz_format_with_stream_state_no_end_date_no_record_filter",
        "date_time_with_tz_format_no_stream_state_with_record_filter",
        "date_time_without_tz_format_no_stream_state_no_record_filter",
        "date_time_without_tz_format_no_stream_state_no_end_date_no_record_filter",
        "date_time_without_tz_format_with_stream_state_no_record_filter",
        "date_time_without_tz_format_with_stream_state_no_end_date_no_record_filter",
        "date_time_without_tz_format_no_stream_state_with_record_filter",
    ],
)
def test_client_side_record_filter_decorator_no_parent_stream(
    datetime_format: str,
    stream_state: Optional[Mapping],
    record_filter_expression: str,
    end_datetime: Optional[str],
    records_to_filter: List[Mapping],
    expected_record_ids: List[int],
):
    datetime_based_cursor = ConcurrentCursor(
        stream_name="any_stream",
        stream_namespace=None,
        stream_state=stream_state,
        message_repository=Mock(),
        connector_state_manager=Mock(),
        connector_state_converter=CustomFormatConcurrentStreamStateConverter(
            datetime_format=datetime_format
        ),
        cursor_field=CursorField("created_at"),
        slice_boundary_fields=("start", "end"),
        start=datetime(2021, 1, 1, tzinfo=timezone.utc),
        end_provider=lambda: ab_datetime_parse(end_datetime) if end_datetime else ab_datetime_now(),
        slice_range=timedelta(days=365 * 10),
    )

    record_filter_decorator = ClientSideIncrementalRecordFilterDecorator(
        config={},
        condition=record_filter_expression,
        parameters={},
        cursor=datetime_based_cursor,
    )

    filtered_records = list(
        record_filter_decorator.filter_records(
            records=records_to_filter,
            stream_state=stream_state,
            stream_slice={},
            next_page_token=None,
        )
    )

    assert [x.get("id") for x in filtered_records] == expected_record_ids


@pytest.mark.parametrize(
    "stream_state, cursor_type, expected_record_ids",
    [
        # Use only DatetimeBasedCursor
        ({}, "datetime", [2, 3, 5]),
        # Use GlobalSubstreamCursor with no state
        ({}, "global_substream", [2, 3, 5]),
        # Use GlobalSubstreamCursor with global state
        ({"state": {"created_at": "2021-01-03"}}, "global_substream", [2, 3]),
        # Use PerPartitionWithGlobalCursor with partition state
        (
            {
                "use_global_cursor": False,
                "state": {"created_at": "2021-01-10"},
                "states": [
                    {
                        "partition": {"id": "some_parent_id", "parent_slice": {}},
                        "cursor": {"created_at": "2021-01-03"},
                    }
                ],
            },
            "per_partition_with_global",
            [2, 3],
        ),
        # Use PerPartitionWithGlobalCursor with global state
        (
            {
                "use_global_cursor": True,
                "state": {"created_at": "2021-01-03"},
                "states": [
                    {
                        "partition": {"id": "some_parent_id", "parent_slice": {}},
                        "cursor": {"created_at": "2021-01-13"},
                    }
                ],
            },
            "global_substream",
            [2, 3],
        ),
        # Use PerPartitionWithGlobalCursor with partition state missing, global cursor used
        (
            {"use_global_cursor": True, "state": {"created_at": "2021-01-03"}},
            "per_partition_with_global",
            [2, 3],
        ),
    ],
    ids=[
        "datetime_cursor_only",
        "global_substream_no_state",
        "global_substream_with_state",
        "per_partition_with_partition_state",
        "per_partition_with_global_state",
        "per_partition_partition_missing_global_cursor_used",
    ],
)
def test_client_side_record_filter_decorator_with_cursor_types(
    stream_state: Optional[Mapping], cursor_type: str, expected_record_ids: List[int]
):
    def date_time_based_cursor_factory(stream_state, runtime_lookback_window) -> ConcurrentCursor:
        return ConcurrentCursor(
            stream_name="any_stream",
            stream_namespace=None,
            stream_state=stream_state,
            message_repository=Mock(),
            connector_state_manager=Mock(),
            connector_state_converter=CustomFormatConcurrentStreamStateConverter(
                datetime_format=DATE_FORMAT
            ),
            cursor_field=CursorField("created_at"),
            slice_boundary_fields=("start", "end"),
            start=datetime(2021, 1, 1, tzinfo=timezone.utc),
            end_provider=lambda: datetime(2021, 1, 5, tzinfo=timezone.utc),
            slice_range=timedelta(days=365 * 10),
            cursor_granularity=timedelta(days=1),
            lookback_window=runtime_lookback_window,
        )

    date_time_based_cursor = date_time_based_cursor_factory(stream_state, timedelta(0))

    substream_cursor = None
    partition_router = SubstreamPartitionRouter(
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

    if cursor_type == "datetime":
        # Use only DatetimeBasedCursor
        pass  # No additional cursor needed
    elif cursor_type in ["global_substream", "per_partition_with_global"]:
        # Create GlobalSubstreamCursor instance
        substream_cursor = ConcurrentPerPartitionCursor(
            cursor_factory=ConcurrentCursorFactory(date_time_based_cursor_factory),
            partition_router=partition_router,
            stream_name="a_stream",
            stream_namespace=None,
            stream_state=stream_state,
            message_repository=Mock(),
            connector_state_manager=Mock(),
            connector_state_converter=CustomFormatConcurrentStreamStateConverter(
                datetime_format=DATE_FORMAT
            ),
            cursor_field=CursorField("created_at"),
            use_global_cursor=cursor_type == "global_substream",
            attempt_to_create_cursor_if_not_provided=True,
        )
    else:
        raise ValueError(f"Unsupported cursor type: {cursor_type}")

    # Create the record_filter_decorator with appropriate cursor
    record_filter_decorator = ClientSideIncrementalRecordFilterDecorator(
        config={},
        parameters={},
        cursor=substream_cursor or date_time_based_cursor,
    )

    # The partition we're testing
    stream_slice = StreamSlice(
        partition={"id": "some_parent_id", "parent_slice": {}}, cursor_slice={}
    )
    records_with_stream_slice = [
        Record(data=x, associated_slice=stream_slice, stream_name="test_stream")
        for x in RECORDS_TO_FILTER_DATE_FORMAT
    ]
    filtered_records = list(
        record_filter_decorator.filter_records(
            records=records_with_stream_slice,
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=None,
        )
    )

    assert [x.get("id") for x in filtered_records] == expected_record_ids
