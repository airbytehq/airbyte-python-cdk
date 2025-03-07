#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import datetime

import pytest

from airbyte_cdk.sources.declarative.datetime.datetime_parser import DatetimeParser


@pytest.mark.parametrize(
    "input_date, date_format, expected_output_date",
    [
        (
            "2021-01-01T00:00:00.000000+0000",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        ),
        (
            "2021-01-01T00:00:00.000000+0400",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            datetime.datetime(
                2021, 1, 1, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=14400))
            ),
        ),
        (
            "1609459200",
            "%s",
            datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        ),
        (
            "1675092508.873709",
            "%s_as_float",
            datetime.datetime(2023, 1, 30, 15, 28, 28, 873709, tzinfo=datetime.timezone.utc),
        ),
        (
            "1675092508873709",
            "%epoch_microseconds",
            datetime.datetime(2023, 1, 30, 15, 28, 28, 873709, tzinfo=datetime.timezone.utc),
        ),
        (
            "1609459200001",
            "%ms",
            datetime.datetime(2021, 1, 1, 0, 0, 0, 1000, tzinfo=datetime.timezone.utc),
        ),
        (
            "20210101",
            "%Y%m%d",
            datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        ),
        (
            "2021-11-22T08:41:55.640Z",
            "%Y-%m-%dT%H:%M:%S.%_msZ",
            datetime.datetime(2021, 11, 22, 8, 41, 55, 640000, tzinfo=datetime.timezone.utc),
        ),
    ],
    ids=[
        "test_parse_date_iso",
        "test_parse_date_iso_with_timezone_not_utc",
        "test_parse_timestamp",
        "test_parse_timestamp_as_float",
        "test_parse_timestamp_microseconds",
        "test_parse_ms_timestamp",
        "test_parse_date_ms",
        "test_parse_format_datetime_with__ms",
    ],
)
def test_parse_date(input_date: str, date_format: str, expected_output_date: datetime.datetime):
    parser = DatetimeParser()
    output_date = parser.parse(input_date, date_format)
    assert output_date == expected_output_date


@pytest.mark.parametrize(
    "input_dt, datetimeformat, expected_output",
    [
        (
            datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
            "%s",
            "1609459200",
        ),
        (
            datetime.datetime(2021, 1, 1, 0, 0, 0, 1000, tzinfo=datetime.timezone.utc),
            "%ms",
            "1609459200001",
        ),
        (
            datetime.datetime(2023, 1, 30, 15, 28, 28, 873709, tzinfo=datetime.timezone.utc),
            "%s_as_float",
            "1675092508.873709",
        ),
        (
            datetime.datetime(2023, 1, 30, 15, 28, 28, 873709, tzinfo=datetime.timezone.utc),
            "%epoch_microseconds",
            "1675092508873709",
        ),
        (
            datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
            "%Y-%m-%d",
            "2021-01-01",
        ),
        (
            datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
            "%Y%m%d",
            "20210101",
        ),
        (
            datetime.datetime(2021, 11, 22, 8, 41, 55, 640000, tzinfo=datetime.timezone.utc),
            "%Y-%m-%dT%H:%M:%S.%_msZ",
            "2021-11-22T08:41:55.640Z",
        ),
    ],
    ids=[
        "test_format_timestamp",
        "test_format_timestamp_ms",
        "test_format_timestamp_as_float",
        "test_format_timestamp_microseconds",
        "test_format_string",
        "test_format_to_number",
        "test_parse_format_datetime_with__ms",
    ],
)
def test_format_datetime(input_dt: datetime.datetime, datetimeformat: str, expected_output: str):
    parser = DatetimeParser()
    output_date = parser.format(input_dt, datetimeformat)
    assert output_date == expected_output
