#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import datetime

import pytest

from airbyte_cdk.sources.declarative.interpolation.macros import macros


@pytest.mark.parametrize(
    "test_name, fn_name, found_in_macros",
    [
        ("test_now_utc", "now_utc", True),
        ("test_today_utc", "today_utc", True),
        ("test_max", "max", True),
        ("test_min", "min", True),
        ("test_day_delta", "day_delta", True),
        ("test_format_datetime", "format_datetime", True),
        ("test_duration", "duration", True),
        ("test_not_a_macro", "thisisnotavalidmacro", False),
    ],
)
def test_macros_export(test_name, fn_name, found_in_macros):
    if found_in_macros:
        assert fn_name in macros
    else:
        assert fn_name not in macros


@pytest.mark.parametrize(
    "input_value, format, input_format, expected_output",
    [
        ("2022-01-01T01:01:01Z", "%Y-%m-%d", None, "2022-01-01"),
        ("2022-01-01", "%Y-%m-%d", None, "2022-01-01"),
        ("2022-01-01T00:00:00Z", "%Y-%m-%d", None, "2022-01-01"),
        (
            "2022-01-01T00:00:00Z",
            "%Y-%m-%d",
            None,
            "2022-01-01",
        ),
        (
            "2022-01-01T01:01:01Z",
            "%Y-%m-%dT%H:%M:%SZ",
            None,
            "2022-01-01T01:01:01Z",
        ),
        (
            "2022-01-01T01:01:01-0800",
            "%Y-%m-%dT%H:%M:%SZ",
            None,
            "2022-01-01T09:01:01Z",
        ),
        (
            datetime.datetime(2022, 1, 1, 1, 1, 1),
            "%Y-%m-%d",
            None,
            "2022-01-01",
        ),
        (
            datetime.datetime(2022, 1, 1, 1, 1, 1),
            "%Y-%m-%dT%H:%M:%SZ",
            None,
            "2022-01-01T01:01:01Z",
        ),
        (
            "Sat, 01 Jan 2022 01:01:01 +0000",
            "%Y-%m-%d",
            "%a, %d %b %Y %H:%M:%S %z",
            "2022-01-01",
        ),
        (
            "2022-01-01T01:01:01Z",
            "%s",
            "%Y-%m-%dT%H:%M:%SZ",
            "1640998861",  # Fails when run outside the GMT time zone
        ),
        (
            "2022-01-01T01:01:01Z",
            "%epoch_microseconds",
            "%Y-%m-%dT%H:%M:%SZ",
            "1640998861000000",   # Fails when run outside the GMT time zone
        ),
    ],
    ids=[
        "test_datetime_string_to_date",
        "test_date_string_to_date",
        "test_datetime_string_to_date",
        "test_datetime_with_tz_string_to_date",
        "test_datetime_string_to_datetime",
        "test_datetime_string_with_tz_to_datetime",
        "test_datetime_object_tz_to_date",
        "test_datetime_object_tz_to_datetime",
        "test_datetime_string_to_rfc2822_date",
        "test_datetime_string_to_timestamp_in_seconds",
        "test_datetime_string_to_timestamp_in_microseconds",
    ],
)
def test_format_datetime(input_value, format, input_format, expected_output):
    format_datetime = macros["format_datetime"]
    assert format_datetime(input_value, format, input_format) == expected_output


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        ("P1D", datetime.timedelta(days=1)),
        ("P6DT23H", datetime.timedelta(days=6, hours=23)),
    ],
    ids=[
        "test_one_day",
        "test_6_days_23_hours",
    ],
)
def test_duration(input_value: str, expected_output: datetime.timedelta):
    duration_fn = macros["duration"]
    assert duration_fn(input_value) == expected_output


@pytest.mark.parametrize(
    "test_name, input_value, expected_output",
    [
        ("test_int_input", 1646006400, 1646006400),
        ("test_float_input", 100.0, 100),
        ("test_float_input_is_floored", 100.9, 100),
        ("test_string_date_iso8601", "2022-02-28", 1646006400),
        ("test_string_datetime_midnight_iso8601", "2022-02-28T00:00:00Z", 1646006400),
        ("test_string_datetime_midnight_iso8601_with_tz", "2022-02-28T00:00:00-08:00", 1646035200),
        ("test_string_datetime_midnight_iso8601_no_t", "2022-02-28 00:00:00Z", 1646006400),
        ("test_string_datetime_iso8601", "2022-02-28T10:11:12", 1646043072),
    ],
)
def test_timestamp(test_name, input_value, expected_output):
    timestamp_function = macros["timestamp"]
    actual_output = timestamp_function(input_value)
    assert actual_output == expected_output


def test_utc_datetime_to_local_timestamp_conversion():
    """
    This test ensures correct timezone handling independent of the timezone of the system on which the sync is running.
    """
    assert macros["format_datetime"](dt="2020-10-01T00:00:00Z", format="%s") == "1601510400"


@pytest.mark.parametrize(
    "test_name, input_value, expected_output",
    [
        (
            "test_basic_date",
            "2022-01-14",
            datetime.datetime(2022, 1, 14, tzinfo=datetime.timezone.utc),
        ),
        (
            "test_datetime_with_time",
            "2022-01-01 13:45:30",
            datetime.datetime(2022, 1, 1, 13, 45, 30, tzinfo=datetime.timezone.utc),
        ),
        (
            "test_datetime_with_timezone",
            "2022-01-01T13:45:30+00:00",
            datetime.datetime(2022, 1, 1, 13, 45, 30, tzinfo=datetime.timezone.utc),
        ),
        (
            "test_datetime_with_timezone_offset",
            "2022-01-01T13:45:30+05:30",
            datetime.datetime(2022, 1, 1, 8, 15, 30, tzinfo=datetime.timezone.utc),
        ),
        (
            "test_datetime_with_microseconds",
            "2022-01-01T13:45:30.123456Z",
            datetime.datetime(2022, 1, 1, 13, 45, 30, 123456, tzinfo=datetime.timezone.utc),
        ),
    ],
)
def test_give_valid_date_str_to_datetime_returns_datetime_object(
    test_name, input_value, expected_output
):
    str_to_datetime_fn = macros["str_to_datetime"]
    actual_output = str_to_datetime_fn(input_value)
    assert actual_output == expected_output


def test_given_invalid_date_str_to_datetime_raises_value_error():
    str_to_datetime_fn = macros["str_to_datetime"]
    with pytest.raises(ValueError):
        str_to_datetime_fn("invalid-date")
