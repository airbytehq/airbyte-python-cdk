"""
Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""

from datetime import datetime, timedelta, timezone

import freezegun
import pytest

from airbyte_cdk.utils.datetime_helpers import (
    AirbyteDateTime,
    ab_datetime_add_seconds,
    ab_datetime_format,
    ab_datetime_is_valid_format,
    ab_datetime_now,
    ab_datetime_parse,
    ab_datetime_subtract_seconds,
)


def test_airbyte_datetime_str_representation():
    """Test that AirbyteDateTime provides consistent string representation."""
    dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, 535897, tzinfo=timezone.utc)
    assert str(dt) == "2023-03-14T15:09:26.535897Z"

    # Test non-UTC timezone
    tz = timezone(timedelta(hours=-4))
    dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, 535897, tzinfo=tz)
    assert str(dt) == "2023-03-14T15:09:26.535897-04:00"


def test_airbyte_datetime_from_datetime():
    """Test conversion from standard datetime."""
    standard_dt = datetime(2023, 3, 14, 15, 9, 26, 535897, tzinfo=timezone.utc)
    airbyte_dt = AirbyteDateTime.from_datetime(standard_dt)
    assert isinstance(airbyte_dt, AirbyteDateTime)
    assert str(airbyte_dt) == "2023-03-14T15:09:26.535897Z"

    # Test naive datetime conversion (should assume UTC)
    naive_dt = datetime(2023, 3, 14, 15, 9, 26, 535897)
    airbyte_dt = AirbyteDateTime.from_datetime(naive_dt)
    assert str(airbyte_dt) == "2023-03-14T15:09:26.535897Z"


@freezegun.freeze_time("2023-03-14T15:09:26.535897Z")
def test_now():
    """Test ab_datetime_now() returns current time in UTC."""
    dt = ab_datetime_now()
    assert isinstance(dt, AirbyteDateTime)
    assert str(dt) == "2023-03-14T15:09:26.535897Z"


def test_parse():
    """Test parsing various datetime string formats."""
    # Test ISO8601/RFC3339
    dt = ab_datetime_parse("2023-03-14T15:09:26Z")
    assert isinstance(dt, AirbyteDateTime)
    assert str(dt) == "2023-03-14T15:09:26Z"

    # Test with timezone offset
    dt = ab_datetime_parse("2023-03-14T15:09:26-04:00")
    assert str(dt) == "2023-03-14T15:09:26-04:00"

    # Test without timezone (should assume UTC)
    dt = ab_datetime_parse("2023-03-14T15:09:26")
    assert str(dt) == "2023-03-14T15:09:26Z"

    # Test with microseconds
    dt = ab_datetime_parse("2023-03-14T15:09:26.123456Z")
    assert str(dt) == "2023-03-14T15:09:26.123456Z"

    # Test Unix timestamp as integer
    dt = ab_datetime_parse(1678806000)  # 2023-03-14T15:00:00Z
    assert str(dt) == "2023-03-14T15:00:00Z"

    # Test Unix timestamp as string
    dt = ab_datetime_parse("1678806000")  # 2023-03-14T15:00:00Z
    assert str(dt) == "2023-03-14T15:00:00Z"

    # Test date-only format
    dt = ab_datetime_parse("2023-12-14")
    assert str(dt) == "2023-12-14T00:00:00Z"

    # Test invalid formats
    with pytest.raises(ValueError):
        ab_datetime_parse("invalid datetime")

    with pytest.raises(ValueError):
        ab_datetime_parse("not_a_number")  # Invalid when trying to parse as timestamp

    with pytest.raises(ValueError):
        ab_datetime_parse("2023-13-14")  # Invalid month

    with pytest.raises(ValueError):
        ab_datetime_parse("2023-12-32")  # Invalid day

    with pytest.raises(ValueError):
        ab_datetime_parse("2023/12/14")  # Wrong separator


def test_format():
    """Test formatting various datetime objects."""
    # Test formatting standard datetime with UTC timezone
    standard_dt = datetime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
    assert ab_datetime_format(standard_dt) == "2023-03-14T15:09:26Z"

    # Test formatting naive datetime (should assume UTC)
    naive_dt = datetime(2023, 3, 14, 15, 9, 26)
    assert ab_datetime_format(naive_dt) == "2023-03-14T15:09:26Z"

    # Test formatting AirbyteDateTime with UTC timezone
    airbyte_dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
    assert ab_datetime_format(airbyte_dt) == "2023-03-14T15:09:26Z"

    # Test formatting with microseconds
    dt_with_micros = datetime(2023, 3, 14, 15, 9, 26, 123456, tzinfo=timezone.utc)
    assert ab_datetime_format(dt_with_micros) == "2023-03-14T15:09:26.123456Z"

    # Test formatting with non-UTC timezone
    tz = timezone(timedelta(hours=-4))
    dt_with_offset = datetime(2023, 3, 14, 15, 9, 26, tzinfo=tz)
    assert ab_datetime_format(dt_with_offset) == "2023-03-14T15:09:26-04:00"


def test_operator_overloading():
    """Test datetime operator overloading (+, -, etc.)."""
    dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)

    # Test adding timedelta
    delta = timedelta(hours=1)
    result = dt + delta
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T16:09:26Z"

    # Test reverse add (timedelta + datetime)
    result = delta + dt
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T16:09:26Z"

    # Test subtracting timedelta
    result = dt - delta
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T14:09:26Z"

    # Test datetime subtraction (returns timedelta)
    other_dt = AirbyteDateTime(2023, 3, 14, 14, 9, 26, tzinfo=timezone.utc)
    result = dt - other_dt
    assert isinstance(result, timedelta)
    assert result == timedelta(hours=1)

    # Test reverse datetime subtraction
    result = other_dt - dt
    assert isinstance(result, timedelta)
    assert result == timedelta(hours=-1)

    # Test add() and subtract() methods
    result = dt.add(delta)
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T16:09:26Z"

    result = dt.subtract(delta)
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T14:09:26Z"

    # Test invalid operations
    with pytest.raises(TypeError):
        _ = dt + "invalid"
    with pytest.raises(TypeError):
        _ = "invalid" + dt
    with pytest.raises(TypeError):
        _ = dt - "invalid"
    with pytest.raises(TypeError):
        _ = "invalid" - dt


def test_add_subtract_seconds():
    """Test adding and subtracting seconds from datetime objects."""
    dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)

    # Test adding seconds
    result = ab_datetime_add_seconds(dt, 3600)  # Add 1 hour
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T16:09:26Z"

    # Test subtracting seconds
    result = ab_datetime_subtract_seconds(dt, 3600)  # Subtract 1 hour
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T14:09:26Z"


def test_is_valid_format():
    """Test datetime string format validation."""
    # Valid formats
    assert ab_datetime_is_valid_format("2023-03-14T15:09:26Z")  # Basic UTC format
    assert ab_datetime_is_valid_format("2023-03-14T15:09:26.123Z")  # With milliseconds
    assert ab_datetime_is_valid_format("2023-03-14T15:09:26.123456Z")  # With microseconds
    assert ab_datetime_is_valid_format("2023-03-14T15:09:26-04:00")  # With timezone offset
    assert ab_datetime_is_valid_format("2023-03-14T15:09:26+00:00")  # With explicit UTC offset

    # Invalid formats
    assert not ab_datetime_is_valid_format("invalid datetime")  # Completely invalid
    assert not ab_datetime_is_valid_format("2023-03-14 15:09:26")  # Missing T delimiter
    assert not ab_datetime_is_valid_format("2023-03-14")  # Missing time component
    assert not ab_datetime_is_valid_format("15:09:26Z")  # Missing date component
    assert not ab_datetime_is_valid_format("2023-03-14T15:09:26")  # Missing timezone
    assert not ab_datetime_is_valid_format("2023-03-14T15:09:26GMT")  # Invalid timezone format


def test_epoch_millis():
    """Test Unix epoch millisecond timestamp conversion methods."""
    # Test to_epoch_millis()
    dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
    assert dt.to_epoch_millis() == 1678806566000

    # Test from_epoch_millis()
    dt2 = AirbyteDateTime.from_epoch_millis(1678806566000)
    assert str(dt2) == "2023-03-14T15:09:26Z"

    # Test roundtrip conversion
    dt3 = AirbyteDateTime.from_epoch_millis(dt.to_epoch_millis())
    assert dt3 == dt
