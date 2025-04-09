"""
Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""

from datetime import datetime, timedelta, timezone

import freezegun
import pytest

from airbyte_cdk.utils.datetime_helpers import (
    AirbyteDateTime,
    ab_datetime_format,
    ab_datetime_now,
    ab_datetime_parse,
    ab_datetime_try_parse,
)


def test_airbyte_datetime_str_representation():
    """Test that AirbyteDateTime provides consistent string representation."""
    dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, 535897, tzinfo=timezone.utc)
    assert str(dt) == "2023-03-14T15:09:26.535897+00:00"

    # Test non-UTC timezone
    tz = timezone(timedelta(hours=-4))
    dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, 535897, tzinfo=tz)
    assert str(dt) == "2023-03-14T15:09:26.535897-04:00"


def test_airbyte_datetime_from_datetime():
    """Test conversion from standard datetime."""
    standard_dt = datetime(2023, 3, 14, 15, 9, 26, 535897, tzinfo=timezone.utc)
    airbyte_dt = AirbyteDateTime.from_datetime(standard_dt)
    assert isinstance(airbyte_dt, AirbyteDateTime)
    assert str(airbyte_dt) == "2023-03-14T15:09:26.535897+00:00"

    # Test naive datetime conversion (should assume UTC)
    naive_dt = datetime(2023, 3, 14, 15, 9, 26, 535897)
    airbyte_dt = AirbyteDateTime.from_datetime(naive_dt)
    assert str(airbyte_dt) == "2023-03-14T15:09:26.535897+00:00"


@freezegun.freeze_time("2023-03-14T15:09:26.535897Z")
def test_now():
    """Test ab_datetime_now() returns current time in UTC."""
    dt = ab_datetime_now()
    assert isinstance(dt, AirbyteDateTime)
    assert str(dt) == "2023-03-14T15:09:26.535897+00:00"


@pytest.mark.parametrize(
    "input_value,expected_output,error_type,error_match",
    [
        # Valid formats - must have T delimiter and timezone
        ("2023-03-14T15:09:26+00:00", "2023-03-14T15:09:26+00:00", None, None),  # Basic UTC format
        (
            "2023-03-14T15:09:26.123+00:00",
            "2023-03-14T15:09:26.123000+00:00",
            None,
            None,
        ),  # With milliseconds
        (
            "2023-03-14T15:09:26.123456+00:00",
            "2023-03-14T15:09:26.123456+00:00",
            None,
            None,
        ),  # With microseconds
        (
            "2023-03-14T15:09:26-04:00",
            "2023-03-14T15:09:26-04:00",
            None,
            None,
        ),  # With timezone offset
        ("2023-03-14T15:09:26Z", "2023-03-14T15:09:26+00:00", None, None),  # With Z timezone
        (
            "2023-03-14T00:00:00+00:00",
            "2023-03-14T00:00:00+00:00",
            None,
            None,
        ),  # Full datetime with zero time
        (
            "2023-03-14T15:09:26GMT",
            "2023-03-14T15:09:26+00:00",
            None,
            None,
        ),  # Non-standard timezone name ok
        (
            "2023-03-14T15:09:26",
            "2023-03-14T15:09:26+00:00",
            None,
            None,
        ),  # Missing timezone, assume UTC
        (
            "2023-03-14 15:09:26",
            "2023-03-14T15:09:26+00:00",
            None,
            None,
        ),  # Missing T delimiter ok, assume UTC
        (
            "2023-03-14",
            "2023-03-14T00:00:00+00:00",
            None,
            None,
        ),  # Date only, missing time and timezone
        (
            "2023/03/14T15:09:26Z",
            "2023-03-14T15:09:26+00:00",
            None,
            None,
        ),  # Wrong date separator, ok
        # Valid formats
        ("2023-03-14T15:09:26Z", "2023-03-14T15:09:26+00:00", None, None),
        ("2023-03-14T15:09:26-04:00", "2023-03-14T15:09:26-04:00", None, None),
        ("2023-03-14T15:09:26", "2023-03-14T15:09:26+00:00", None, None),
        ("2023-03-14T15:09:26.123456Z", "2023-03-14T15:09:26.123456+00:00", None, None),
        (1678806000, "2023-03-14T15:00:00+00:00", None, None),
        ("1678806000", "2023-03-14T15:00:00+00:00", None, None),
        ("2023-12-14", "2023-12-14T00:00:00+00:00", None, None),
        # Invalid formats
        ("invalid datetime", None, ValueError, "Could not parse datetime string: invalid datetime"),
        ("not_a_number", None, ValueError, "Could not parse datetime string: not_a_number"),
        (-1, None, ValueError, "Timestamp cannot be negative"),
        (32503683600, None, ValueError, "Timestamp value too large"),
        ("-1", None, ValueError, "Timestamp cannot be negative"),
        ("32503683600", None, ValueError, "Timestamp value too large"),
        # Invalid date components
        ("2023-13-14", None, ValueError, "Invalid date format: 2023-13-14"),
        ("2023-12-32", None, ValueError, "Invalid date format: 2023-12-32"),
        ("2023-00-14", None, ValueError, "Invalid date format: 2023-00-14"),
        ("2023-12-00", None, ValueError, "Invalid date format: 2023-12-00"),
        # Non-standard separators and formats, ok
        ("2023/12/14", "2023-12-14T00:00:00+00:00", None, None),
        ("2023-03-14 15:09:26Z", "2023-03-14T15:09:26+00:00", None, None),
        ("2023-03-14T15:09:26GMT", "2023-03-14T15:09:26+00:00", None, None),
        # Invalid time components
        (
            "2023-03-14T25:09:26Z",
            None,
            ValueError,
            "Could not parse datetime string: 2023-03-14T25:09:26Z",
        ),
        (
            "2023-03-14T15:99:26Z",
            None,
            ValueError,
            "Could not parse datetime string: 2023-03-14T15:99:26Z",
        ),
        (
            "2023-03-14T15:09:99Z",
            None,
            ValueError,
            "Could not parse datetime string: 2023-03-14T15:09:99Z",
        ),
    ],
    # ("invalid datetime", None),  # Completely invalid
    # ("15:09:26Z", None),  # Missing date component
    # ("2023-03-14T25:09:26Z", None),  # Invalid hour
    # ("2023-03-14T15:99:26Z", None),  # Invalid minute
    # ("2023-03-14T15:09:99Z", None),  # Invalid second
    # ("2023-02-30T00:00:00Z", None),  # Impossible date
)
def test_parse(input_value, expected_output, error_type, error_match):
    """Test parsing various datetime string formats."""
    if error_type:
        with pytest.raises(error_type, match=error_match):
            ab_datetime_parse(input_value)
        assert not ab_datetime_try_parse(input_value)
    else:
        dt = ab_datetime_parse(input_value)
        assert isinstance(dt, AirbyteDateTime)
        assert str(dt) == expected_output
        assert ab_datetime_try_parse(input_value) and ab_datetime_try_parse(input_value) == dt


@pytest.mark.parametrize(
    "input_dt,expected_output",
    [
        # Standard datetime with UTC timezone
        (datetime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc), "2023-03-14T15:09:26+00:00"),
        # Naive datetime (should assume UTC)
        (datetime(2023, 3, 14, 15, 9, 26), "2023-03-14T15:09:26+00:00"),
        # AirbyteDateTime with UTC timezone
        (AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc), "2023-03-14T15:09:26+00:00"),
        # Datetime with microseconds
        (
            datetime(2023, 3, 14, 15, 9, 26, 123456, tzinfo=timezone.utc),
            "2023-03-14T15:09:26.123456+00:00",
        ),
        # Datetime with non-UTC timezone
        (
            datetime(2023, 3, 14, 15, 9, 26, tzinfo=timezone(timedelta(hours=-4))),
            "2023-03-14T15:09:26-04:00",
        ),
    ],
)
def test_format(input_dt, expected_output):
    """Test formatting various datetime objects."""
    assert ab_datetime_format(input_dt) == expected_output


def test_operator_overloading():
    """Test datetime operator overloading (+, -, etc.)."""
    dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)

    # Test adding timedelta
    delta = timedelta(hours=1)
    result = dt + delta
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T16:09:26+00:00"

    # Test reverse add (timedelta + datetime)
    result = delta + dt
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T16:09:26+00:00"

    # Test subtracting timedelta
    result = dt - delta
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T14:09:26+00:00"

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
    assert str(result) == "2023-03-14T16:09:26+00:00"

    result = dt.subtract(delta)
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T14:09:26+00:00"

    # Test invalid operations
    with pytest.raises(TypeError):
        _ = dt + "invalid"
    with pytest.raises(TypeError):
        _ = "invalid" + dt
    with pytest.raises(TypeError):
        _ = dt - "invalid"
    with pytest.raises(TypeError):
        _ = "invalid" - dt


def test_epoch_millis():
    """Test Unix epoch millisecond timestamp conversion methods."""
    # Test to_epoch_millis()
    dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
    assert dt.to_epoch_millis() == 1678806566000

    # Test from_epoch_millis()
    dt2 = AirbyteDateTime.from_epoch_millis(1678806566000)
    assert str(dt2) == "2023-03-14T15:09:26+00:00"

    # Test roundtrip conversion
    dt3 = AirbyteDateTime.from_epoch_millis(dt.to_epoch_millis())
    assert dt3 == dt
