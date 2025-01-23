"""
Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""

from datetime import datetime, timedelta, timezone

import freezegun
import pytest

from airbyte_cdk.utils.datetime_helpers import (
    AirbyteDateTime,
    add_seconds,
    format,
    is_valid_format,
    now,
    parse,
    subtract_seconds,
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
    """Test now() returns current time in UTC."""
    dt = now()
    assert isinstance(dt, AirbyteDateTime)
    assert str(dt) == "2023-03-14T15:09:26.535897Z"


def test_parse():
    """Test parsing various datetime string formats."""
    # Test ISO8601/RFC3339
    dt = parse("2023-03-14T15:09:26Z")
    assert isinstance(dt, AirbyteDateTime)
    assert str(dt) == "2023-03-14T15:09:26Z"

    # Test with timezone offset
    dt = parse("2023-03-14T15:09:26-04:00")
    assert str(dt) == "2023-03-14T15:09:26-04:00"

    # Test without timezone (should assume UTC)
    dt = parse("2023-03-14T15:09:26")
    assert str(dt) == "2023-03-14T15:09:26Z"

    # Test invalid format
    with pytest.raises(ValueError):
        parse("invalid datetime")


def test_format():
    """Test formatting various datetime objects."""
    # Test formatting standard datetime
    standard_dt = datetime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
    assert format(standard_dt) == "2023-03-14T15:09:26Z"

    # Test formatting naive datetime
    naive_dt = datetime(2023, 3, 14, 15, 9, 26)
    assert format(naive_dt) == "2023-03-14T15:09:26Z"

    # Test formatting AirbyteDateTime
    airbyte_dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
    assert format(airbyte_dt) == "2023-03-14T15:09:26Z"


def test_add_subtract_seconds():
    """Test adding and subtracting seconds from datetime objects."""
    dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
    
    # Test adding seconds
    result = add_seconds(dt, 3600)  # Add 1 hour
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T16:09:26Z"

    # Test subtracting seconds
    result = subtract_seconds(dt, 3600)  # Subtract 1 hour
    assert isinstance(result, AirbyteDateTime)
    assert str(result) == "2023-03-14T14:09:26Z"


def test_is_valid_format():
    """Test datetime string format validation."""
    assert is_valid_format("2023-03-14T15:09:26Z")
    assert is_valid_format("2023-03-14T15:09:26.123Z")
    assert is_valid_format("2023-03-14T15:09:26-04:00")
    assert not is_valid_format("invalid datetime")
    assert not is_valid_format("2023-03-14 15:09:26")  # Missing T delimiter
