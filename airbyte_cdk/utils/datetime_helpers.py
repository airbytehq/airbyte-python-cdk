"""
Copyright (c) 2023 Airbyte, Inc., all rights reserved.

This module provides a custom datetime class and helper functions for consistent datetime handling across Airbyte.
All datetime strings are formatted according to ISO8601/RFC3339 with 'T' delimiter and 'Z' for UTC timezone.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union

from dateutil import parser


class AirbyteDateTime(datetime):
    """A datetime class that ensures consistent ISO8601/RFC3339 string representation."""

    def __new__(cls, *args: Any, **kwargs: Any) -> "AirbyteDateTime":
        # Ensure we're creating a timezone-aware datetime
        self = super().__new__(cls, *args, **kwargs)
        if self.tzinfo is None:
            return self.replace(tzinfo=timezone.utc)
        return self

    @classmethod
    def from_datetime(cls, dt: datetime) -> "AirbyteDateTime":
        """Convert a standard datetime to AirbyteDateTime."""
        return cls(
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            dt.microsecond,
            dt.tzinfo or timezone.utc,
        )

    def __str__(self) -> str:
        """
        Returns the datetime in ISO8601/RFC3339 format with 'T' delimiter.
        Always includes timezone, using 'Z' for UTC.
        """
        # Ensure we have a tz-aware datetime
        aware_self = self if self.tzinfo else self.replace(tzinfo=timezone.utc)
        iso = aware_self.isoformat()
        return iso.replace("+00:00", "Z") if aware_self.tzinfo == timezone.utc else iso

    def __repr__(self) -> str:
        """Returns the same string representation as __str__ for consistency."""
        return self.__str__()


def now() -> AirbyteDateTime:
    """Returns the current time as an AirbyteDateTime in UTC."""
    return AirbyteDateTime.from_datetime(datetime.now(timezone.utc))


def parse(dt_str: Union[str, int]) -> AirbyteDateTime:
    """
    Parses a datetime string or timestamp into an AirbyteDateTime.
    Handles:
    - ISO8601/RFC3339 format strings
    - Unix timestamps (as integers or strings)
    - Falls back to dateutil.parser for other string formats
    Always returns a timezone-aware datetime (defaults to UTC if no timezone specified).
    """
    try:
        if isinstance(dt_str, int) or (isinstance(dt_str, str) and dt_str.isdigit()):
            # Always treat numeric values as Unix timestamps (UTC)
            timestamp = int(dt_str)
            # Use utcfromtimestamp to ensure consistent UTC handling without local timezone influence
            dt_obj = datetime.fromtimestamp(timestamp, timezone.utc)
            return AirbyteDateTime.from_datetime(dt_obj)

        if not isinstance(dt_str, str):
            raise ValueError(f"Expected string or integer, got {type(dt_str)}")

        # For string inputs, first check if it's a valid datetime format
        if isinstance(dt_str, str):
            if dt_str.isdigit():
                # Handle Unix timestamp as string
                try:
                    timestamp = int(dt_str)
                    dt_obj = datetime.utcfromtimestamp(timestamp)
                    return AirbyteDateTime.from_datetime(dt_obj.replace(tzinfo=timezone.utc))
                except (ValueError, TypeError, OSError):
                    raise ValueError(f"Invalid timestamp: {dt_str}")
            # For date-only strings (YYYY-MM-DD), add time component
            if "T" not in dt_str and ":" not in dt_str:
                # Check for wrong separators
                if "/" in dt_str:
                    raise ValueError(f"Invalid date format (expected YYYY-MM-DD): {dt_str}")
                parts = dt_str.split("-")
                if len(parts) != 3:
                    raise ValueError(f"Invalid date format (expected YYYY-MM-DD): {dt_str}")
                try:
                    # Validate date components before adding time
                    year, month, day = map(int, parts)
                    if not (1 <= month <= 12 and 1 <= day <= 31):
                        raise ValueError(f"Invalid date components in: {dt_str}")
                    # Create datetime directly instead of string manipulation
                    return AirbyteDateTime(year, month, day, tzinfo=timezone.utc)
                except ValueError as e:
                    raise ValueError(f"Invalid date format: {dt_str}") from e
            # For string inputs, check if it uses 'Z' timezone format
        if isinstance(dt_str, str) and dt_str.endswith("Z"):
            # Remove Z, parse as UTC, then ensure we output Z format
            dt_obj = parser.parse(dt_str[:-1])
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            return AirbyteDateTime.from_datetime(dt_obj)

        # Normal parsing for other formats
        dt_obj = parser.parse(dt_str)
        # For strings without timezone, assume UTC as documented
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        return AirbyteDateTime.from_datetime(dt_obj)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Could not parse datetime string: {dt_str}") from e


def format(dt: Union[datetime, AirbyteDateTime]) -> str:
    """
    Formats any datetime object as an ISO8601/RFC3339 string with 'T' delimiter.
    If the datetime is naive (no timezone), UTC is assumed.
    Returns 'Z' for UTC timezone, otherwise keeps the original timezone offset.
    """
    if isinstance(dt, AirbyteDateTime):
        return str(dt)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    iso = dt.isoformat()
    return iso.replace("+00:00", "Z") if dt.tzinfo == timezone.utc else iso


def add_seconds(
    dt: Union[datetime, AirbyteDateTime], seconds: Union[int, float]
) -> AirbyteDateTime:
    """
    Adds the specified number of seconds to a datetime.
    Returns an AirbyteDateTime.
    """
    if not isinstance(dt, AirbyteDateTime):
        dt = AirbyteDateTime.from_datetime(dt)
    return AirbyteDateTime.from_datetime(dt + timedelta(seconds=seconds))


def subtract_seconds(
    dt: Union[datetime, AirbyteDateTime], seconds: Union[int, float]
) -> AirbyteDateTime:
    """
    Subtracts the specified number of seconds from a datetime.
    Returns an AirbyteDateTime.
    """
    return add_seconds(dt, -seconds)


def is_valid_format(dt_str: str) -> bool:
    """
    Checks if a datetime string matches ISO8601/RFC3339 format with 'T' delimiter.
    Requires:
    - 'T' as date/time delimiter
    - Timezone specification (Z, +HH:MM, or -HH:MM)
    """
    try:
        # First try parsing with dateutil to validate basic datetime structure
        dt = parser.parse(dt_str)
        # Then verify the string contains required ISO8601/RFC3339 elements
        # For date-only strings, we'll consider them valid
        if ":" in dt_str and "T" not in dt_str:  # If it has time but no T delimiter
            return False
        # Must have valid timezone format (Z, +HH:MM, or -HH:MM) if time is present
        if ":" in dt_str and not any(x in dt_str for x in ("+", "-", "Z")):
            return False
        # Additional check for timezone format - only allow Z, +HH:MM, -HH:MM
        if dt_str.endswith("Z"):
            return True
        # Check for +HH:MM or -HH:MM format
        if len(dt_str) >= 6:  # Need at least 6 chars for timezone offset
            tz_part = dt_str[-6:]  # Get last 6 chars (e.g., +00:00 or -04:00)
            if (
                tz_part[0] in ("+", "-")
                and tz_part[1:3].isdigit()
                and tz_part[3] == ":"
                and tz_part[4:].isdigit()
            ):
                return True
        return False
    except (ValueError, TypeError):
        return False
