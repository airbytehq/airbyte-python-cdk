"""
Copyright (c) 2023 Airbyte, Inc., all rights reserved.

This module provides a custom datetime class and helper functions for consistent datetime handling across Airbyte.
All datetime strings are formatted according to ISO8601/RFC3339 with 'T' delimiter and 'Z' for UTC timezone.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Union

from dateutil import parser


class AirbyteDateTime(datetime):
    """A datetime class that ensures consistent ISO8601/RFC3339 string representation."""

    def __new__(cls, *args, **kwargs):
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
        iso = self.isoformat()
        # Replace +00:00 with Z for UTC
        return iso.replace("+00:00", "Z") if self.tzinfo == timezone.utc else iso

    def __repr__(self) -> str:
        """Returns the same string representation as __str__ for consistency."""
        return self.__str__()


def now() -> AirbyteDateTime:
    """Returns the current time as an AirbyteDateTime in UTC."""
    return AirbyteDateTime.from_datetime(datetime.now(timezone.utc))


def parse(dt_str: str) -> AirbyteDateTime:
    """
    Parses a datetime string into an AirbyteDateTime.
    Handles ISO8601/RFC3339 formats and falls back to dateutil.parser for other formats.
    Always returns a timezone-aware datetime (defaults to UTC if no timezone specified).
    """
    try:
        dt_obj = parser.parse(dt_str)
        return AirbyteDateTime.from_datetime(dt_obj)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Could not parse datetime string: {dt_str}") from e


def format(dt: Union[datetime, AirbyteDateTime]) -> str:
    """
    Formats any datetime object as an ISO8601/RFC3339 string with 'T' delimiter.
    If the datetime is naive (no timezone), UTC is assumed.
    """
    if isinstance(dt, AirbyteDateTime):
        return str(dt)
    
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def add_seconds(dt: Union[datetime, AirbyteDateTime], seconds: Union[int, float]) -> AirbyteDateTime:
    """
    Adds the specified number of seconds to a datetime.
    Returns an AirbyteDateTime.
    """
    if not isinstance(dt, AirbyteDateTime):
        dt = AirbyteDateTime.from_datetime(dt)
    return AirbyteDateTime.from_datetime(dt + timedelta(seconds=seconds))


def subtract_seconds(dt: Union[datetime, AirbyteDateTime], seconds: Union[int, float]) -> AirbyteDateTime:
    """
    Subtracts the specified number of seconds from a datetime.
    Returns an AirbyteDateTime.
    """
    return add_seconds(dt, -seconds)


def is_valid_format(dt_str: str) -> bool:
    """
    Checks if a datetime string matches ISO8601/RFC3339 format with 'T' delimiter.
    """
    try:
        parse(dt_str)
        return True
    except ValueError:
        return False
