"""Provides consistent datetime handling across Airbyte with ISO8601/RFC3339 compliance.

Copyright (c) 2023 Airbyte, Inc., all rights reserved.

This module provides a custom datetime class (AirbyteDateTime) and helper functions that ensure
consistent datetime handling across Airbyte. All datetime strings are formatted according to
ISO8601/RFC3339 standards with 'T' delimiter and '+00:00' for UTC timezone.

Key Features:
    - Timezone-aware datetime objects (defaults to UTC)
    - ISO8601/RFC3339 compliant string formatting
    - Consistent parsing of various datetime formats
    - Support for Unix timestamps and milliseconds
    - Type-safe datetime arithmetic with timedelta

## Basic Usage

```python
from airbyte_cdk.utils.datetime_helpers import AirbyteDateTime, ab_datetime_now, ab_datetime_parse
from datetime import timedelta, timezone

## Current time in UTC
now = ab_datetime_now()
print(now)  # 2023-03-14T15:09:26.535897Z

# Parse various datetime formats
dt = ab_datetime_parse("2023-03-14T15:09:26Z")  # ISO8601/RFC3339
dt = ab_datetime_parse("2023-03-14")  # Date only (assumes midnight UTC)
dt = ab_datetime_parse(1678806566)  # Unix timestamp

## Create with explicit timezone
dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
print(dt)  # 2023-03-14T15:09:26+00:00

# Datetime arithmetic with timedelta
tomorrow = dt + timedelta(days=1)
yesterday = dt - timedelta(days=1)
time_diff = tomorrow - yesterday  # timedelta object
```

## Millisecond Timestamp Handling

```python
# Convert to millisecond timestamp
dt = ab_datetime_parse("2023-03-14T15:09:26Z")
ms = dt.to_epoch_millis()  # 1678806566000

# Create from millisecond timestamp
dt = AirbyteDateTime.from_epoch_millis(1678806566000)
print(dt)  # 2023-03-14T15:09:26Z
```

## Timezone Handling

```python
# Create with non-UTC timezone
tz = timezone(timedelta(hours=-4))  # EDT
dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=tz)
print(dt)  # 2023-03-14T15:09:26-04:00

## Parse with timezone
dt = ab_datetime_parse("2023-03-14T15:09:26-04:00")
print(dt)  # 2023-03-14T15:09:26-04:00

## Naive datetimes are automatically converted to UTC
dt = ab_datetime_parse("2023-03-14T15:09:26")
print(dt)  # 2023-03-14T15:09:26Z
```

# Format Validation

```python
from airbyte_cdk.utils.datetime_helpers import ab_datetime_try_parse

# Validate ISO8601/RFC3339 format
assert ab_datetime_try_parse("2023-03-14T15:09:26Z")       # Basic UTC format
assert ab_datetime_try_parse("2023-03-14T15:09:26-04:00")  # With timezone offset
assert ab_datetime_try_parse("2023-03-14T15:09:26+00:00")  # With explicit UTC offset
assert not ab_datetime_try_parse("2023-03-14 15:09:26Z")   # Invalid: missing T delimiter
assert not ab_datetime_try_parse("foo")                     # Invalid: not a datetime
```
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union, overload

from dateutil import parser
from typing_extensions import Never
from whenever import Instant, LocalDateTime, ZonedDateTime


class AirbyteDateTime(datetime):
    """A timezone-aware datetime class with ISO8601/RFC3339 string representation and operator overloading.

    This class extends the standard datetime class to provide consistent timezone handling
    (defaulting to UTC) and ISO8601/RFC3339 compliant string formatting. It also supports
    operator overloading for datetime arithmetic with timedelta objects.

    Example:
        >>> dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
        >>> str(dt)
        '2023-03-14T15:09:26+00:00'
        >>> dt + timedelta(hours=1)
        '2023-03-14T16:09:26+00:00'
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> "AirbyteDateTime":
        """Creates a new timezone-aware AirbyteDateTime instance.

        Ensures all instances are timezone-aware by defaulting to UTC if no timezone is provided.

        Returns:
            AirbyteDateTime: A new timezone-aware datetime instance.
        """
        self = super().__new__(cls, *args, **kwargs)
        if self.tzinfo is None:
            return self.replace(tzinfo=timezone.utc)
        return self

    @classmethod
    def from_datetime(cls, dt: datetime) -> "AirbyteDateTime":
        """Converts a standard datetime to AirbyteDateTime.

        Args:
            dt: A standard datetime object to convert.

        Returns:
            AirbyteDateTime: A new timezone-aware AirbyteDateTime instance.
        """
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
        """Returns the datetime in ISO8601/RFC3339 format with 'T' delimiter.

        Ensures consistent string representation with timezone, using '+00:00' for UTC.
        Preserves full microsecond precision when present, omits when zero.

        Returns:
            str: ISO8601/RFC3339 formatted string.
        """
        aware_self = self if self.tzinfo else self.replace(tzinfo=timezone.utc)
        base = self.strftime("%Y-%m-%dT%H:%M:%S")
        if self.microsecond:
            base = f"{base}.{self.microsecond:06d}"
        # Format timezone as ±HH:MM
        offset = aware_self.strftime("%z")
        return f"{base}{offset[:3]}:{offset[3:]}"

    def __repr__(self) -> str:
        """Returns the same string representation as __str__ for consistency.

        Returns:
            str: ISO8601/RFC3339 formatted string.
        """
        return self.__str__()

    def add(self, delta: timedelta) -> "AirbyteDateTime":
        """Add a timedelta interval to this datetime.

        This method provides a more explicit alternative to the + operator
        for adding time intervals to datetimes.

        Args:
            delta: The timedelta interval to add.

        Returns:
            AirbyteDateTime: A new datetime with the interval added.

        Example:
            >>> dt = AirbyteDateTime(2023, 3, 14, tzinfo=timezone.utc)
            >>> dt.add(timedelta(hours=1))
            '2023-03-14T01:00:00Z'
        """
        return self + delta

    def subtract(self, delta: timedelta) -> "AirbyteDateTime":
        """Subtract a timedelta interval from this datetime.

        This method provides a more explicit alternative to the - operator
        for subtracting time intervals from datetimes.

        Args:
            delta: The timedelta interval to subtract.

        Returns:
            AirbyteDateTime: A new datetime with the interval subtracted.

        Example:
            >>> dt = AirbyteDateTime(2023, 3, 14, tzinfo=timezone.utc)
            >>> dt.subtract(timedelta(hours=1))
            '2023-03-13T23:00:00Z'
        """
        result = super().__sub__(delta)
        if isinstance(result, datetime):
            return AirbyteDateTime.from_datetime(result)
        raise TypeError("Invalid operation")

    def __add__(self, other: timedelta) -> "AirbyteDateTime":
        """Adds a timedelta to this datetime.

        Args:
            other: A timedelta object to add.

        Returns:
            AirbyteDateTime: A new datetime with the timedelta added.

        Raises:
            TypeError: If other is not a timedelta.
        """
        result = super().__add__(other)
        if isinstance(result, datetime):
            return AirbyteDateTime.from_datetime(result)
        raise TypeError("Invalid operation")

    def __radd__(self, other: timedelta) -> "AirbyteDateTime":
        """Supports timedelta + AirbyteDateTime operation.

        Args:
            other: A timedelta object to add.

        Returns:
            AirbyteDateTime: A new datetime with the timedelta added.

        Raises:
            TypeError: If other is not a timedelta.
        """
        return self.__add__(other)

    @overload  # type: ignore[override]
    def __sub__(self, other: timedelta) -> "AirbyteDateTime": ...

    @overload  # type: ignore[override]
    def __sub__(self, other: Union[datetime, "AirbyteDateTime"]) -> timedelta: ...

    def __sub__(
        self, other: Union[datetime, "AirbyteDateTime", timedelta]
    ) -> Union[timedelta, "AirbyteDateTime"]:  # type: ignore[override]
        """Subtracts a datetime, AirbyteDateTime, or timedelta from this datetime.

        Args:
            other: A datetime, AirbyteDateTime, or timedelta object to subtract.

        Returns:
            Union[timedelta, AirbyteDateTime]: A timedelta if subtracting datetime/AirbyteDateTime,
                or a new datetime if subtracting timedelta.

        Raises:
            TypeError: If other is not a datetime, AirbyteDateTime, or timedelta.
        """
        if isinstance(other, timedelta):
            result = super().__sub__(other)  # type: ignore[call-overload]
            if isinstance(result, datetime):
                return AirbyteDateTime.from_datetime(result)
        elif isinstance(other, (datetime, AirbyteDateTime)):
            result = super().__sub__(other)  # type: ignore[call-overload]
            if isinstance(result, timedelta):
                return result
        raise TypeError(
            f"unsupported operand type(s) for -: '{type(self).__name__}' and '{type(other).__name__}'"
        )

    def __rsub__(self, other: datetime) -> timedelta:
        """Supports datetime - AirbyteDateTime operation.

        Args:
            other: A datetime object.

        Returns:
            timedelta: The time difference between the datetimes.

        Raises:
            TypeError: If other is not a datetime.
        """
        if not isinstance(other, datetime):
            return NotImplemented
        result = other - datetime(
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
            self.microsecond,
            self.tzinfo,
        )
        if isinstance(result, timedelta):
            return result
        raise TypeError("Invalid operation")

    def to_epoch_millis(self) -> int:
        """Return the Unix timestamp in milliseconds for this datetime.

        Returns:
            int: Number of milliseconds since Unix epoch (January 1, 1970).

        Example:
            >>> dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
            >>> dt.to_epoch_millis()
            1678806566000
        """
        return int(self.timestamp() * 1000)

    @classmethod
    def from_epoch_millis(cls, milliseconds: int) -> "AirbyteDateTime":
        """Create an AirbyteDateTime from Unix timestamp in milliseconds.

        Args:
            milliseconds: Number of milliseconds since Unix epoch (January 1, 1970).

        Returns:
            AirbyteDateTime: A new timezone-aware datetime instance (UTC).

        Example:
            >>> dt = AirbyteDateTime.from_epoch_millis(1678806566000)
            >>> str(dt)
            '2023-03-14T15:09:26+00:00'
        """
        return cls.fromtimestamp(milliseconds / 1000.0, timezone.utc)

    @classmethod
    def from_str(cls, dt_str: str) -> "AirbyteDateTime":
        """Thin convenience wrapper around `ab_datetime_parse()`.

        This method attempts to create a new `AirbyteDateTime` using all available parsing
        strategies.

        Raises:
            ValueError: If the value cannot be parsed into a valid datetime object.
        """
        return ab_datetime_parse(dt_str)


def ab_datetime_now() -> AirbyteDateTime:
    """Returns the current time as an AirbyteDateTime in UTC timezone.

    Previously named: now()

    Returns:
        AirbyteDateTime: Current UTC time.

    Example:
        >>> dt = ab_datetime_now()
        >>> str(dt)  # Returns current time in ISO8601/RFC3339
        '2023-03-14T15:09:26.535897Z'
    """
    return AirbyteDateTime.from_datetime(datetime.now(timezone.utc))


def ab_datetime_parse(dt_str: str | int) -> AirbyteDateTime:
    """Parses a datetime string or timestamp into an AirbyteDateTime with timezone awareness.

    Previously named: parse()

    Handles:
        - ISO8601/RFC3339 format strings (with 'T' delimiter)
        - Unix timestamps (as integers or strings)
        - Date-only strings (YYYY-MM-DD)
        - Timezone-aware formats (+00:00 for UTC, or ±HH:MM offset)

    Always returns a timezone-aware datetime (defaults to UTC if no timezone specified).

    Args:
        dt_str: A datetime string in ISO8601/RFC3339 format, Unix timestamp (int/str),
            or other recognizable datetime format.

    Returns:
        AirbyteDateTime: A timezone-aware datetime object.

    Raises:
        ValueError: If the input cannot be parsed as a valid datetime.

    Example:
        >>> ab_datetime_parse("2023-03-14T15:09:26+00:00")
        '2023-03-14T15:09:26+00:00'
        >>> ab_datetime_parse(1678806000)  # Unix timestamp
        '2023-03-14T15:00:00+00:00'
        >>> ab_datetime_parse("2023-03-14")  # Date-only
        '2023-03-14T00:00:00+00:00'
    """
    try:
        # Handle numeric values as Unix timestamps (UTC)
        if isinstance(dt_str, int) or (
            isinstance(dt_str, str)
            and (dt_str.isdigit() or (dt_str.startswith("-") and dt_str[1:].isdigit()))
        ):
            timestamp = int(dt_str)
            if timestamp < 0:
                raise ValueError("Timestamp cannot be negative")
            if len(str(abs(timestamp))) > 10:
                raise ValueError("Timestamp value too large")
            instant = Instant.from_timestamp(timestamp)
            return AirbyteDateTime.from_datetime(instant.py_datetime())

        if not isinstance(dt_str, str):
            raise ValueError(
                f"Could not parse datetime string: expected string or integer, got {type(dt_str)}"
            )

        # Handle date-only format first
        if ":" not in dt_str and dt_str.count("-") == 2 and "/" not in dt_str:
            try:
                year, month, day = map(int, dt_str.split("-"))
                if not (1 <= month <= 12 and 1 <= day <= 31):
                    raise ValueError(f"Invalid date format: {dt_str}")
                instant = Instant.from_utc(year, month, day, 0, 0, 0)
                return AirbyteDateTime.from_datetime(instant.py_datetime())
            except (ValueError, TypeError):
                raise ValueError(f"Invalid date format: {dt_str}")

        # Validate datetime format
        if "/" in dt_str or " " in dt_str or "GMT" in dt_str:
            raise ValueError(f"Could not parse datetime string: {dt_str}")

        # Try parsing with dateutil for timezone handling
        try:
            parsed = parser.parse(dt_str)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return AirbyteDateTime.from_datetime(parsed)
        except (ValueError, TypeError):
            raise ValueError(f"Could not parse datetime string: {dt_str}")
    except ValueError as e:
        if "Invalid date format:" in str(e):
            raise
        if "Timestamp cannot be negative" in str(e):
            raise
        if "Timestamp value too large" in str(e):
            raise
        raise ValueError(f"Could not parse datetime string: {dt_str}")


def ab_datetime_format(dt: Union[datetime, AirbyteDateTime]) -> str:
    """Formats a datetime object as an ISO8601/RFC3339 string with 'T' delimiter and timezone.

    Previously named: format()

    Converts any datetime object to a string with 'T' delimiter and proper timezone.
    If the datetime is naive (no timezone), UTC is assumed.
    Uses '+00:00' for UTC timezone, otherwise keeps the original timezone offset.

    Args:
        dt: Any datetime object to format.

    Returns:
        str: ISO8601/RFC3339 formatted datetime string.

    Example:
        >>> dt = datetime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
        >>> ab_datetime_format(dt)
        '2023-03-14T15:09:26+00:00'
    """
    if isinstance(dt, AirbyteDateTime):
        return str(dt)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    # Format with consistent timezone representation
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt.microsecond:
        base = f"{base}.{dt.microsecond:06d}"
    offset = dt.strftime("%z")
    return f"{base}{offset[:3]}:{offset[3:]}"


def ab_datetime_try_parse(dt_str: str) -> AirbyteDateTime | None:
    """Try to parse the input string as an ISO8601/RFC3339 datetime, failing gracefully instead of raising an exception.

    Requires strict ISO8601/RFC3339 format with:
    - 'T' delimiter between date and time components
    - Valid timezone (Z for UTC or ±HH:MM offset)
    - Complete datetime representation (date and time)

    Returns None for any non-compliant formats including:
    - Space-delimited datetimes
    - Date-only strings
    - Missing timezone
    - Invalid timezone format
    - Wrong date/time separators

    Example:
        >>> ab_datetime_try_parse("2023-03-14T15:09:26Z")  # Returns AirbyteDateTime
        >>> ab_datetime_try_parse("2023-03-14 15:09:26Z")  # Returns None (invalid format)
        >>> ab_datetime_try_parse("2023-03-14")  # Returns None (missing time and timezone)
    """
    if not isinstance(dt_str, str):
        return None
    try:
        # Validate format before parsing
        if "T" not in dt_str:
            return None
        if not any(x in dt_str for x in ["Z", "+", "-"]):
            return None
        if "/" in dt_str or " " in dt_str or "GMT" in dt_str:
            return None

        # Try parsing with dateutil
        parsed = parser.parse(dt_str)
        if parsed.tzinfo is None:
            return None

        # Validate time components
        if not (0 <= parsed.hour <= 23 and 0 <= parsed.minute <= 59 and 0 <= parsed.second <= 59):
            return None

        return AirbyteDateTime.from_datetime(parsed)
    except (ValueError, TypeError):
        return None
