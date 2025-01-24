"""Provides consistent datetime handling across Airbyte with ISO8601/RFC3339 compliance.

Copyright (c) 2023 Airbyte, Inc., all rights reserved.

This module provides a custom datetime class (AirbyteDateTime) and helper functions that ensure
consistent datetime handling across Airbyte. All datetime strings are formatted according to
ISO8601/RFC3339 standards with 'T' delimiter and 'Z' for UTC timezone.

Key Features:
    - Timezone-aware datetime objects (defaults to UTC)
    - ISO8601/RFC3339 compliant string formatting
    - Consistent parsing of various datetime formats
    - Support for Unix timestamps and milliseconds
    - Type-safe datetime arithmetic with timedelta

# Basic Usage

```python
from airbyte_cdk.utils.datetime_helpers import AirbyteDateTime, ab_datetime_now, ab_datetime_parse
from datetime import timedelta, timezone

# Current time in UTC
now = ab_datetime_now()
print(now)  # 2023-03-14T15:09:26.535897Z

# Parse various datetime formats
dt = ab_datetime_parse("2023-03-14T15:09:26Z")  # ISO8601/RFC3339
dt = ab_datetime_parse("2023-03-14")  # Date only (assumes midnight UTC)
dt = ab_datetime_parse(1678806566)  # Unix timestamp

# Create with explicit timezone
dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
print(dt)  # 2023-03-14T15:09:26Z

# Datetime arithmetic with timedelta
tomorrow = dt + timedelta(days=1)
yesterday = dt - timedelta(days=1)
time_diff = tomorrow - yesterday  # timedelta object
```

# Millisecond Timestamp Handling

```python
# Convert to millisecond timestamp
dt = ab_datetime_parse("2023-03-14T15:09:26Z")
ms = dt.to_epoch_millis()  # 1678806566000

# Create from millisecond timestamp
dt = AirbyteDateTime.from_epoch_millis(1678806566000)
print(dt)  # 2023-03-14T15:09:26Z
```

# Timezone Handling

```python
# Create with non-UTC timezone
tz = timezone(timedelta(hours=-4))  # EDT
dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=tz)
print(dt)  # 2023-03-14T15:09:26-04:00

# Parse with timezone
dt = ab_datetime_parse("2023-03-14T15:09:26-04:00")
print(dt)  # 2023-03-14T15:09:26-04:00

# Naive datetimes are automatically converted to UTC
dt = ab_datetime_parse("2023-03-14T15:09:26")
print(dt)  # 2023-03-14T15:09:26Z
```

# Format Validation

```python
from airbyte_cdk.utils.datetime_helpers import ab_datetime_try_parse

# Validate ISO8601/RFC3339 format
assert ab_datetime_try_parse("2023-03-14T15:09:26Z")       # The parsed datetime is truthy
assert ab_datetime_try_parse("2023-03-14T15:09:26-04:00")  # The parsed datetime is truthy
assert ab_datetime_try_parse("2023-03-14 15:09:26Z")       # The parsed datetime is truthy
assert !ab_datetime_try_parse("foo")                # 'foo' can't be parsed, returns `None`
```
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union, overload

from dateutil import parser
from typing_extensions import Never


class AirbyteDateTime(datetime):
    """A timezone-aware datetime class with ISO8601/RFC3339 string representation and operator overloading.

    This class extends the standard datetime class to provide consistent timezone handling
    (defaulting to UTC) and ISO8601/RFC3339 compliant string formatting. It also supports
    operator overloading for datetime arithmetic with timedelta objects.

    Example:
        >>> dt = AirbyteDateTime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
        >>> str(dt)
        '2023-03-14T15:09:26Z'
        >>> dt + timedelta(hours=1)
        '2023-03-14T16:09:26Z'
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

        Ensures consistent string representation with timezone, using 'Z' for UTC.
        Preserves full microsecond precision when present, omits when zero.

        Returns:
            str: ISO8601/RFC3339 formatted string.
        """
        aware_self = self if self.tzinfo else self.replace(tzinfo=timezone.utc)
        base = self.strftime("%Y-%m-%dT%H:%M:%S")
        if self.microsecond:
            base = f"{base}.{self.microsecond:06d}"
        if aware_self.tzinfo == timezone.utc:
            return f"{base}Z"
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
            '2023-03-14T15:09:26Z'
        """
        return cls.fromtimestamp(milliseconds / 1000.0, timezone.utc)


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


# Backward compatibility aliases
def now() -> AirbyteDateTime:
    """Alias for ab_datetime_now() for backward compatibility."""
    return ab_datetime_now()


def parse(dt_str: Union[str, int]) -> AirbyteDateTime:
    """Alias for ab_datetime_parse() for backward compatibility."""
    return ab_datetime_parse(dt_str)


def format(dt: Union[datetime, AirbyteDateTime]) -> str:
    """Alias for ab_datetime_format() for backward compatibility."""
    return ab_datetime_format(dt)



def ab_datetime_parse(dt_str: Union[str, int]) -> AirbyteDateTime:
    """Parses a datetime string or timestamp into an AirbyteDateTime with timezone awareness.

    Previously named: parse()

    Handles:
        - ISO8601/RFC3339 format strings
        - Unix timestamps (as integers or strings)
        - Date-only strings (YYYY-MM-DD)
        - Falls back to dateutil.parser for other string formats

    Always returns a timezone-aware datetime (defaults to UTC if no timezone specified).

    Args:
        dt_str: A datetime string in ISO8601/RFC3339 format, Unix timestamp (int/str),
            or other recognizable datetime format.

    Returns:
        AirbyteDateTime: A timezone-aware datetime object.

    Raises:
        ValueError: If the input cannot be parsed as a valid datetime.

    Example:
        >>> ab_datetime_parse("2023-03-14T15:09:26Z")
        '2023-03-14T15:09:26Z'
        >>> ab_datetime_parse(1678806000)  # Unix timestamp
        '2023-03-14T15:00:00Z'
        >>> ab_datetime_parse("2023-03-14")  # Date-only
        '2023-03-14T00:00:00Z'
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


def ab_datetime_format(dt: Union[datetime, AirbyteDateTime]) -> str:
    """Formats a datetime object as an ISO8601/RFC3339 string with 'T' delimiter and timezone.

    Previously named: format()

    Converts any datetime object to a string with 'T' delimiter and proper timezone.
    If the datetime is naive (no timezone), UTC is assumed.
    Uses 'Z' for UTC timezone, otherwise keeps the original timezone offset.

    Args:
        dt: Any datetime object to format.

    Returns:
        str: ISO8601/RFC3339 formatted datetime string.

    Example:
        >>> dt = datetime(2023, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
        >>> ab_datetime_format(dt)
        '2023-03-14T15:09:26Z'
    """
    if isinstance(dt, AirbyteDateTime):
        return str(dt)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    iso = dt.isoformat()
    return iso.replace("+00:00", "Z") if dt.tzinfo == timezone.utc else iso


def ab_datetime_try_parse(dt_str: str) -> AirbyteDateTime | None:
    """Try to parse the input string, failing gracefully instead of raising an exception.

    If not parseable, return `None`. Otherwise, return the `AirbyteDataTime` object.
    """
    try:
        return ab_datetime_parse(dt_str)
    except:
        return None
