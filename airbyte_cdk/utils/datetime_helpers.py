"""Provides consistent datetime handling across Airbyte with ISO8601/RFC3339 compliance.

Copyright (c) 2023 Airbyte, Inc., all rights reserved.

This module provides a custom datetime class (AirbyteDateTime) and helper functions that ensure
consistent datetime handling across Airbyte. All datetime strings are formatted according to
ISO8601/RFC3339 standards with 'T' delimiter and 'Z' for UTC timezone.

Key Features:
    - Timezone-aware datetime objects (defaults to UTC)
    - ISO8601/RFC3339 compliant string formatting
    - Consistent parsing of various datetime formats
    - Support for Unix timestamps
    - Operator overloading for datetime arithmetic

Example:
    >>> dt = ab_datetime_parse("2023-03-14T15:09:26Z")
    >>> dt + timedelta(hours=1)
    2023-03-14T16:09:26Z
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

        Returns:
            str: ISO8601/RFC3339 formatted string.
        """
        aware_self = self if self.tzinfo else self.replace(tzinfo=timezone.utc)
        iso = aware_self.isoformat()
        return iso.replace("+00:00", "Z") if aware_self.tzinfo == timezone.utc else iso

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


def add_seconds(
    dt: Union[datetime, AirbyteDateTime], seconds: Union[int, float]
) -> AirbyteDateTime:
    """Alias for ab_datetime_add_seconds() for backward compatibility."""
    return ab_datetime_add_seconds(dt, seconds)


def subtract_seconds(
    dt: Union[datetime, AirbyteDateTime], seconds: Union[int, float]
) -> AirbyteDateTime:
    """Alias for ab_datetime_subtract_seconds() for backward compatibility."""
    return ab_datetime_subtract_seconds(dt, seconds)


def is_valid_format(dt_str: str) -> bool:
    """Alias for ab_datetime_is_valid_format() for backward compatibility."""
    return ab_datetime_is_valid_format(dt_str)


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


def ab_datetime_add_seconds(
    dt: Union[datetime, AirbyteDateTime], seconds: Union[int, float]
) -> AirbyteDateTime:
    """Adds seconds to a datetime object. DEPRECATED: Use AirbyteDateTime + timedelta(seconds=N) instead.

    Previously named: add_seconds()

    This function is deprecated in favor of using the + operator with timedelta:
        dt + timedelta(seconds=N)

    Args:
        dt: The datetime to add seconds to.
        seconds: Number of seconds to add.

    Returns:
        AirbyteDateTime: A new datetime with seconds added.

    Example:
        >>> # Instead of using this function:
        >>> ab_datetime_add_seconds(dt, 3600)  # Add 1 hour
        >>> # Use this:
        >>> dt + timedelta(seconds=3600)
    """
    import warnings

    warnings.warn(
        "ab_datetime_add_seconds is deprecated; use AirbyteDateTime + timedelta(seconds=N) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    if not isinstance(dt, AirbyteDateTime):
        dt = AirbyteDateTime.from_datetime(dt)
    return AirbyteDateTime.from_datetime(dt + timedelta(seconds=seconds))


def ab_datetime_subtract_seconds(
    dt: Union[datetime, AirbyteDateTime], seconds: Union[int, float]
) -> AirbyteDateTime:
    """Subtracts seconds from a datetime object. DEPRECATED: Use AirbyteDateTime - timedelta(seconds=N) instead.

    Previously named: subtract_seconds()

    This function is deprecated in favor of using the - operator with timedelta:
        dt - timedelta(seconds=N)

    Args:
        dt: The datetime to subtract seconds from.
        seconds: Number of seconds to subtract.

    Returns:
        AirbyteDateTime: A new datetime with seconds subtracted.

    Example:
        >>> # Instead of using this function:
        >>> ab_datetime_subtract_seconds(dt, 3600)  # Subtract 1 hour
        >>> # Use this:
        >>> dt - timedelta(seconds=3600)
    """
    import warnings

    warnings.warn(
        "ab_datetime_subtract_seconds is deprecated; use AirbyteDateTime - timedelta(seconds=N) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return ab_datetime_add_seconds(dt, -seconds)


def ab_datetime_is_valid_format(dt_str: str) -> bool:
    """Validates if a string matches ISO8601/RFC3339 datetime format with 'T' delimiter and timezone.

    Previously named: is_valid_format()

    Checks if the string follows strict ISO8601/RFC3339 format requirements:
        - Uses 'T' as date/time delimiter
        - Includes timezone specification (Z, +HH:MM, or -HH:MM)

    Args:
        dt_str: The datetime string to validate.

    Returns:
        bool: True if the string matches ISO8601/RFC3339 format, False otherwise.

    Example:
        >>> ab_datetime_is_valid_format("2023-03-14T15:09:26Z")
        True
        >>> ab_datetime_is_valid_format("2023-03-14 15:09:26")  # Missing T and timezone
        False
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
