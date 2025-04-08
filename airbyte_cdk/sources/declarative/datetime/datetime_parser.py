#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import datetime
from typing import Union

from airbyte_cdk.utils.datetime_helpers import ab_datetime_format, ab_datetime_parse


class DatetimeParser:
    """
    Parses and formats datetime objects according to a specified format.

    This class mainly acts as a wrapper to properly handling timestamp formatting through the "%s" directive.

    %s is part of the list of format codes required by  the 1989 C standard, but it is unreliable because it always return a datetime in the system's timezone.
    Instead of using the directive directly, we can use datetime.fromtimestamp and dt.timestamp()
    """

    _UNIX_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

    def parse(self, date: Union[str, int], format: str) -> datetime.datetime:
        """
        Parse a datetime string according to a specified format.

        Args:
            date: String to parse
            format: Format string as described in https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
                with the following extensions:
                - %s: Unix timestamp
                - %s_as_float: Unix timestamp as float
                - %epoch_microseconds: Microseconds since epoch
                - %ms: Milliseconds since epoch
                - %_ms: Custom millisecond format

        Returns:
            The parsed datetime
        """
        # Special format handling
        if format == "%s":
            return datetime.datetime.fromtimestamp(int(date), tz=datetime.timezone.utc)
        elif format == "%s_as_float":
            return datetime.datetime.fromtimestamp(float(date), tz=datetime.timezone.utc)
        elif format == "%epoch_microseconds":
            epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
            return epoch + datetime.timedelta(microseconds=int(date))
        elif format == "%ms":
            epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
            return epoch + datetime.timedelta(milliseconds=int(date))
        elif "%_ms" in format:
            # Convert custom millisecond format to standard format
            format = format.replace("%_ms", "%f")
            
        # For standard formats, use ab_datetime_parse with the specific format
        try:
            result = ab_datetime_parse(date, formats=[format], disallow_other_formats=True)
            return result.to_datetime()  # Convert AirbyteDateTime to standard datetime
        except ValueError:
            # Fallback to original implementation for backward compatibility
            parsed_datetime = datetime.datetime.strptime(str(date), format)
            if self._is_naive(parsed_datetime):
                return parsed_datetime.replace(tzinfo=datetime.timezone.utc)
            return parsed_datetime.replace(tzinfo=datetime.timezone.utc)
        return parsed_datetime.replace(tzinfo=datetime.timezone.utc)

    def format(self, dt: datetime.datetime, format: str) -> str:
        """
        Format a datetime object according to a specified format.

        Args:
            dt: The datetime object to format
            format: Format string as described in https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
                with the following extensions:
                - %s: Unix timestamp
                - %s_as_float: Unix timestamp as float
                - %epoch_microseconds: Microseconds since epoch
                - %ms: Milliseconds since epoch
                - %_ms: Custom millisecond format

        Returns:
            The formatted string
        """
        # Handle special formats
        if format == "%s":
            return str(int(dt.timestamp()))
        elif format == "%s_as_float":
            return str(float(dt.timestamp()))
        elif format == "%epoch_microseconds":
            return str(int(dt.timestamp() * 1_000_000))
        elif format == "%ms":
            return str(int(dt.timestamp() * 1000))
        elif "%_ms" in format:
            _format = format.replace("%_ms", "%f")
            milliseconds = int(dt.microsecond / 1000)
            return dt.strftime(_format).replace(dt.strftime("%f"), "%03d" % milliseconds)

        # For standard formats, use ab_datetime_format
        return ab_datetime_format(dt, format)

    def _is_naive(self, dt: datetime.datetime) -> bool:
        return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None
