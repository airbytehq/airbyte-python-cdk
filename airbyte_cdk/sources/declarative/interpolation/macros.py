#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import builtins
import datetime
import re
import typing
from typing import Optional, Union
from urllib.parse import quote_plus

import isodate
import pytz
from dateutil import parser
from isodate import parse_duration

from airbyte_cdk.sources.declarative.datetime.datetime_parser import DatetimeParser

"""
This file contains macros that can be evaluated by a `JinjaInterpolation` object
"""


def now_utc() -> datetime.datetime:
    """
    Current local date and time in UTC timezone

    Usage:
    `"{{ now_utc() }}"`
    """
    return datetime.datetime.now(datetime.timezone.utc)


def today_utc() -> datetime.date:
    """
    Current date in UTC timezone

    Usage:
    `"{{ today_utc() }}"`
    """
    return datetime.datetime.now(datetime.timezone.utc).date()


def today_with_timezone(timezone: str) -> datetime.date:
    """
    Current date in custom timezone

    :param timezone: timezone expressed as IANA keys format. Example: "Pacific/Tarawa"
    :return:
    """
    return datetime.datetime.now(tz=pytz.timezone(timezone)).date()


def timestamp(dt: Union[float, str]) -> Union[int, float]:
    """
    Converts a number or a string to a timestamp

    If dt is a number, then convert to an int
    If dt is a string, then parse it using dateutil.parser

    Usage:
    `"{{ timestamp(1658505815.223235) }}"

    :param dt: datetime to convert to timestamp
    :return: unix timestamp
    """
    if isinstance(dt, (int, float)):
        return int(dt)
    else:
        return str_to_datetime(dt).astimezone(pytz.utc).timestamp()


def str_to_datetime(s: str) -> datetime.datetime:
    """
    Converts a string to a datetime object with UTC timezone

    If the input string does not contain timezone information, UTC is assumed.
    Supports both basic date strings like "2022-01-14" and datetime strings with optional timezone
    like "2022-01-01T13:45:30+00:00".

    Usage:
    `"{{ str_to_datetime('2022-01-14') }}"`

    :param s: string to parse as datetime
    :return: datetime object in UTC timezone
    """

    parsed_date = parser.isoparse(s)
    if not parsed_date.tzinfo:
        # Assume UTC if the input does not contain a timezone
        parsed_date = parsed_date.replace(tzinfo=pytz.utc)
    return parsed_date.astimezone(pytz.utc)


def max(*args: typing.Any) -> typing.Any:
    """
    Returns biggest object of an iterable, or two or more arguments.

    max(iterable, *[, default=obj, key=func]) -> value
    max(arg1, arg2, *args, *[, key=func]) -> value

    Usage:
    `"{{ max(2,3) }}"

    With a single iterable argument, return its biggest item. The
    default keyword-only argument specifies an object to return if
    the provided iterable is empty.
    With two or more arguments, return the largest argument.
    :param args: args to compare
    :return: largest argument
    """
    return builtins.max(*args)


def min(*args: typing.Any) -> typing.Any:
    """
    Returns smallest object of an iterable, or two or more arguments.

    min(iterable, *[, default=obj, key=func]) -> value
    min(arg1, arg2, *args, *[, key=func]) -> value

    Usage:
    `"{{ min(2,3) }}"

    With a single iterable argument, return its smallest item. The
    default keyword-only argument specifies an object to return if
    the provided iterable is empty.
    With two or more arguments, return the smallest argument.
    :param args: args to compare
    :return: smallest argument
    """
    return builtins.min(*args)


def day_delta(num_days: int, format: str = "%Y-%m-%dT%H:%M:%S.%f%z") -> str:
    """
    Returns datetime of now() + num_days

    Usage:
    `"{{ day_delta(25) }}"`

    :param num_days: number of days to add to current date time
    :return: datetime formatted as RFC3339
    """
    return (
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=num_days)
    ).strftime(format)


def duration(datestring: str) -> Union[datetime.timedelta, isodate.Duration]:
    """
    Converts ISO8601 duration to datetime.timedelta

    Usage:
    `"{{ now_utc() - duration('P1D') }}"`
    """
    return parse_duration(datestring)


def format_datetime(
    dt: Union[str, datetime.datetime, int], format: str, input_format: Optional[str] = None
) -> str:
    """
    Converts datetime to another format

    Usage:
    `"{{ format_datetime(config.start_date, '%Y-%m-%d') }}"`

    CPython Datetime package has known bug with `stfrtime` method: '%s' formatting uses locale timezone
    https://github.com/python/cpython/issues/77169
    https://github.com/python/cpython/issues/56959
    """
    if isinstance(dt, datetime.datetime):
        return dt.strftime(format)

    if isinstance(dt, int):
        dt_datetime = DatetimeParser().parse(dt, input_format if input_format else "%s")
    else:
        dt_datetime = (
            datetime.datetime.strptime(dt, input_format) if input_format else str_to_datetime(dt)
        )
    if dt_datetime.tzinfo is None:
        dt_datetime = dt_datetime.replace(tzinfo=pytz.utc)
    return DatetimeParser().format(dt=dt_datetime, format=format)


def sanitize_url(value: str) -> str:
    """
    Sanitizes a value by via urllib.parse.quote_plus

    Usage:
    `"{{ sanitize_url('https://example.com/path?query=value') }}"`
    """
    sanitization_strategy = quote_plus
    return sanitization_strategy(value)


def camel_case_to_snake_case(value: str) -> str:
    """
     Converts CamelCase strings to snake_case format

     Usage:
    `"{{ camel_case_to_snake_case('CamelCase') }}"`
     :param value: string to convert from CamelCase to snake_case
     :return: snake_case formatted string
    """
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


_macros_list = [
    now_utc,
    today_utc,
    timestamp,
    max,
    min,
    day_delta,
    duration,
    format_datetime,
    today_with_timezone,
    str_to_datetime,
    sanitize_url,
    camel_case_to_snake_case,
]
macros = {f.__name__: f for f in _macros_list}
