#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import numbers
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from re import Pattern

    import requests


def get_numeric_value_from_header(
    response: requests.Response, header: str, regex: Pattern[str] | None
) -> float | None:
    """Extract a header value from the response as a float
    :param response: response the extract header value from
    :param header: Header to extract
    :param regex: optional regex to apply on the header to obtain the value
    :return: header value as float if it's a number. None otherwise
    """
    header_value = response.headers.get(header, None)
    if not header_value:
        return None
    if isinstance(header_value, str):
        if regex:
            match = regex.match(header_value)
            if match:
                header_value = match.group()
        return _as_float(header_value)
    if isinstance(header_value, numbers.Number):
        return float(header_value)  # type: ignore[arg-type]
    return None


def _as_float(s: str) -> float | None:
    try:
        return float(s)
    except ValueError:
        return None
