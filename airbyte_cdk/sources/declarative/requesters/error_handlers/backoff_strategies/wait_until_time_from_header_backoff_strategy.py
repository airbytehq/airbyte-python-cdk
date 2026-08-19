#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import numbers
import re
import time
from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union

import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.header_helper import (
    get_numeric_value_from_header,
)
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategy import (
    BackoffStrategy,
)
from airbyte_cdk.sources.types import Config
from airbyte_cdk.utils import AirbyteTracedException


@dataclass
class WaitUntilTimeFromHeaderBackoffStrategy(BackoffStrategy):
    """
    Extract time at which we can retry the request from response header
    and wait for the difference between now and that time

    Attributes:
        header (str): header to read wait time from
        min_wait (Optional[float]): minimum time to wait for safety
        regex (Optional[str]): optional regex to apply on the header to extract its value
        max_waiting_time_in_seconds (Optional[float]): stop the stream rather than wait longer than this
    """

    header: Union[InterpolatedString, str]
    parameters: InitVar[Mapping[str, Any]]
    config: Config
    min_wait: Optional[Union[float, InterpolatedString, str]] = None
    regex: Optional[Union[InterpolatedString, str]] = None
    max_waiting_time_in_seconds: Optional[Union[float, InterpolatedString, str]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.header = InterpolatedString.create(self.header, parameters=parameters)
        self.regex = (
            InterpolatedString.create(self.regex, parameters=parameters) if self.regex else None
        )
        if not isinstance(self.min_wait, InterpolatedString):
            self.min_wait = InterpolatedString.create(str(self.min_wait), parameters=parameters)
        self._max_waiting_time_in_seconds = (
            self.max_waiting_time_in_seconds
            if self.max_waiting_time_in_seconds is None
            or isinstance(self.max_waiting_time_in_seconds, InterpolatedString)
            else InterpolatedString.create(
                str(self.max_waiting_time_in_seconds), parameters=parameters
            )
        )

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        now = time.time()
        header = self.header.eval(self.config)  # type: ignore # header is always cast to an interpolated string
        if self.regex:
            evaled_regex = self.regex.eval(self.config)  # type: ignore # header is always cast to an interpolated string
            regex = re.compile(evaled_regex)
        else:
            regex = None
        wait_until = None
        if isinstance(response_or_exception, requests.Response):
            wait_until = get_numeric_value_from_header(response_or_exception, header, regex)
        min_wait = self.min_wait.eval(self.config)  # type: ignore # header is always cast to an interpolated string
        if wait_until is None or not wait_until:
            return self._capped(float(min_wait)) if min_wait else None
        if (isinstance(wait_until, str) and wait_until.isnumeric()) or isinstance(
            wait_until, numbers.Number
        ):
            wait_time = float(wait_until) - now
        else:
            return self._capped(float(min_wait))
        if min_wait:
            return self._capped(float(max(wait_time, min_wait)))
        elif wait_time < 0:
            return None
        return self._capped(wait_time)

    def _capped(self, wait_time: float) -> float:
        """Raise rather than wait longer than `max_waiting_time_in_seconds`.

        The cap is compared against the wait this strategy is about to return, not against the
        raw header: unlike `Retry-After`, the header here is an absolute timestamp, so only the
        computed difference is a duration. It is also applied after the `min_wait` floor, so a
        cap below the floor wins -- a caller asking never to wait more than N seconds means it,
        even when the floor would otherwise round the wait up past N.
        """
        max_waiting_time = self._eval_max_waiting_time()
        if max_waiting_time is not None and wait_time > max_waiting_time:
            raise AirbyteTracedException(
                internal_message=(
                    f"Rate limit wait time {wait_time}s is greater than the maximum of "
                    f"{max_waiting_time}s this stream is allowed to wait. Stopping the stream..."
                ),
                message="The rate limit wait time is longer than the connector is allowed to wait.",
                failure_type=FailureType.transient_error,
            )
        return wait_time

    def _eval_max_waiting_time(self) -> Optional[float]:
        if self._max_waiting_time_in_seconds is None:
            return None
        evaluated = self._max_waiting_time_in_seconds.eval(self.config)
        # `is None` rather than a truthiness check: 0 is a meaningful cap -- "never wait" -- and
        # the equivalent field on WaitTimeFromHeader silently disables itself when set to 0.
        if evaluated is None or evaluated == "":
            return None
        return float(evaluated)
