#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import logging
import math
from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union, cast

import requests
from jinja2.exceptions import UndefinedError

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.max_waiting_time_helper import (
    evaluate_max_waiting_time,
    interpolated_max_waiting_time,
)
from airbyte_cdk.sources.streams.http.error_handlers import BackoffStrategy
from airbyte_cdk.sources.types import Config
from airbyte_cdk.utils import AirbyteTracedException

LOGGER = logging.getLogger("airbyte")


@dataclass
class WaitTimeFromResponseBackoffStrategy(BackoffStrategy):
    """
    Extract wait time from the response body or headers.

    Attributes:
        wait_time_in_seconds: Expression defining how long to wait before retrying. An empty,
            negative, non-finite or non-numeric result, or an expression that cannot be evaluated
            on the response body, defers to the next backoff strategy.
        max_waiting_time_in_seconds: Optional inclusive cap on the computed wait.
    """

    wait_time_in_seconds: Union[InterpolatedString, str]
    parameters: InitVar[Mapping[str, Any]]
    config: Config
    max_waiting_time_in_seconds: Optional[Union[float, InterpolatedString, str]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.wait_time_in_seconds = InterpolatedString.create(
            self.wait_time_in_seconds, parameters=parameters
        )
        self._max_waiting_time_in_seconds = interpolated_max_waiting_time(
            self.max_waiting_time_in_seconds, parameters
        )
        evaluate_max_waiting_time(self._max_waiting_time_in_seconds, self.config)

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        response: Any = {}
        headers: Mapping[str, Any] = {}
        if isinstance(response_or_exception, requests.Response):
            try:
                response = response_or_exception.json()
            except requests.exceptions.JSONDecodeError:
                # Non-JSON body: interpolate with an empty response.
                pass
            headers = response_or_exception.headers

        try:
            evaluated_wait_time = cast(InterpolatedString, self.wait_time_in_seconds).eval(
                self.config, response=response, headers=headers
            )
        except (UndefinedError, TypeError, ValueError) as error:
            # The strategy runs on every retryable response, not only the one the expression was
            # written for: a body of another shape (a list, or a string where an object was
            # expected) must defer to the next strategy, not fail the stream. The body is not
            # logged; the error type is enough to point at the expression.
            LOGGER.warning(
                "WaitTimeFromResponse: `wait_time_in_seconds` could not be evaluated on this "
                f"response ({type(error).__name__}); deferring to the next backoff strategy."
            )
            return None
        if evaluated_wait_time is None or (
            isinstance(evaluated_wait_time, str) and not evaluated_wait_time.strip()
        ):
            return None

        try:
            wait_time = float(evaluated_wait_time)
        except (TypeError, ValueError):
            # Mirrors the header strategies, which return None for a value they cannot read, so
            # the next backoff strategy decides. The value is not logged: it comes from the
            # response body and may hold anything.
            LOGGER.warning(
                "WaitTimeFromResponse: `wait_time_in_seconds` resolved to a non-numeric "
                f"{type(evaluated_wait_time).__name__}; deferring to the next backoff strategy."
            )
            return None
        if not math.isfinite(wait_time) or wait_time < 0:
            # A retry time that has already passed gives a negative wait, which is not a wait to
            # take: the next strategy decides, as `WaitUntilTimeFromHeader` does for a past
            # timestamp. Returned as-is, a negative value would reach `time.sleep` and fail the
            # stream. A zero wait is returned as 0, like `WaitTimeFromHeader` for a `0` header.
            return None
        max_waiting_time = evaluate_max_waiting_time(self._max_waiting_time_in_seconds, self.config)
        if max_waiting_time is not None and wait_time and wait_time >= max_waiting_time:
            raise AirbyteTracedException(
                internal_message=(
                    f"Rate limit wait time {wait_time}s is greater than or equal to the "
                    f"maximum of {max_waiting_time}s this stream is allowed to wait. "
                    f"Stopping the stream..."
                ),
                message="The rate limit wait time is longer than the connector is allowed to wait.",
                failure_type=FailureType.transient_error,
            )

        return wait_time
