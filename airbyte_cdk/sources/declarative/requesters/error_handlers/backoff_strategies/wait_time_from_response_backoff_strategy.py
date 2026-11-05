#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import logging
import math
from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union

import requests
from jinja2.exceptions import UndefinedError

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.interpolation.jinja import JinjaInterpolation
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.max_waiting_time_helper import (
    evaluate_max_waiting_time,
    interpolated_max_waiting_time,
)
from airbyte_cdk.sources.streams.http.error_handlers import BackoffStrategy
from airbyte_cdk.sources.types import Config
from airbyte_cdk.utils import AirbyteTracedException

LOGGER = logging.getLogger("airbyte")

_INTERPOLATION_CONTEXT = frozenset({"config", "response", "headers", "parameters"})


@dataclass
class WaitTimeFromResponseBackoffStrategy(BackoffStrategy):
    """
    Extract wait time, in relative seconds, from the JSON response body or the response headers.

    Attributes:
        wait_time_in_seconds: Expression defining how long to wait before retrying. An empty,
            zero, negative, non-finite or non-numeric result, or an expression that cannot be
            evaluated on the response body, defers to the next backoff strategy.
        max_waiting_time_in_seconds: Optional inclusive cap on the computed wait.
    """

    wait_time_in_seconds: Union[InterpolatedString, str]
    parameters: InitVar[Mapping[str, Any]]
    config: Config
    max_waiting_time_in_seconds: Optional[Union[float, InterpolatedString, str]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        # Evaluated with `JinjaInterpolation` directly rather than `InterpolatedString`: the latter
        # caches a template as plain text when its first evaluation returns the template unchanged,
        # which is what `JinjaInterpolation` does when rendering raises `TypeError`. One response
        # body of an unexpected shape would then turn the strategy off for the rest of the sync.
        self._wait_time_template = (
            self.wait_time_in_seconds.string
            if isinstance(self.wait_time_in_seconds, InterpolatedString)
            else self.wait_time_in_seconds
        )
        self._interpolation = JinjaInterpolation()
        self._parameters = parameters
        undeclared = (
            JinjaInterpolation._find_undeclared_variables(self._wait_time_template)
            - _INTERPOLATION_CONTEXT
        )
        if undeclared:
            # A typo such as `resposne` would otherwise fail on every response and leave the
            # strategy silently inert; it is a manifest bug, so it fails at startup.
            raise AirbyteTracedException(
                internal_message=(
                    f"WaitTimeFromResponse `wait_time_in_seconds` {self._wait_time_template!r} "
                    f"uses undeclared variables {sorted(undeclared)}; only "
                    f"{sorted(_INTERPOLATION_CONTEXT)} are available."
                ),
                message=(
                    "The connector's rate limit wait time expression is invalid. This is a "
                    "problem with the connector rather than with your configuration."
                ),
                failure_type=FailureType.system_error,
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
            except (ValueError, requests.RequestException, RecursionError):
                # Unreadable body (not JSON, too deeply nested, or a broken stream): interpolate
                # with an empty response so an expression reading only `headers` still works.
                pass
            headers = response_or_exception.headers

        try:
            evaluated_wait_time = self._interpolation.eval(
                self._wait_time_template,
                self.config,
                # The template as its own default re-raises an `UndefinedError` from the first
                # render instead of returning None, so it is reported below.
                self._wait_time_template,
                parameters=self._parameters,
                response=response,
                headers=headers,
            )
            if evaluated_wait_time == self._wait_time_template:
                # `JinjaInterpolation._eval` returns the raw template when rendering raises
                # `TypeError`, e.g. arithmetic on a string from the body.
                raise TypeError("the expression could not be rendered")
        except (UndefinedError, TypeError, ValueError, ArithmeticError) as error:
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
        except (TypeError, ValueError, ArithmeticError):
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
            # stream. A zero wait is returned as 0, but `DefaultErrorHandler` treats it as no
            # answer and moves on to the next strategy, as it does for `WaitTimeFromHeader`.
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
