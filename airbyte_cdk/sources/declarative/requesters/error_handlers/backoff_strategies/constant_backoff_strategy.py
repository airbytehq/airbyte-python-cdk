#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
import random
from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union, cast

import requests
from jinja2.exceptions import UndefinedError

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.streams.http.error_handlers import BackoffStrategy
from airbyte_cdk.sources.types import Config

logger = logging.getLogger("airbyte")


@dataclass
class ConstantBackoffStrategy(BackoffStrategy):
    """
    Backoff strategy with a constant backoff interval.

    The ``backoff_time_in_seconds`` field is evaluated on every call with the
    following interpolation context, in addition to ``config``:

    * ``response``: the parsed JSON body of the response (``{}`` when the body is
      not JSON or when the call was made with a request exception or ``None``).
    * ``headers``: the response headers (``{}`` on a request exception).
    * ``attempt_count``: the number of the current retry attempt.

    If the expression cannot be evaluated against the response (for example an
    attribute access on a non-object JSON body), the rendered value is falsy
    (``None``, ``""``, ``0``), evaluates to ``0`` seconds, or cannot be
    converted to a number, this strategy returns ``None`` (no jitter is
    applied) so that the next entry in ``backoff_strategies`` or the default
    backoff is used.

    Attributes:
        backoff_time_in_seconds (float): time to backoff before retrying a retryable request.
    """

    backoff_time_in_seconds: Union[float, InterpolatedString, str]
    parameters: InitVar[Mapping[str, Any]]
    config: Config
    jitter_range_in_seconds: Optional[float] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not isinstance(self.backoff_time_in_seconds, InterpolatedString):
            self.backoff_time_in_seconds = str(self.backoff_time_in_seconds)
        if isinstance(self.backoff_time_in_seconds, float):
            self.backoff_time_in_seconds = InterpolatedString.create(
                str(self.backoff_time_in_seconds), parameters=parameters
            )
        else:
            self.backoff_time_in_seconds = InterpolatedString.create(
                self.backoff_time_in_seconds, parameters=parameters
            )

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        response: Any
        headers: Mapping[str, Any]
        if isinstance(response_or_exception, requests.Response):
            response = self._safe_response_json(response_or_exception)
            headers = response_or_exception.headers
        else:
            response = {}
            headers = {}

        try:
            rendered = cast(InterpolatedString, self.backoff_time_in_seconds).eval(
                self.config,
                response=response,
                headers=headers,
                attempt_count=attempt_count,
            )
        except UndefinedError:
            return None
        if not rendered:
            return None
        try:
            backoff_time = float(rendered)
        except (TypeError, ValueError):
            logger.warning(
                f"ConstantBackoffStrategy backoff_time_in_seconds rendered to a non-numeric value {rendered!r}; "
                "skipping this strategy so the next backoff strategy or the default backoff is used."
            )
            return None
        if backoff_time == 0:
            return None
        if self.jitter_range_in_seconds is None:
            return backoff_time

        return random.uniform(backoff_time, backoff_time + (self.jitter_range_in_seconds * 2))

    @staticmethod
    def _safe_response_json(response: requests.Response) -> Any:
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {}
