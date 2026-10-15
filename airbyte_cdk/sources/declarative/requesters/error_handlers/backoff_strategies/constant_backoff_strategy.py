#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import random
from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union, cast

import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.max_waiting_time_helper import (
    evaluate_max_waiting_time,
    interpolated_max_waiting_time,
)
from airbyte_cdk.sources.streams.http.error_handlers import BackoffStrategy
from airbyte_cdk.sources.types import Config
from airbyte_cdk.utils import AirbyteTracedException


@dataclass
class ConstantBackoffStrategy(BackoffStrategy):
    """Compute a constant backoff from config, response JSON, and response headers.

    An empty interpolated result defers to the next strategy. The optional cap is evaluated from
    config and checked against the final backoff after jitter.

    Attributes:
        backoff_time_in_seconds: Backoff time in seconds.
        jitter_range_in_seconds: Optional additive jitter range that only increases the wait.
        max_waiting_time_in_seconds: Optional inclusive cap on the computed wait after jitter.
    """

    backoff_time_in_seconds: Union[float, InterpolatedString, str]
    parameters: InitVar[Mapping[str, Any]]
    config: Config
    jitter_range_in_seconds: Optional[float] = None
    max_waiting_time_in_seconds: Optional[Union[float, InterpolatedString, str]] = None

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

        evaluated_backoff_time = cast(InterpolatedString, self.backoff_time_in_seconds).eval(
            self.config, response=response, headers=headers
        )
        if evaluated_backoff_time is None or (
            isinstance(evaluated_backoff_time, str) and not evaluated_backoff_time.strip()
        ):
            return None

        backoff_time = float(evaluated_backoff_time)
        if self.jitter_range_in_seconds is not None:
            backoff_time = random.uniform(
                backoff_time, backoff_time + (self.jitter_range_in_seconds * 2)
            )

        max_waiting_time = evaluate_max_waiting_time(self._max_waiting_time_in_seconds, self.config)
        if max_waiting_time is not None and backoff_time and backoff_time >= max_waiting_time:
            raise AirbyteTracedException(
                internal_message=(
                    f"Rate limit wait time {backoff_time}s is greater than or equal to the "
                    f"maximum of {max_waiting_time}s this stream is allowed to wait. "
                    f"Stopping the stream..."
                ),
                message="The rate limit wait time is longer than the connector is allowed to wait.",
                failure_type=FailureType.transient_error,
            )

        return backoff_time
