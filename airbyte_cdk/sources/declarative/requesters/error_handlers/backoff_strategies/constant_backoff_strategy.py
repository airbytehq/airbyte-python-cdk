#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import random
from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union, cast

import requests

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.streams.http.error_handlers import BackoffStrategy
from airbyte_cdk.sources.types import Config


@dataclass
class ConstantBackoffStrategy(BackoffStrategy):
    """
    Backoff strategy with a constant backoff interval

    Attributes:
        backoff_time_in_seconds (float): time to backoff before retrying a retryable request.
    """

    backoff_time_in_seconds: Union[float, InterpolatedString, str]
    parameters: InitVar[Mapping[str, Any]]
    config: Config
    jitter_range_in_seconds: Optional[Union[float, InterpolatedString, str]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.backoff_time_in_seconds = self._as_interpolated_string(
            self.backoff_time_in_seconds, parameters
        )
        if self.jitter_range_in_seconds is not None:
            self.jitter_range_in_seconds = self._as_interpolated_string(
                self.jitter_range_in_seconds, parameters
            )

    @staticmethod
    def _as_interpolated_string(
        value: Union[float, InterpolatedString, str], parameters: Mapping[str, Any]
    ) -> InterpolatedString:
        if not isinstance(value, InterpolatedString):
            value = str(value)
        return InterpolatedString.create(value, parameters=parameters)

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        backoff_time = float(
            cast(InterpolatedString, self.backoff_time_in_seconds).eval(self.config)
        )
        jitter_range_in_seconds = self.jitter_range_in_seconds
        if jitter_range_in_seconds is None:
            return backoff_time

        jitter_range = float(cast(InterpolatedString, jitter_range_in_seconds).eval(self.config))
        if jitter_range < 0:
            raise ValueError("jitter_range_in_seconds must be greater than or equal to 0")

        return random.uniform(max(0, backoff_time - jitter_range), backoff_time + jitter_range)
