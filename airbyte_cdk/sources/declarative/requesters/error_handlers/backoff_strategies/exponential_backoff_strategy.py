#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import random
from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union

import requests

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.streams.http.error_handlers import BackoffStrategy
from airbyte_cdk.sources.types import Config


@dataclass
class ExponentialBackoffStrategy(BackoffStrategy):
    """
    Backoff strategy with an exponential backoff interval

    Attributes:
        factor (float): multiplicative factor
    """

    parameters: InitVar[Mapping[str, Any]]
    config: Config
    factor: Union[float, InterpolatedString, str] = 5
    jitter_range_in_seconds: Optional[Union[float, InterpolatedString, str]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._factor = self._as_interpolated_string(self.factor, parameters)
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

    @property
    def _retry_factor(self) -> float:
        return self._factor.eval(self.config)  # type: ignore # factor is always cast to an interpolated string

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        backoff_time = float(self._retry_factor * 2**attempt_count)
        if self.jitter_range_in_seconds is None:
            return backoff_time

        jitter_range = float(self.jitter_range_in_seconds.eval(self.config))
        if jitter_range < 0:
            raise ValueError("jitter_range_in_seconds must be greater than or equal to 0")

        return random.uniform(max(0, backoff_time - jitter_range), backoff_time + jitter_range)
