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
    jitter_range_in_seconds: Optional[float] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not isinstance(self.factor, InterpolatedString):
            self.factor = str(self.factor)
        if isinstance(self.factor, float):
            self._factor = InterpolatedString.create(str(self.factor), parameters=parameters)
        else:
            self._factor = InterpolatedString.create(self.factor, parameters=parameters)

    @property
    def _retry_factor(self) -> float:
        return float(self._factor.eval(self.config))

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        backoff_time = float(self._retry_factor * 2**attempt_count)
        if self.jitter_range_in_seconds is None:
            return backoff_time

        return random.uniform(backoff_time, backoff_time + (self.jitter_range_in_seconds * 2))
