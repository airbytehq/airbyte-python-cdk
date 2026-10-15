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
        backoff_time = float(
            cast(InterpolatedString, self.backoff_time_in_seconds).eval(self.config)
        )
        if self.jitter_range_in_seconds is None:
            return backoff_time

        return random.uniform(backoff_time, backoff_time + (self.jitter_range_in_seconds * 2))
