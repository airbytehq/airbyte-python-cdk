#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections.abc import Mapping
from dataclasses import InitVar, dataclass
from typing import Any

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
    factor: float | InterpolatedString | str = 5

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not isinstance(self.factor, InterpolatedString):
            self.factor = str(self.factor)
        if isinstance(self.factor, float):
            self._factor = InterpolatedString.create(str(self.factor), parameters=parameters)
        else:
            self._factor = InterpolatedString.create(self.factor, parameters=parameters)

    @property
    def _retry_factor(self) -> float:
        return self._factor.eval(self.config)  # type: ignore # factor is always cast to an interpolated string

    def backoff_time(
        self,
        response_or_exception: requests.Response | requests.RequestException | None,
        attempt_count: int,
    ) -> float | None:
        return self._retry_factor * 2**attempt_count  # type: ignore # factor is always cast to an interpolated string
