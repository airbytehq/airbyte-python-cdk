#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import re
from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union

import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.header_helper import (
    get_numeric_value_from_header,
)
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.max_waiting_time_helper import (
    evaluate_max_waiting_time,
    interpolated_max_waiting_time,
)
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategy import (
    BackoffStrategy,
)
from airbyte_cdk.sources.types import Config
from airbyte_cdk.utils import AirbyteTracedException


@dataclass
class WaitTimeFromHeaderBackoffStrategy(BackoffStrategy):
    """
    Extract wait time from http header

    Attributes:
        header (str): header to read wait time from
        regex (Optional[str]): optional regex to apply on the header to extract its value
        max_waiting_time_in_seconds (Optional[Union[float, InterpolatedString, str]]): stop the stream
            rather than wait longer than this
    """

    header: Union[InterpolatedString, str]
    parameters: InitVar[Mapping[str, Any]]
    config: Config
    regex: Optional[Union[InterpolatedString, str]] = None
    max_waiting_time_in_seconds: Optional[Union[float, InterpolatedString, str]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.regex = (
            InterpolatedString.create(self.regex, parameters=parameters) if self.regex else None
        )
        self.header = InterpolatedString.create(self.header, parameters=parameters)
        self._max_waiting_time_in_seconds = interpolated_max_waiting_time(
            self.max_waiting_time_in_seconds, parameters
        )

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        header = self.header.eval(config=self.config)  # type: ignore  # header is always cast to an interpolated stream
        if self.regex:
            evaled_regex = self.regex.eval(self.config)  # type: ignore # header is always cast to an interpolated string
            regex = re.compile(evaled_regex)
        else:
            regex = None
        header_value = None
        if isinstance(response_or_exception, requests.Response):
            header_value = get_numeric_value_from_header(response_or_exception, header, regex)
            max_waiting_time = evaluate_max_waiting_time(
                self._max_waiting_time_in_seconds, self.config
            )
            # `max_waiting_time is not None` rather than a truthiness check, so that 0 means
            # "never wait" instead of silently disabling the cap. The comparison stays `>=`,
            # which is what this cap has always done; `WaitUntilTimeFromHeader` stops at `>`,
            # so a wait exactly equal to the cap is allowed there and refused here.
            # `header_value` is checked for truthiness rather than `is not None` on purpose: a
            # header of `0` asks for no wait at all, which no cap -- not even 0 -- should refuse.
            if max_waiting_time is not None and header_value and header_value >= max_waiting_time:
                raise AirbyteTracedException(
                    internal_message=(
                        f"Rate limit wait time {header_value}s is greater than or equal to the "
                        f"maximum of {max_waiting_time}s this stream is allowed to wait. "
                        f"Stopping the stream..."
                    ),
                    message="The rate limit wait time is longer than the connector is allowed to wait.",
                    failure_type=FailureType.transient_error,
                )
        return header_value
