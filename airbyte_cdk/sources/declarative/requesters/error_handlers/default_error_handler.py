#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, List, Mapping, MutableMapping, Optional, Union

import requests

from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.error_handlers.default_http_response_filter import (
    DefaultHttpResponseFilter,
)
from airbyte_cdk.sources.declarative.requesters.error_handlers.http_response_filter import (
    HttpResponseFilter,
)
from airbyte_cdk.sources.streams.http.error_handlers import BackoffStrategy, ErrorHandler
from airbyte_cdk.sources.streams.http.error_handlers.response_models import (
    SUCCESS_RESOLUTION,
    ErrorResolution,
    create_fallback_error_resolution,
)
from airbyte_cdk.sources.types import Config


@dataclass
class DefaultErrorHandler(ErrorHandler):
    """
    Default error handler.

    By default, the handler will only use the `DEFAULT_ERROR_MAPPING` that is part of the Python CDK's `HttpStatusErrorHandler`.

    If the response is successful, then a SUCCESS_RESOLUTION is returned.
    Otherwise, iterate over the response_filters.
    If any of the filter match the response, then return the appropriate status.
    When `DefaultErrorHandler.backoff_time()` is invoked, iterate sequentially over the backoff_strategies and return the first non-None backoff time, else return None.

    Sample configs:

    1. retry 10 times
    `
        error_handler:
          max_retries: 10
    `
    2. backoff for 5 seconds
    `
        error_handler:
          backoff_strategies:
            - type: "ConstantBackoff"
              backoff_time_in_seconds: 5
    `
    3. retry on HTTP 404
    `
        error_handler:
          response_filters:
            - http_codes: [ 404 ]
              action: RETRY
    `
    4. ignore HTTP 404
    `
      error_handler:
        response_filters:
          - http_codes: [ 404 ]
            action: IGNORE
    `
    5. retry if error message contains `retrythisrequest!` substring
    `
        error_handler:
          response_filters:
            - error_message_contain: "retrythisrequest!"
              action: IGNORE
    `
    6. retry if 'code' is a field present in the response body
    `
        error_handler:
          response_filters:
            - predicate: "{{ 'code' in response }}"
              action: IGNORE
    `

    7. ignore 429 and retry on 404
    `
        error_handler:
        - http_codes: [ 429 ]
          action: IGNORE
        - http_codes: [ 404 ]
          action: RETRY
    `

    Attributes:
        response_filters (Optional[List[HttpResponseFilter]]): response filters to iterate on
        max_retries (Optional[Union[int, str]]): maximum retry attempts. Either a hardcoded int or
            a string that interpolates from the connector config (e.g.
            `"{{ config['max_retries_on_throttle'] }}"`). The string variant is evaluated once at
            construction time and replaced with the resolved int.
        backoff_strategies (Optional[List[BackoffStrategy]]): list of backoff strategies to use to determine how long
        to wait before retrying
    """

    parameters: InitVar[Mapping[str, Any]]
    config: Config
    response_filters: Optional[List[HttpResponseFilter]] = None
    # The base class declares max_retries as Optional[int]. We widen the input type to
    # also accept a Jinja-interpolatable string (e.g. "{{ config['max_retries_on_throttle'] }}"),
    # which is resolved to an int in __post_init__ so the post-construction invariant matches
    # the base class contract.
    max_retries: Optional[Union[int, str]] = 5  # type: ignore[assignment]
    max_time: int = 60 * 10
    backoff_strategies: Optional[List[BackoffStrategy]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not self.response_filters:
            self.response_filters = [HttpResponseFilter(config=self.config, parameters={})]

        self._last_request_to_attempt_count: MutableMapping[requests.PreparedRequest, int] = {}

        if isinstance(self.max_retries, str):
            evaluated = InterpolatedString(
                string=self.max_retries, default="5", parameters=parameters
            ).eval(config=self.config)
            try:
                self.max_retries = int(evaluated)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"DefaultErrorHandler.max_retries did not evaluate to an integer "
                    f"(got {evaluated!r})"
                ) from exc

    def interpret_response(
        self, response_or_exception: Optional[Union[requests.Response, Exception]]
    ) -> ErrorResolution:
        if self.response_filters:
            for response_filter in self.response_filters:
                matched_error_resolution = response_filter.matches(
                    response_or_exception=response_or_exception
                )
                if matched_error_resolution:
                    return matched_error_resolution
        if isinstance(response_or_exception, requests.Response):
            if response_or_exception.ok:
                return SUCCESS_RESOLUTION

        default_reponse_filter = DefaultHttpResponseFilter(parameters={}, config=self.config)
        default_response_filter_resolution = default_reponse_filter.matches(response_or_exception)

        return (
            default_response_filter_resolution
            if default_response_filter_resolution
            else create_fallback_error_resolution(response_or_exception)
        )

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int = 0,
    ) -> Optional[float]:
        backoff = None
        if self.backoff_strategies:
            for backoff_strategy in self.backoff_strategies:
                backoff = backoff_strategy.backoff_time(
                    response_or_exception=response_or_exception, attempt_count=attempt_count
                )
                if backoff:
                    return backoff
        return backoff
