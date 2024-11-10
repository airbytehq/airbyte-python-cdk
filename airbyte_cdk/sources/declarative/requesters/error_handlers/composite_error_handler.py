#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from dataclasses import InitVar, dataclass
from typing import TYPE_CHECKING, Any

from airbyte_cdk.sources.streams.http.error_handlers import ErrorHandler
from airbyte_cdk.sources.streams.http.error_handlers.response_models import (
    ErrorResolution,
    ResponseAction,
    create_fallback_error_resolution,
)


if TYPE_CHECKING:
    from collections.abc import Mapping

    import requests


@dataclass
class CompositeErrorHandler(ErrorHandler):
    """Error handler that sequentially iterates over a list of `ErrorHandler`s

    Sample config chaining 2 different retriers:
        error_handler:
          type: "CompositeErrorHandler"
          error_handlers:
            - response_filters:
                - predicate: "{{ 'codase' in response }}"
                  action: RETRY
              backoff_strategies:
                - type: "ConstantBackoff"
                  backoff_time_in_seconds: 5
            - response_filters:
                - http_codes: [ 403 ]
                  action: RETRY
              backoff_strategies:
                - type: "ConstantBackoff"
                  backoff_time_in_seconds: 10
    Attributes:
        error_handlers (List[ErrorHandler]): list of error handlers
    """

    error_handlers: list[ErrorHandler]
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not self.error_handlers:
            raise ValueError("CompositeErrorHandler expects at least 1 underlying error handler")

    @property
    def max_retries(self) -> int | None:
        return self.error_handlers[0].max_retries

    @property
    def max_time(self) -> int | None:
        return max(error_handler.max_time or 0 for error_handler in self.error_handlers)

    def interpret_response(
        self, response_or_exception: requests.Response | Exception | None
    ) -> ErrorResolution:
        matched_error_resolution = None
        for error_handler in self.error_handlers:
            matched_error_resolution = error_handler.interpret_response(response_or_exception)

            if not isinstance(matched_error_resolution, ErrorResolution):
                continue

            if matched_error_resolution.response_action == ResponseAction.SUCCESS:
                return matched_error_resolution

            if matched_error_resolution.response_action in {
                ResponseAction.RETRY,
                ResponseAction.IGNORE,
            }:
                return matched_error_resolution
        if matched_error_resolution:
            return matched_error_resolution

        return create_fallback_error_resolution(response_or_exception)
