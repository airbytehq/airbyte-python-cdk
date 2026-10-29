#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union

from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.request_option import (
    RequestOption,
    RequestOptionType,
)
from airbyte_cdk.sources.declarative.requesters.request_options.request_options_provider import (
    RequestOptionsProvider,
)
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class SyncTokenRequestOptionsProvider(RequestOptionsProvider):
    """Inject the stored sync token, or the configured initial token, into requests."""

    token_option: RequestOption
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    initial_token: Optional[str] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._initial_token = (
            InterpolatedString.create(self.initial_token, parameters=parameters)
            if self.initial_token is not None
            else None
        )

    def _get_request_options(
        self, option_type: RequestOptionType, stream_slice: Optional[StreamSlice]
    ) -> Mapping[str, Any]:
        if self.token_option.inject_into != option_type:
            return {}
        token = (
            stream_slice.cursor_slice.get("token")
            if stream_slice and stream_slice.cursor_slice
            else None
        )
        if token is None and self._initial_token:
            token = self._initial_token.eval(self.config, valid_types=(str,))
        if token is None:
            return {}
        result: dict[str, Any] = {}
        self.token_option.inject_into_request(result, token, self.config)
        return result

    def get_request_params(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.request_parameter, stream_slice)

    def get_request_headers(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.header, stream_slice)

    def get_request_body_data(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Union[Mapping[str, Any], str]:
        return self._get_request_options(RequestOptionType.body_data, stream_slice)

    def get_request_body_json(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.body_json, stream_slice)
