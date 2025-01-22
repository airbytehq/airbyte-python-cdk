#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from collections.abc import Mapping
from dataclasses import InitVar, dataclass
from typing import Any

from airbyte_cdk.sources.declarative.requesters.request_options.request_options_provider import (
    RequestOptionsProvider,
)
from airbyte_cdk.sources.types import StreamSlice, StreamState


@dataclass
class DefaultRequestOptionsProvider(RequestOptionsProvider):
    """
    Request options provider that extracts fields from the stream_slice and injects them into the respective location in the
    outbound request being made
    """

    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        pass

    def get_request_params(
        self,
        *,
        stream_state: StreamState | None = None,  # noqa: ARG002
        stream_slice: StreamSlice | None = None,  # noqa: ARG002
        next_page_token: Mapping[str, Any] | None = None,  # noqa: ARG002
    ) -> Mapping[str, Any]:
        return {}

    def get_request_headers(
        self,
        *,
        stream_state: StreamState | None = None,  # noqa: ARG002
        stream_slice: StreamSlice | None = None,  # noqa: ARG002
        next_page_token: Mapping[str, Any] | None = None,  # noqa: ARG002
    ) -> Mapping[str, Any]:
        return {}

    def get_request_body_data(
        self,
        *,
        stream_state: StreamState | None = None,  # noqa: ARG002
        stream_slice: StreamSlice | None = None,  # noqa: ARG002
        next_page_token: Mapping[str, Any] | None = None,  # noqa: ARG002
    ) -> Mapping[str, Any] | str:
        return {}

    def get_request_body_json(
        self,
        *,
        stream_state: StreamState | None = None,  # noqa: ARG002
        stream_slice: StreamSlice | None = None,  # noqa: ARG002
        next_page_token: Mapping[str, Any] | None = None,  # noqa: ARG002
    ) -> Mapping[str, Any]:
        return {}
