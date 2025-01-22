#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections.abc import Mapping, MutableMapping
from dataclasses import InitVar, dataclass
from typing import Any

import requests

from airbyte_cdk.sources.declarative.requesters.paginators.paginator import Paginator
from airbyte_cdk.sources.types import Record, StreamSlice, StreamState


@dataclass
class NoPagination(Paginator):
    """
    Pagination implementation that never returns a next page.
    """

    parameters: InitVar[Mapping[str, Any]]

    def path(self, next_page_token: Mapping[str, Any] | None) -> str | None:  # noqa: ARG002
        return None

    def get_request_params(
        self,
        *,
        stream_state: StreamState | None = None,  # noqa: ARG002
        stream_slice: StreamSlice | None = None,  # noqa: ARG002
        next_page_token: Mapping[str, Any] | None = None,  # noqa: ARG002
    ) -> MutableMapping[str, Any]:
        return {}

    def get_request_headers(
        self,
        *,
        stream_state: StreamState | None = None,  # noqa: ARG002
        stream_slice: StreamSlice | None = None,  # noqa: ARG002
        next_page_token: Mapping[str, Any] | None = None,  # noqa: ARG002
    ) -> Mapping[str, str]:
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

    def get_initial_token(self) -> Any | None:  # noqa: ANN401
        return None

    def next_page_token(
        self,
        response: requests.Response,  # noqa: ARG002
        last_page_size: int,  # noqa: ARG002
        last_record: Record | None,  # noqa: ARG002
        last_page_token_value: Any | None,  # noqa: ANN401, ARG002
    ) -> Mapping[str, Any] | None:
        return {}
