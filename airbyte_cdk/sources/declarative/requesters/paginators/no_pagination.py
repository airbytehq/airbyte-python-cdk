#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Optional, Union
from collections.abc import Mapping, MutableMapping

import requests

from airbyte_cdk.sources.declarative.requesters.paginators.paginator import Paginator
from airbyte_cdk.sources.types import Record, StreamSlice, StreamState


@dataclass
class NoPagination(Paginator):
    """
    Pagination implementation that never returns a next page.
    """

    parameters: InitVar[Mapping[str, Any]]

    def path(
        self,
        next_page_token: Mapping[str, Any] | None,
        stream_state: Mapping[str, Any] | None = None,
        stream_slice: StreamSlice | None = None,
    ) -> str | None:
        return None

    def get_request_params(
        self,
        *,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> MutableMapping[str, Any]:
        return {}

    def get_request_headers(
        self,
        *,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> Mapping[str, str]:
        return {}

    def get_request_body_data(
        self,
        *,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any] | str:
        return {}

    def get_request_body_json(
        self,
        *,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        return {}

    def get_initial_token(self) -> Any | None:
        return None

    def next_page_token(
        self,
        response: requests.Response,
        last_page_size: int,
        last_record: Record | None,
        last_page_token_value: Any | None,
    ) -> Mapping[str, Any] | None:
        return {}
