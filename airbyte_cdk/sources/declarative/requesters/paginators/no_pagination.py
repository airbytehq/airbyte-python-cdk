#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from dataclasses import InitVar, dataclass
from typing import TYPE_CHECKING, Any

from airbyte_cdk.sources.declarative.requesters.paginators.paginator import Paginator


if TYPE_CHECKING:
    from collections.abc import Mapping, MutableMapping

    import requests

    from airbyte_cdk.sources.types import Record, StreamSlice, StreamState


@dataclass
class NoPagination(Paginator):
    """Pagination implementation that never returns a next page."""

    parameters: InitVar[Mapping[str, Any]]

    def path(self) -> str | None:
        return None

    def get_request_params(
        self,
        *,
        stream_state: StreamState | None = None,  # noqa: ARG002  (unused)
        stream_slice: StreamSlice | None = None,  # noqa: ARG002  (unused)
        next_page_token: Mapping[str, Any] | None = None,  # noqa: ARG002  (unused)
    ) -> MutableMapping[str, Any]:
        return {}

    def get_request_headers(
        self,
        *,
        stream_state: StreamState | None = None,  # noqa: ARG002  (unused)
        stream_slice: StreamSlice | None = None,  # noqa: ARG002  (unused)
        next_page_token: Mapping[str, Any] | None = None,  # noqa: ARG002  (unused)
    ) -> Mapping[str, str]:
        return {}

    def get_request_body_data(
        self,
        *,
        stream_state: StreamState | None = None,  # noqa: ARG002  (unused)
        stream_slice: StreamSlice | None = None,  # noqa: ARG002  (unused)
        next_page_token: Mapping[str, Any] | None = None,  # noqa: ARG002  (unused)
    ) -> Mapping[str, Any] | str:
        return {}

    def get_request_body_json(
        self,
        *,
        stream_state: StreamState | None = None,  # noqa: ARG002  (unused)
        stream_slice: StreamSlice | None = None,  # noqa: ARG002  (unused)
        next_page_token: Mapping[str, Any] | None = None,  # noqa: ARG002  (unused)
    ) -> Mapping[str, Any]:
        return {}

    def next_page_token(
        self,
        response: requests.Response,  # noqa: ARG002  (unused)
        last_page_size: int,  # noqa: ARG002  (unused)
        last_record: Record | None,  # noqa: ARG002  (unused)
    ) -> Mapping[str, Any]:
        return {}

    def reset(self, reset_value: Any | None = None) -> None:  # noqa: ANN401  (any-type)
        # No state to reset
        pass
