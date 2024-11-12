# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Mapping


class HttpResponse:
    def __init__(
        self, body: str, status_code: int = 200, headers: Mapping[str, str] = MappingProxyType({})
    ) -> None:
        self._body = body
        self._status_code = status_code
        self._headers = headers

    @property
    def body(self) -> str:
        return self._body

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def headers(self) -> Mapping[str, str]:
        return self._headers
