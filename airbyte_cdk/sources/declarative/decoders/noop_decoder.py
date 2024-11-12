# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from airbyte_cdk.sources.declarative.decoders.decoder import Decoder


if TYPE_CHECKING:
    from collections.abc import Generator, Mapping

    import requests


logger = logging.getLogger("airbyte")


class NoopDecoder(Decoder):
    def is_stream_response(self) -> bool:
        return False

    def decode(  # type: ignore[override]  # Base class returns MutableMapping
        self,
        response: requests.Response,  # noqa: ARG002  (unused)
    ) -> Generator[Mapping[str, Any], None, None]:
        yield from [{}]
