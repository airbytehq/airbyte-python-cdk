#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Generator, MutableMapping

    import requests


@dataclass
class Decoder:
    """Decoder strategy to transform a requests.Response into a Mapping[str, Any]"""

    @abstractmethod
    def is_stream_response(self) -> bool:
        """Set to True if you'd like to use stream=True option in http requester"""

    @abstractmethod
    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        """Decodes a requests.Response into a Mapping[str, Any] or an array
        :param response: the response to decode
        :return: Generator of Mapping describing the response
        """
