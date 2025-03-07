#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Generator, MutableMapping

import requests

COMPRESSSED_RESPONSE_TYPES = [
    "gzip",
    "x-gzip",
    "gzip, deflate",
    "x-gzip, deflate",
    "application/zip",
    "application/gzip",
    "application/x-gzip",
    "application/x-zip-compressed",
]


@dataclass
class Decoder:
    """
    Decoder strategy to transform a requests.Response into a Mapping[str, Any]
    """

    @abstractmethod
    def is_stream_response(self) -> bool:
        """
        Set to True if you'd like to use stream=True option in http requester
        """

    @abstractmethod
    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        """
        Decodes a requests.Response into a Mapping[str, Any] or an array
        :param response: the response to decode
        :return: Generator of Mapping describing the response
        """

    def is_compressed_response(self, response: requests.Response) -> bool:
        """
        Check if the response is compressed based on the `Content-Encoding` or `Content-Type` header.
        """
        return (
            response.headers.get("Content-Encoding") in COMPRESSSED_RESPONSE_TYPES
            or response.headers.get("Content-Type") in COMPRESSSED_RESPONSE_TYPES
        )
