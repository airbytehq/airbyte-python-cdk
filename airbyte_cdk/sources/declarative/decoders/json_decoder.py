#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# import codecs
import logging
from dataclasses import InitVar, dataclass
from typing import Any, Generator, List, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.sources.declarative.decoders import CompositeRawDecoder, JsonParser
from airbyte_cdk.sources.declarative.decoders.decoder import Decoder

logger = logging.getLogger("airbyte")


class JsonDecoder(Decoder):
    """
    Decoder strategy that returns the json-encoded content of a response, if any.

    Usually, we would try to instantiate the equivalent `CompositeRawDecoder(parser=JsonParser(), stream_response=False)` but there were specific historical behaviors related to the JsonDecoder that we didn't know if we could remove like the fallback on {} in case of errors.
    """

    def __init__(self, parameters: Mapping[str, Any]):
        self._decoder = CompositeRawDecoder(parser=JsonParser(), stream_response=False)

    def is_stream_response(self) -> bool:
        return self._decoder.is_stream_response()

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        """
        Given the response is an empty string or an emtpy list, the function will return a generator with an empty mapping.
        """
        try:
            yield from self._decoder.decode(response)
        except requests.exceptions.JSONDecodeError:
            logger.warning(
                f"Response cannot be parsed into json: {response.status_code=}, {response.text=}"
            )
            yield {}


@dataclass
class IterableDecoder(Decoder):
    """
    Decoder strategy that returns the string content of the response, if any.
    """

    parameters: InitVar[Mapping[str, Any]]

    def is_stream_response(self) -> bool:
        return True

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        for line in response.iter_lines():
            yield {"record": line.decode()}
