#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import gzip
import io
import logging
import zipfile
from dataclasses import InitVar, dataclass
from typing import Any, Generator, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.sources.declarative.decoders import Decoder
from airbyte_cdk.sources.declarative.decoders.parsers import JsonParser, Parser

logger = logging.getLogger("airbyte")


@dataclass
class ZipfileDecoder(Decoder):
    parameters: InitVar[Mapping[str, Any]]
    parser: Optional[Parser] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parser = (
            self.parser(parameters=parameters) if self.parser else JsonParser(parameters=parameters)
        )

    def is_stream_response(self) -> bool:
        return False

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        try:
            zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        except zipfile.BadZipFile as e:
            logger.exception(e)
            logger.error(
                f"Received an invalid zip file in response to URL: {response.request.url}. "
                f"The size of the response body is: {len(response.content)}"
            )
            yield {}

        for gzip_filename in zip_file.namelist():
            with zip_file.open(gzip_filename) as file:
                try:
                    for data in gzip.open(file):
                        yield from self._parser.parse(data)
                except gzip.BadGzipFile as e:
                    logger.exception(e)
                    logger.error(f"Fail to read contents of zipped response: {e}")
                    yield {}
