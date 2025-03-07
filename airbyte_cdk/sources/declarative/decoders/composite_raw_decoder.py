#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import csv
import gzip
import io
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from io import BufferedIOBase, TextIOWrapper
from typing import Any, Dict, Generator, List, MutableMapping, Optional, Set, Tuple

import orjson
import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.decoders.decoder import Decoder
from airbyte_cdk.utils import AirbyteTracedException

logger = logging.getLogger("airbyte")


@dataclass
class Parser(ABC):
    @abstractmethod
    def parse(
        self,
        data: BufferedIOBase,
    ) -> Generator[MutableMapping[str, Any], None, None]:
        """
        Parse data and yield dictionaries.
        """
        pass


@dataclass
class GzipParser(Parser):
    inner_parser: Parser

    def parse(
        self,
        data: BufferedIOBase,
    ) -> Generator[MutableMapping[str, Any], None, None]:
        """
        Decompress gzipped bytes and pass decompressed data to the inner parser.

        IMPORTANT:
            - If the data is not gzipped, reset the pointer and pass the data to the inner parser as is.

        Note:
            - The data is not decoded by default.
        """

        with gzip.GzipFile(fileobj=data, mode="rb") as gzipobj:
            yield from self.inner_parser.parse(gzipobj)


@dataclass
class JsonParser(Parser):
    encoding: str = "utf-8"

    def parse(
        self,
        data: BufferedIOBase,
    ) -> Generator[MutableMapping[str, Any], None, None]:
        """
        Attempts to deserialize data using orjson library. As an extra layer of safety we fallback on the json library to deserialize the data.
        """
        raw_data = data.read()
        body_json = self._parse_orjson(raw_data) or self._parse_json(raw_data)

        if body_json is None:
            raise AirbyteTracedException(
                message="Response JSON data failed to be parsed. See logs for more information.",
                internal_message=f"Response JSON data failed to be parsed.",
                failure_type=FailureType.system_error,
            )

        if isinstance(body_json, list):
            yield from body_json
        else:
            yield from [body_json]

    def _parse_orjson(self, raw_data: bytes) -> Optional[Any]:
        try:
            return orjson.loads(raw_data.decode(self.encoding))
        except Exception as exc:
            logger.debug(
                f"Failed to parse JSON data using orjson library. Falling back to json library. {exc}"
            )
            return None

    def _parse_json(self, raw_data: bytes) -> Optional[Any]:
        try:
            return json.loads(raw_data.decode(self.encoding))
        except Exception as exc:
            logger.error(f"Failed to parse JSON data using json library. {exc}")
            return None


@dataclass
class JsonLineParser(Parser):
    encoding: Optional[str] = "utf-8"

    def parse(
        self,
        data: BufferedIOBase,
    ) -> Generator[MutableMapping[str, Any], None, None]:
        for line in data:
            try:
                yield json.loads(line.decode(encoding=self.encoding or "utf-8"))
            except json.JSONDecodeError as e:
                logger.warning(f"Cannot decode/parse line {line!r} as JSON, error: {e}")


@dataclass
class CsvParser(Parser):
    # TODO: migrate implementation to re-use file-base classes
    encoding: Optional[str] = "utf-8"
    delimiter: Optional[str] = ","

    def _get_delimiter(self) -> Optional[str]:
        """
        Get delimiter from the configuration. Check for the escape character and decode it.
        """
        if self.delimiter is not None:
            if self.delimiter.startswith("\\"):
                self.delimiter = self.delimiter.encode("utf-8").decode("unicode_escape")

        return self.delimiter

    def parse(
        self,
        data: BufferedIOBase,
    ) -> Generator[MutableMapping[str, Any], None, None]:
        """
        Parse CSV data from decompressed bytes.
        """
        text_data = TextIOWrapper(data, encoding=self.encoding)  # type: ignore
        reader = csv.DictReader(text_data, delimiter=self._get_delimiter() or ",")
        for row in reader:
            yield row


_HEADER = str
_HEADER_VALUE = str


class CompositeRawDecoder(Decoder):
    """
    Decoder strategy to transform a requests.Response into a Generator[MutableMapping[str, Any], None, None]
    passed response.raw to parser(s).

    Note: response.raw is not decoded/decompressed by default. Parsers should be instantiated recursively.

    Example:
        composite_raw_decoder = CompositeRawDecoder(
            parser=GzipParser(
                inner_parser=JsonLineParser(encoding="iso-8859-1")
            )
        )
    """

    @classmethod
    def by_headers(
        cls,
        parsers: List[Tuple[Set[_HEADER], Set[_HEADER_VALUE], Parser]],
        stream_response: bool,
        fallback_parser: Parser,
    ) -> "CompositeRawDecoder":
        parsers_by_header = {}
        for headers, header_values, parser in parsers:
            for header in headers:
                parsers_by_header[header] = {header_value: parser for header_value in header_values}
        return cls(fallback_parser, stream_response, parsers_by_header)

    def __init__(self, parser: Parser, stream_response: bool = True, parsers_by_header: Optional[Dict[_HEADER, Dict[_HEADER_VALUE, Parser]]] = None) -> None:
        self._parsers_by_header = parsers_by_header if parsers_by_header else {}
        self._fallback_parser = parser
        self._stream_response = stream_response

    def is_stream_response(self) -> bool:
        return self._stream_response

    def decode(
        self,
        response: requests.Response,
    ) -> Generator[MutableMapping[str, Any], None, None]:
        parser = self._select_parser(response)
        if self.is_stream_response():
            # urllib mentions that some interfaces don't play nice with auto_close
            # More info here: https://urllib3.readthedocs.io/en/stable/user-guide.html#using-io-wrappers-with-response-content
            # We have indeed observed some issues with CSV parsing.
            # Hence, we will manage the closing of the file ourselves until we find a better solution.
            response.raw.auto_close = False
            yield from parser.parse(
                data=response.raw,  # type: ignore[arg-type]
            )
            response.raw.close()
        else:
            yield from parser.parse(data=io.BytesIO(response.content))

    def _select_parser(self, response: requests.Response) -> Parser:
        for header, parser_by_header_value in self._parsers_by_header.items():
            if (
                header in response.headers
                and response.headers[header] in parser_by_header_value.keys()
            ):
                return parser_by_header_value[response.headers[header]]
        return self._fallback_parser
