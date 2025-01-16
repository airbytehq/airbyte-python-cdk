import csv
import gzip
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from io import BufferedIOBase, TextIOWrapper
from typing import Any, Generator, MutableMapping, Optional

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
        """
        with gzip.GzipFile(fileobj=data, mode="rb") as gzipobj:
            yield from self.inner_parser.parse(gzipobj)


@dataclass
class JsonParser(Parser):
    encoding: str = "utf-8"

    def parse(self, data: BufferedIOBase) -> Generator[MutableMapping[str, Any], None, None]:
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

    def parse(
        self,
        data: BufferedIOBase,
    ) -> Generator[MutableMapping[str, Any], None, None]:
        """
        Parse CSV data from decompressed bytes.
        """
        text_data = TextIOWrapper(data, encoding=self.encoding)  # type: ignore
        reader = csv.DictReader(text_data, delimiter=self.delimiter or ",")
        yield from reader


@dataclass
class CompositeRawDecoder(Decoder):
    """
    Decoder strategy to transform a requests.Response into a Generator[MutableMapping[str, Any], None, None]
    passed response.raw to parser(s).
    Note: response.raw is not decoded/decompressed by default.
    parsers should be instantiated recursively.
    Example:
    composite_raw_decoder = CompositeRawDecoder(parser=GzipParser(inner_parser=JsonLineParser(encoding="iso-8859-1")))
    """

    parser: Parser

    def is_stream_response(self) -> bool:
        return True

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        yield from self.parser.parse(data=response.raw)  # type: ignore[arg-type]
