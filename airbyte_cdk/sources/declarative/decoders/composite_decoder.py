import gzip
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from io import BufferedIOBase
from typing import Any, Generator, MutableMapping, Optional

import pandas as pd
import requests
from numpy import nan

from airbyte_cdk.sources.declarative.decoders.decoder import Decoder

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
        gzipobj = gzip.GzipFile(fileobj=data, mode="rb")
        yield from self.inner_parser.parse(gzipobj)


@dataclass
class JsonLineParser(Parser):
    encoding: Optional[str] = "utf-8"

    def parse(
        self,
        data: BufferedIOBase,
    ) -> Generator[MutableMapping[str, Any], None, None]:
        for line in data:
            try:
                yield json.loads(line.decode(self.encoding))
            except json.JSONDecodeError:
                logger.warning(f"Cannot decode/parse line {line} as JSON")
                # Handle invalid JSON lines gracefully (e.g., log and skip)
                pass


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
        reader = pd.read_csv(data, sep=self.delimiter, iterator=True, dtype=object)
        for chunk in reader:
            chunk = chunk.replace({nan: None}).to_dict(orient="records")
            for row in chunk:
                yield row


@dataclass
class CompositeRawDecoder(Decoder):
    """
    Decoder strategy to transform a requests.Response into a Generator[MutableMapping[str, Any], None, None]
    passed response.raw to parser(s).
    Note: response.raw is not decoded/decompressed by default.
    parsers should be instantiated recursively.
    Example:
    composite_decoder = CompositeDecoder(parser=GzipParser(inner_parser=JsonLineParser(encoding="iso-8859-1")))
    """

    parser: Parser

    def is_stream_response(self) -> bool:
        return True

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        yield from self.parser.parse(data=response.raw)
