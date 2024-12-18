#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
import logging
from abc import abstractmethod
from dataclasses import InitVar, dataclass
from typing import Any, Generator, Mapping, MutableMapping, Union

logger = logging.getLogger("airbyte")


@dataclass
class Parser:
    """
    Parser strategy to convert str, bytes, or bytearray data into MutableMapping[str, Any].
    """

    @abstractmethod
    def parse(
        self, data: Union[str, bytes, bytearray]
    ) -> Generator[MutableMapping[str, Any], None, None]:
        pass


@dataclass
class JsonParser(Parser):
    """
    Parser strategy for converting JSON-structure str, bytes, or bytearray data into MutableMapping[str, Any].
    """

    parameters: InitVar[Mapping[str, Any]]

    def parse(
        self, data: Union[str, bytes, bytearray]
    ) -> Generator[MutableMapping[str, Any], None, None]:
        try:
            body_json = json.loads(data)
        except json.JSONDecodeError:
            logger.warning(f"Data cannot be parsed into json: {data=}")
            yield {}

        if not isinstance(body_json, list):
            body_json = [body_json]
        if len(body_json) == 0:
            yield {}
        else:
            yield from body_json
