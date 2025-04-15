#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from abc import ABC, abstractmethod
from collections.abc import Generator, MutableMapping
from dataclasses import dataclass
from io import BufferedIOBase
from typing import Any, Optional

logger = logging.getLogger("airbyte")


PARSER_OUTPUT_TYPE = Generator[MutableMapping[str, Any], None, None]


@dataclass
class Parser(ABC):
    @abstractmethod
    def parse(self, data: BufferedIOBase) -> PARSER_OUTPUT_TYPE:
        """
        Parse data and yield dictionaries.
        """
        pass


# reusable parser types
PARSERS_TYPE = list[tuple[set[str], set[str], Parser]]
PARSERS_BY_HEADER_TYPE = Optional[dict[str, dict[str, Parser]]]
