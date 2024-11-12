#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic

from airbyte_cdk.connector import TConfig


if TYPE_CHECKING:
    from collections.abc import Iterable

    from airbyte_cdk.models import (
        AirbyteCatalog,
        AirbyteMessage,
        AirbyteStateMessage,
        ConfiguredAirbyteCatalog,
        ConnectorSpecification,
    )
    from airbyte_cdk.sources.source import Source


class SourceRunner(ABC, Generic[TConfig]):
    @abstractmethod
    def spec(self) -> ConnectorSpecification:
        pass

    @abstractmethod
    def discover(self, config: TConfig) -> AirbyteCatalog:
        pass

    @abstractmethod
    def read(
        self,
        config: TConfig,
        catalog: ConfiguredAirbyteCatalog,
        state: AirbyteStateMessage | None,
    ) -> Iterable[AirbyteMessage]:
        pass


class CDKRunner(SourceRunner[TConfig]):
    def __init__(self, source: Source, name: str) -> None:
        self._source = source
        self._logger = logging.getLogger(name)

    def spec(self) -> ConnectorSpecification:
        return self._source.spec(self._logger)

    def discover(self, config: TConfig) -> AirbyteCatalog:
        return self._source.discover(self._logger, config)

    def read(
        self,
        config: TConfig,
        catalog: ConfiguredAirbyteCatalog,
        state: AirbyteStateMessage | None,
    ) -> Iterable[AirbyteMessage]:
        return self._source.read(self._logger, config, catalog, state=[state] if state else [])
