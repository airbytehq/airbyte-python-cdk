#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Generic

from airbyte_cdk.connector import TConfig
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
    def __init__(self, source: Source, name: str):  # noqa: ANN204
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
