#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Optional, Type

from airbyte_cdk.sources.file_based.availability_strategy.abstract_file_based_availability_strategy import (
    AbstractFileBasedAvailabilityStrategy,
)
from airbyte_cdk.sources.file_based.discovery_policy import (
    AbstractDiscoveryPolicy,
    DefaultDiscoveryPolicy,
)
from airbyte_cdk.sources.file_based.file_based_source import default_parsers
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.schema_validation_policies import AbstractSchemaValidationPolicy
from airbyte_cdk.sources.file_based.stream.cursor import AbstractFileBasedCursor
from airbyte_cdk.sources.source import TState
from unit_tests.sources.file_based.in_memory_files_source import InMemoryFilesSource
from unit_tests.sources.file_based.scenarios.scenario_builder import SourceBuilder


class FileBasedSourceBuilder(SourceBuilder[InMemoryFilesSource]):
    def __init__(self) -> None:
        self._files: Mapping[str, Any] = {}
        self._file_type: str | None = None
        self._availability_strategy: AbstractFileBasedAvailabilityStrategy | None = None
        self._discovery_policy: AbstractDiscoveryPolicy = DefaultDiscoveryPolicy()
        self._validation_policies: Mapping[str, AbstractSchemaValidationPolicy] | None = None
        self._parsers = default_parsers
        self._stream_reader: AbstractFileBasedStreamReader | None = None
        self._file_write_options: Mapping[str, Any] = {}
        self._cursor_cls: type[AbstractFileBasedCursor] | None = None
        self._config: Mapping[str, Any] | None = None
        self._state: TState | None = None

    def build(
        self,
        configured_catalog: Mapping[str, Any] | None,
        config: Mapping[str, Any] | None,
        state: TState | None,
    ) -> InMemoryFilesSource:
        if self._file_type is None:
            raise ValueError("file_type is not set")
        return InMemoryFilesSource(
            self._files,
            self._file_type,
            self._availability_strategy,
            self._discovery_policy,
            self._validation_policies,
            self._parsers,
            self._stream_reader,
            configured_catalog,
            config,
            state,
            self._file_write_options,
            self._cursor_cls,
        )

    def set_files(self, files: Mapping[str, Any]) -> "FileBasedSourceBuilder":
        self._files = files
        return self

    def set_file_type(self, file_type: str) -> "FileBasedSourceBuilder":
        self._file_type = file_type
        return self

    def set_parsers(self, parsers: Mapping[type[Any], FileTypeParser]) -> "FileBasedSourceBuilder":
        self._parsers = parsers
        return self

    def set_availability_strategy(
        self, availability_strategy: AbstractFileBasedAvailabilityStrategy
    ) -> "FileBasedSourceBuilder":
        self._availability_strategy = availability_strategy
        return self

    def set_discovery_policy(
        self, discovery_policy: AbstractDiscoveryPolicy
    ) -> "FileBasedSourceBuilder":
        self._discovery_policy = discovery_policy
        return self

    def set_validation_policies(
        self, validation_policies: Mapping[str, AbstractSchemaValidationPolicy]
    ) -> "FileBasedSourceBuilder":
        self._validation_policies = validation_policies
        return self

    def set_stream_reader(
        self, stream_reader: AbstractFileBasedStreamReader
    ) -> "FileBasedSourceBuilder":
        self._stream_reader = stream_reader
        return self

    def set_cursor_cls(self, cursor_cls: AbstractFileBasedCursor) -> "FileBasedSourceBuilder":
        self._cursor_cls = cursor_cls
        return self

    def set_file_write_options(
        self, file_write_options: Mapping[str, Any]
    ) -> "FileBasedSourceBuilder":
        self._file_write_options = file_write_options
        return self

    def copy(self) -> "FileBasedSourceBuilder":
        return deepcopy(self)
