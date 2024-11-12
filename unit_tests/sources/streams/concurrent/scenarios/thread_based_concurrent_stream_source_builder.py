#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping
from typing import Any

from unit_tests.sources.file_based.scenarios.scenario_builder import SourceBuilder

from airbyte_cdk.models import (
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
    DestinationSyncMode,
    SyncMode,
)
from airbyte_cdk.sources.concurrent_source.concurrent_source import ConcurrentSource
from airbyte_cdk.sources.concurrent_source.concurrent_source_adapter import ConcurrentSourceAdapter
from airbyte_cdk.sources.message import InMemoryMessageRepository, MessageRepository
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.concurrent.adapters import StreamFacade
from airbyte_cdk.sources.streams.concurrent.cursor import FinalStateCursor
from airbyte_cdk.sources.streams.concurrent.default_stream import DefaultStream
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.partition_generator import PartitionGenerator
from airbyte_cdk.sources.streams.concurrent.partitions.record import Record
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.utils.slice_logger import SliceLogger


class LegacyStream(Stream):
    def primary_key(self) -> str | list[str] | list[list[str]] | None:
        return None

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[StreamData]:
        yield from []


class ConcurrentCdkSource(ConcurrentSourceAdapter):
    def __init__(
        self,
        streams: list[DefaultStream],
        message_repository: MessageRepository | None,
        max_workers,
        timeout_in_seconds,
    ):
        concurrent_source = ConcurrentSource.create(
            1, 1, streams[0]._logger, NeverLogSliceLogger(), message_repository
        )
        super().__init__(concurrent_source)
        self._streams = streams
        self._message_repository = message_repository

    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> tuple[bool, Any | None]:
        # Check is not verified because it is up to the source to implement this method
        return True, None

    def streams(self, config: Mapping[str, Any]) -> list[Stream]:
        return [
            StreamFacade(
                s,
                LegacyStream(),
                FinalStateCursor(
                    stream_name=s.name,
                    stream_namespace=s.namespace,
                    message_repository=self.message_repository,
                ),
                NeverLogSliceLogger(),
                s._logger,
            )
            for s in self._streams
        ]

    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        return ConnectorSpecification(connectionSpecification={})

    def read_catalog(self, catalog_path: str) -> ConfiguredAirbyteCatalog:
        return ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=StreamFacade(
                        s,
                        LegacyStream(),
                        FinalStateCursor(
                            stream_name=s.name,
                            stream_namespace=s.namespace,
                            message_repository=InMemoryMessageRepository(),
                        ),
                        NeverLogSliceLogger(),
                        s._logger,
                    ).as_airbyte_stream(),
                    sync_mode=SyncMode.full_refresh,
                    destination_sync_mode=DestinationSyncMode.overwrite,
                )
                for s in self._streams
            ]
        )

    @property
    def message_repository(self) -> None | MessageRepository:
        return self._message_repository


class InMemoryPartitionGenerator(PartitionGenerator):
    def __init__(self, partitions: list[Partition]):
        self._partitions = partitions

    def generate(self) -> Iterable[Partition]:
        yield from self._partitions


class InMemoryPartition(Partition):
    def stream_name(self) -> str:
        return self._stream_name

    def __init__(self, name, stream_name, _slice, records):
        self._name = name
        self._stream_name = stream_name
        self._slice = _slice
        self._records = records
        self._is_closed = False

    def read(self) -> Iterable[Record]:
        for record_or_exception in self._records:
            if isinstance(record_or_exception, Exception):
                raise record_or_exception
            else:
                yield record_or_exception

    def to_slice(self) -> Mapping[str, Any] | None:
        return self._slice

    def __hash__(self) -> int:
        if self._slice:
            # Convert the slice to a string so that it can be hashed
            s = json.dumps(self._slice, sort_keys=True)
            return hash((self._name, s))
        return hash(self._name)

    def close(self) -> None:
        self._is_closed = True

    def is_closed(self) -> bool:
        return self._is_closed


class ConcurrentSourceBuilder(SourceBuilder[ConcurrentCdkSource]):
    def __init__(self):
        self._streams: list[DefaultStream] = []
        self._message_repository = None

    def build(self, configured_catalog: Mapping[str, Any] | None, _, __) -> ConcurrentCdkSource:
        return ConcurrentCdkSource(self._streams, self._message_repository, 1, 1)

    def set_streams(self, streams: list[DefaultStream]) -> ConcurrentSourceBuilder:
        self._streams = streams
        return self

    def set_message_repository(
        self, message_repository: MessageRepository
    ) -> ConcurrentSourceBuilder:
        self._message_repository = message_repository
        return self


class NeverLogSliceLogger(SliceLogger):
    def should_log_slice_message(self, logger: logging.Logger) -> bool:
        return False
