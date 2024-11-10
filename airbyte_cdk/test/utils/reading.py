# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read


if TYPE_CHECKING:
    from collections.abc import Mapping

    from airbyte_cdk import AbstractSource
    from airbyte_cdk.models import AirbyteStateMessage, ConfiguredAirbyteCatalog, SyncMode


def catalog(stream_name: str, sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    """Create a catalog with a single stream."""
    return CatalogBuilder().with_stream(stream_name, sync_mode).build()


def read_records(
    source: AbstractSource,
    config: Mapping[str, Any],
    stream_name: str,
    sync_mode: SyncMode,
    state: list[AirbyteStateMessage] | None = None,
    expecting_exception: bool = False,
) -> EntrypointOutput:
    """Read records from a stream."""
    _catalog = catalog(stream_name, sync_mode)
    return read(source, config, _catalog, state, expecting_exception)
