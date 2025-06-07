# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import Any, List, Mapping, Optional

from airbyte_cdk import AbstractSource
from airbyte_cdk.models import AirbyteStateMessage, ConfiguredAirbyteCatalog, SyncMode
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read
from airbyte_cdk.test.models.outcome import ExpectedOutcome


def catalog(stream_name: str, sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    """Create a catalog with a single stream."""
    return CatalogBuilder().with_stream(stream_name, sync_mode).build()


def read_records(
    source: AbstractSource,
    config: Mapping[str, Any],
    stream_name: str,
    sync_mode: SyncMode,
    state: Optional[List[AirbyteStateMessage]] = None,
    *,
    expecting_exception: bool | None = None,  # Deprecated, use expected_outcome instead.
    expected_outcome: ExpectedOutcome | None = None,
) -> EntrypointOutput:
    """Read records from a stream."""
    if expecting_exception is not None and expected_outcome is not None:
        raise ValueError("Cannot set both expecting_exception and expected_outcome.")

    if expected_outcome is None:
        expected_outcome = ExpectedOutcome.from_expecting_exception_bool(expecting_exception)

    _catalog = catalog(stream_name, sync_mode)
    return read(source, config, _catalog, state, expected_outcome=expected_outcome)
