#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#
"""File-based streams throttle legacy per-slice state emission.

The wiring lives in `DefaultFileBasedStream._get_checkpoint_reader()` rather than
in `Stream.read()`, so the generic base class is untouched and only file-based
streams change behaviour. Because it is a method on the stream class, connectors
that build their own `DefaultFileBasedStream` subclass from an overridden
`_make_default_stream()` without calling `super()` — source-s3 and source-gcs
both do — inherit it and cannot bypass it.
"""

import logging
from typing import Any, Mapping, Optional
from unittest.mock import MagicMock

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.file_based.config.csv_format import CsvFormat
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.stream import (
    DefaultFileBasedStream,
    PermissionsFileBasedStream,
)
from airbyte_cdk.sources.file_based.stream.cursor import DefaultFileBasedCursor
from airbyte_cdk.sources.streams.checkpoint import (
    DEFAULT_STATE_EMISSION_THROTTLE_SECONDS,
    ThrottledCheckpointReader,
)


def _config() -> FileBasedStreamConfig:
    return FileBasedStreamConfig(name="stream1", format=CsvFormat(), globs=["*.csv"])


def _stream(cls: type = DefaultFileBasedStream, **overrides: Any) -> DefaultFileBasedStream:
    config = _config()
    # No files: `_get_checkpoint_reader` walks `stream_slices()`, and an empty
    # listing is enough to reach the reader construction we care about.
    stream_reader = MagicMock()
    stream_reader.get_matching_files.return_value = []
    kwargs: Mapping[str, Any] = {
        "config": config,
        "catalog_schema": None,
        "stream_reader": stream_reader,
        "availability_strategy": None,
        "discovery_policy": None,
        "parsers": None,
        "validation_policy": None,
        "errors_collector": None,
        "cursor": DefaultFileBasedCursor(config),
        "use_file_transfer": False,
        "preserve_directory_structure": True,
        **overrides,
    }
    return cls(**kwargs)  # type: ignore[arg-type]


def _reader(stream: DefaultFileBasedStream) -> Any:
    return stream._get_checkpoint_reader(
        logger=logging.getLogger("test"),
        cursor_field=None,
        sync_mode=SyncMode.incremental,
        stream_state={},
    )


def test_checkpoint_reader_is_throttled_at_the_shared_default() -> None:
    reader = _reader(_stream())
    assert isinstance(reader, ThrottledCheckpointReader)
    assert reader._throttle_seconds == DEFAULT_STATE_EMISSION_THROTTLE_SECONDS


def test_connector_subclass_cannot_bypass_the_throttle() -> None:
    """The regression this file exists for: source-s3 and source-gcs subclass this
    stream and construct it themselves, so the throttle must ride on the class."""

    class _ConnectorStream(DefaultFileBasedStream):
        """Mirrors source_gcs.stream.GCSStream / source_s3 ThrottledFileBasedStream."""

    assert isinstance(_reader(_stream(_ConnectorStream)), ThrottledCheckpointReader)


def test_permissions_stream_is_throttled_too() -> None:
    """The permissions transfer path is on the same legacy emission path."""
    assert (
        PermissionsFileBasedStream.state_emission_throttle_seconds
        == DEFAULT_STATE_EMISSION_THROTTLE_SECONDS
    )


def test_setting_the_throttle_to_none_restores_the_unwrapped_reader() -> None:
    """An escape hatch that returns the base reader untouched, so the throttle can
    be turned off without a different code path."""

    class _UnthrottledStream(DefaultFileBasedStream):
        state_emission_throttle_seconds: Optional[float] = None

    assert not isinstance(_reader(_stream(_UnthrottledStream)), ThrottledCheckpointReader)
