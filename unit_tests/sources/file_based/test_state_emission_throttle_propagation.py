#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#
"""The source-level throttle hook must reach every stream a FileBasedSource builds.

Connectors routinely override `_make_default_stream` without calling `super()`
(source-s3 and source-gcs both do), so applying the throttle inside that factory
would silently miss exactly the connectors the feature exists for. These tests
pin the propagation to `_make_file_based_stream`, which nothing overrides.
"""

from typing import Any, Optional

from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec
from airbyte_cdk.sources.file_based.config.csv_format import CsvFormat
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.stream import AbstractFileBasedStream, DefaultFileBasedStream
from airbyte_cdk.sources.file_based.stream.cursor import (
    AbstractFileBasedCursor,
    DefaultFileBasedCursor,
)
from unit_tests.sources.file_based.in_memory_files_source import InMemoryFilesSource


def _source(**kwargs: Any) -> InMemoryFilesSource:
    return InMemoryFilesSource(
        files={},
        file_type="csv",
        availability_strategy=None,
        discovery_policy=None,
        validation_policies=None,
        parsers=None,
        stream_reader=None,
        catalog=None,
        config=None,
        state=None,
        file_write_options={},
        cursor_cls=DefaultFileBasedCursor,
        **kwargs,
    )


def _stream_config() -> FileBasedStreamConfig:
    return FileBasedStreamConfig(name="stream1", format=CsvFormat(), globs=["*.csv"])


def _parsed_config(source: InMemoryFilesSource) -> AbstractFileBasedSpec:
    return source.spec_class(streams=[], bucket="b")  # type: ignore[call-arg]


class _OverridingSource(InMemoryFilesSource):
    """Mirrors source-s3 / source-gcs: replaces the factory, never calls super()."""

    _stream_state_emission_throttle_seconds = 600.0

    def _make_default_stream(
        self,
        stream_config: FileBasedStreamConfig,
        cursor: Optional[AbstractFileBasedCursor],
        parsed_config: AbstractFileBasedSpec,
    ) -> AbstractFileBasedStream:
        return DefaultFileBasedStream(
            config=stream_config,
            catalog_schema=None,
            stream_reader=self.stream_reader,
            availability_strategy=self.availability_strategy,
            discovery_policy=self.discovery_policy,
            parsers=self.parsers,
            validation_policy=self._validate_and_get_validation_policy(stream_config),
            errors_collector=self.errors_collector,
            cursor=cursor,
            use_file_transfer=False,
            preserve_directory_structure=True,
        )


def test_default_is_none_so_behaviour_is_unchanged() -> None:
    source = _source()
    assert source._stream_state_emission_throttle_seconds is None
    stream = source._make_file_based_stream(
        _stream_config(), DefaultFileBasedCursor(_stream_config()), _parsed_config(source)
    )
    assert stream.state_emission_throttle_seconds is None


def test_throttle_propagates_to_the_stream() -> None:
    source = _source()
    source._stream_state_emission_throttle_seconds = 600.0
    stream = source._make_file_based_stream(
        _stream_config(), DefaultFileBasedCursor(_stream_config()), _parsed_config(source)
    )
    assert stream.state_emission_throttle_seconds == 600.0


def test_throttle_propagates_even_when_the_factory_is_overridden() -> None:
    """The regression this file exists for: applying the throttle inside
    `_make_default_stream` would make it a no-op for every connector that
    replaces that factory."""
    source = _OverridingSource(
        files={},
        file_type="csv",
        availability_strategy=None,
        discovery_policy=None,
        validation_policies=None,
        parsers=None,
        stream_reader=None,
        catalog=None,
        config=None,
        state=None,
        file_write_options={},
        cursor_cls=DefaultFileBasedCursor,
    )
    stream = source._make_file_based_stream(
        _stream_config(), DefaultFileBasedCursor(_stream_config()), _parsed_config(source)
    )
    assert stream.state_emission_throttle_seconds == 600.0
