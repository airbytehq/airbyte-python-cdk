#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#
"""File-based streams throttle legacy per-slice state emission by default.

The throttle is a class attribute on `DefaultFileBasedStream` rather than
something the source injects, because connectors (source-s3, source-gcs) build
their own subclass in an overridden `_make_default_stream()` that never calls
`super()`. Inheritance reaches those; injection would not.

These tests pin that, and pin the blast radius: the generic `Stream` base is
untouched, so non-file-based legacy streams keep emitting once per slice.
"""

from airbyte_cdk.sources.file_based.stream import (
    DefaultFileBasedStream,
    PermissionsFileBasedStream,
)
from airbyte_cdk.sources.streams.core import DEFAULT_STATE_EMISSION_THROTTLE_SECONDS, Stream


def test_default_file_based_stream_throttles_by_default() -> None:
    assert (
        DefaultFileBasedStream.state_emission_throttle_seconds
        == DEFAULT_STATE_EMISSION_THROTTLE_SECONDS
    )


def test_permissions_stream_inherits_the_throttle() -> None:
    """The permissions transfer path is on the same legacy per-slice emission
    path, so it must not be left out."""
    assert (
        PermissionsFileBasedStream.state_emission_throttle_seconds
        == DEFAULT_STATE_EMISSION_THROTTLE_SECONDS
    )


def test_connector_subclass_inherits_the_throttle() -> None:
    """The regression this file exists for.

    source-s3 and source-gcs subclass `DefaultFileBasedStream` and construct it
    from an overridden `_make_default_stream()` without calling `super()`. A
    throttle applied by the source would silently miss them; an inherited class
    attribute cannot.
    """

    class _ConnectorStream(DefaultFileBasedStream):
        """Mirrors source_gcs.stream.GCSStream / source_s3 ThrottledFileBasedStream."""

    assert (
        _ConnectorStream.state_emission_throttle_seconds == DEFAULT_STATE_EMISSION_THROTTLE_SECONDS
    )


def test_generic_stream_base_is_not_throttled() -> None:
    """Bounds the blast radius: only file-based streams change behaviour."""
    assert Stream.state_emission_throttle_seconds is None
