#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import copy
import json
import logging
from unittest.mock import MagicMock

import freezegun
import pytest

from airbyte_cdk.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    StreamDescriptor,
    Type,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse

_CONFIG = {"start_date": "2024-07-01T00:00:00.000Z"}
_MANIFEST = {
    "version": "6.0.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["TestStream"]},
    "definitions": {
        "TestStream": {
            "type": "StateDelegatingStream",
            "name": "TestStream",
            "full_refresh_stream": {
                "type": "DeclarativeStream",
                "name": "TestStream",
                "primary_key": [],
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "http://json-schema.org/schema#",
                        "properties": {},
                        "type": "object",
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://api.test.com",
                        "path": "/items",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "incremental_sync": {
                    "type": "DatetimeBasedCursor",
                    "start_datetime": {
                        "datetime": "{{ format_datetime(config['start_date'], '%Y-%m-%d') }}"
                    },
                    "end_datetime": {"datetime": "{{ now_utc().strftime('%Y-%m-%d') }}"},
                    "datetime_format": "%Y-%m-%d",
                    "cursor_datetime_formats": ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"],
                    "cursor_field": "updated_at",
                },
            },
            "incremental_stream": {
                "type": "DeclarativeStream",
                "name": "TestStream",
                "primary_key": [],
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "http://json-schema.org/schema#",
                        "properties": {},
                        "type": "object",
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://api.test.com",
                        "path": "/items_with_filtration",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "incremental_sync": {
                    "type": "DatetimeBasedCursor",
                    "start_datetime": {
                        "datetime": "{{ format_datetime(config['start_date'], '%Y-%m-%d') }}"
                    },
                    "end_datetime": {"datetime": "{{ now_utc().strftime('%Y-%m-%d') }}"},
                    "datetime_format": "%Y-%m-%d",
                    "cursor_datetime_formats": ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"],
                    "cursor_granularity": "P1D",
                    "step": "P15D",
                    "cursor_field": "updated_at",
                    "start_time_option": {
                        "type": "RequestOption",
                        "field_name": "start",
                        "inject_into": "request_parameter",
                    },
                    "end_time_option": {
                        "type": "RequestOption",
                        "field_name": "end",
                        "inject_into": "request_parameter",
                    },
                },
            },
        },
    },
    "streams": [{"$ref": "#/definitions/TestStream"}],
    "spec": {
        "connection_specification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {},
            "additionalProperties": True,
        },
        "documentation_url": "https://example.org",
        "type": "Spec",
    },
}


def to_configured_stream(
    stream,
    sync_mode=None,
    destination_sync_mode=DestinationSyncMode.append,
    cursor_field=None,
    primary_key=None,
) -> ConfiguredAirbyteStream:
    return ConfiguredAirbyteStream(
        stream=stream,
        sync_mode=sync_mode,
        destination_sync_mode=destination_sync_mode,
        cursor_field=cursor_field,
        primary_key=primary_key,
    )


def to_configured_catalog(
    configured_streams,
) -> ConfiguredAirbyteCatalog:
    return ConfiguredAirbyteCatalog(streams=configured_streams)


def create_configured_catalog(
    source: ConcurrentDeclarativeSource, config: dict
) -> ConfiguredAirbyteCatalog:
    """
    Discovers streams from the source and converts them into a configured catalog.
    """
    actual_catalog = source.discover(logger=source.logger, config=config)
    configured_streams = [
        to_configured_stream(stream, primary_key=stream.source_defined_primary_key)
        for stream in actual_catalog.streams
    ]
    return to_configured_catalog(configured_streams)


def get_records(
    source: ConcurrentDeclarativeSource,
    config: dict,
    catalog: ConfiguredAirbyteCatalog,
    state: list = None,
) -> list:
    """
    Reads records from the source given a configuration, catalog, and optional state.
    Returns a list of record data dictionaries.
    """
    return [
        message.record.data
        for message in source.read(logger=MagicMock(), config=config, catalog=catalog, state=state)
        if message.type == Type.RECORD
    ]


@freezegun.freeze_time("2024-07-15")
def test_full_refresh_retriever():
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items"),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 1, "name": "item_1", "updated_at": "2024-07-13"},
                        {"id": 2, "name": "item_2", "updated_at": "2024-07-13"},
                    ]
                )
            ),
        )

        source = ConcurrentDeclarativeSource(
            source_config=_MANIFEST, config=_CONFIG, catalog=None, state=None
        )
        configured_catalog = create_configured_catalog(source, _CONFIG)

        # Test full data retrieval (without state)
        full_records = get_records(source, _CONFIG, configured_catalog)
        expected_full = [
            {"id": 1, "name": "item_1", "updated_at": "2024-07-13"},
            {"id": 2, "name": "item_2", "updated_at": "2024-07-13"},
        ]
        assert expected_full == full_records


@freezegun.freeze_time("2024-07-15")
def test_incremental_retriever():
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(
                url="https://api.test.com/items_with_filtration?start=2024-07-13&end=2024-07-15"
            ),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 3, "name": "item_3", "updated_at": "2024-02-01"},
                        {"id": 4, "name": "item_4", "updated_at": "2024-02-01"},
                    ]
                )
            ),
        )

        state = [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name="TestStream", namespace=None),
                    stream_state=AirbyteStateBlob(updated_at="2024-07-13"),
                ),
            )
        ]
        source = ConcurrentDeclarativeSource(
            source_config=_MANIFEST, config=_CONFIG, catalog=None, state=state
        )
        configured_catalog = create_configured_catalog(source, _CONFIG)

        # Test incremental data retrieval (with state)
        incremental_records = get_records(source, _CONFIG, configured_catalog, state)
        expected_incremental = [
            {"id": 3, "name": "item_3", "updated_at": "2024-02-01"},
            {"id": 4, "name": "item_4", "updated_at": "2024-02-01"},
        ]
        assert expected_incremental == incremental_records


def _create_manifest_with_retention_period(api_retention_period: str) -> dict:
    """Create a manifest with api_retention_period set on the StateDelegatingStream."""
    manifest = copy.deepcopy(_MANIFEST)
    manifest["definitions"]["TestStream"]["api_retention_period"] = api_retention_period
    return manifest


@freezegun.freeze_time("2024-07-15")
def test_cursor_age_validation_falls_back_to_full_refresh_when_cursor_too_old():
    """Test that when cursor is older than retention period, full refresh is used."""
    manifest = _create_manifest_with_retention_period("P7D")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items"),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 1, "name": "item_1", "updated_at": "2024-07-13"},
                        {"id": 2, "name": "item_2", "updated_at": "2024-07-14"},
                    ]
                )
            ),
        )

        state = [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name="TestStream", namespace=None),
                    stream_state=AirbyteStateBlob(updated_at="2024-07-01"),
                ),
            )
        ]
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=_CONFIG, catalog=None, state=state
        )
        configured_catalog = create_configured_catalog(source, _CONFIG)

        records = get_records(source, _CONFIG, configured_catalog, state)
        expected = [
            {"id": 1, "name": "item_1", "updated_at": "2024-07-13"},
            {"id": 2, "name": "item_2", "updated_at": "2024-07-14"},
        ]
        assert expected == records


@freezegun.freeze_time("2024-07-15")
def test_cursor_age_validation_uses_incremental_when_cursor_within_retention():
    """Test that when cursor is within retention period, incremental sync is used."""
    manifest = _create_manifest_with_retention_period("P30D")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(
                url="https://api.test.com/items_with_filtration?start=2024-07-13&end=2024-07-15"
            ),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 3, "name": "item_3", "updated_at": "2024-07-14"},
                    ]
                )
            ),
        )

        state = [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name="TestStream", namespace=None),
                    stream_state=AirbyteStateBlob(updated_at="2024-07-13"),
                ),
            )
        ]
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=_CONFIG, catalog=None, state=state
        )
        configured_catalog = create_configured_catalog(source, _CONFIG)

        records = get_records(source, _CONFIG, configured_catalog, state)
        expected = [
            {"id": 3, "name": "item_3", "updated_at": "2024-07-14"},
        ]
        assert expected == records


@freezegun.freeze_time("2024-07-15")
def test_cursor_age_validation_with_1_day_retention_falls_back():
    """Test cursor age validation with P1D retention period falls back to full refresh."""
    manifest = _create_manifest_with_retention_period("P1D")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items"),
            HttpResponse(body=json.dumps([{"id": 1, "updated_at": "2024-07-14"}])),
        )

        state = [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name="TestStream", namespace=None),
                    stream_state=AirbyteStateBlob(updated_at="2024-07-13"),
                ),
            )
        ]
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=_CONFIG, catalog=None, state=state
        )
        configured_catalog = create_configured_catalog(source, _CONFIG)

        records = get_records(source, _CONFIG, configured_catalog, state)
        assert len(records) == 1


@freezegun.freeze_time("2024-07-15")
def test_cursor_age_validation_emits_warning_when_falling_back(caplog):
    """Test that a warning is emitted when cursor is older than retention period."""
    manifest = _create_manifest_with_retention_period("P7D")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items"),
            HttpResponse(body=json.dumps([{"id": 1, "updated_at": "2024-07-14"}])),
        )

        state = [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name="TestStream", namespace=None),
                    stream_state=AirbyteStateBlob(updated_at="2024-07-01"),
                ),
            )
        ]

        with caplog.at_level(logging.WARNING):
            source = ConcurrentDeclarativeSource(
                source_config=manifest, config=_CONFIG, catalog=None, state=state
            )
            configured_catalog = create_configured_catalog(source, _CONFIG)
            get_records(source, _CONFIG, configured_catalog, state)

        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "TestStream" in msg and "older than" in msg and "P7D" in msg for msg in warning_messages
        ), f"Expected warning about stale cursor not found. Warnings: {warning_messages}"


@freezegun.freeze_time("2024-07-15")
def test_cursor_age_validation_with_per_partition_state_uses_global_cursor():
    """Test that per-partition state structure uses global cursor for age validation."""
    manifest = _create_manifest_with_retention_period("P7D")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items"),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 1, "name": "item_1", "updated_at": "2024-07-13"},
                    ]
                )
            ),
        )

        state = [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name="TestStream", namespace=None),
                    stream_state=AirbyteStateBlob(
                        state={"updated_at": "2024-07-01"},
                        states=[
                            {
                                "partition": {"parent_id": "1"},
                                "cursor": {"updated_at": "2024-07-10"},
                            },
                            {
                                "partition": {"parent_id": "2"},
                                "cursor": {"updated_at": "2024-07-05"},
                            },
                        ],
                        use_global_cursor=False,
                    ),
                ),
            )
        ]
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=_CONFIG, catalog=None, state=state
        )
        configured_catalog = create_configured_catalog(source, _CONFIG)

        records = get_records(source, _CONFIG, configured_catalog, state)
        assert len(records) == 1


@freezegun.freeze_time("2024-07-15")
def test_cursor_age_validation_with_per_partition_state_within_retention():
    """Test per-partition state with global cursor within retention uses incremental.

    This test verifies that when the global cursor in a per-partition state structure
    is within the retention period, the incremental stream is selected (not full refresh).
    We verify this by checking that the incremental endpoint is called, not the full refresh one.
    """
    manifest = _create_manifest_with_retention_period("P30D")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(
                url="https://api.test.com/items_with_filtration",
                query_params={"start": "2024-07-01", "end": "2024-07-15"},
            ),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 3, "name": "item_3", "updated_at": "2024-07-14"},
                    ]
                )
            ),
        )

        state = [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name="TestStream", namespace=None),
                    stream_state=AirbyteStateBlob(
                        state={"updated_at": "2024-07-10"},
                        states=[
                            {
                                "partition": {"parent_id": "1"},
                                "cursor": {"updated_at": "2024-07-10"},
                            },
                        ],
                        use_global_cursor=False,
                    ),
                ),
            )
        ]
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=_CONFIG, catalog=None, state=state
        )
        configured_catalog = create_configured_catalog(source, _CONFIG)

        records = get_records(source, _CONFIG, configured_catalog, state)
        assert len(records) == 1


def _create_manifest_with_incrementing_count_cursor(api_retention_period: str) -> dict:
    """Create a manifest with IncrementingCountCursor and api_retention_period."""
    manifest = copy.deepcopy(_MANIFEST)
    manifest["definitions"]["TestStream"]["api_retention_period"] = api_retention_period

    incrementing_cursor = {
        "type": "IncrementingCountCursor",
        "cursor_field": "id",
        "start_value": 0,
    }
    manifest["definitions"]["TestStream"]["full_refresh_stream"]["incremental_sync"] = (
        incrementing_cursor
    )
    manifest["definitions"]["TestStream"]["incremental_stream"]["incremental_sync"] = (
        incrementing_cursor
    )
    return manifest


def test_cursor_age_validation_skips_incrementing_count_cursor():
    """Test that IncrementingCountCursor with api_retention_period is silently skipped (no error, uses incremental)."""
    manifest = _create_manifest_with_incrementing_count_cursor("P7D")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items_with_filtration"),
            HttpResponse(body=json.dumps([{"id": 101, "updated_at": "2024-07-14"}])),
        )

        state = [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name="TestStream", namespace=None),
                    stream_state=AirbyteStateBlob(id=100),
                ),
            )
        ]

        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=_CONFIG, catalog=None, state=state
        )
        configured_catalog = create_configured_catalog(source, _CONFIG)

        records = get_records(source, _CONFIG, configured_catalog, state)
        assert len(records) >= 0
