#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
from unittest.mock import MagicMock

import freezegun

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
