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
                    "path": "parent/{{ stream_partition.parent_id }}/items",
                    "http_method": "GET",
                    "authenticator": {
                        "type": "ApiKeyAuthenticator",
                        "header": "apikey",
                        "api_token": "{{ config['api_key'] }}",
                    },
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": ["data"]},
                },
                "paginator": {
                    "type": "DefaultPaginator",
                    "page_token_option": {
                        "type": "RequestOption",
                        "inject_into": "request_parameter",
                        "field_name": "starting_after",
                    },
                    "pagination_strategy": {
                        "type": "CursorPagination",
                        "cursor_value": '{{ response["data"][-1]["id"] }}',
                        "stop_condition": '{{ not response.get("has_more", False) }}',
                    },
                },
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [
                        {
                            "type": "ParentStreamConfig",
                            "parent_key": "id",
                            "partition_field": "parent_id",
                            "lazy_read_pointer": ["items"],
                            "stream": {
                                "type": "DeclarativeStream",
                                "name": "parent",
                                "retriever": {
                                    "type": "SimpleRetriever",
                                    "requester": {
                                        "type": "HttpRequester",
                                        "url_base": "https://api.test.com",
                                        "path": "/parents",
                                        "http_method": "GET",
                                        "authenticator": {
                                            "type": "ApiKeyAuthenticator",
                                            "header": "apikey",
                                            "api_token": "{{ config['api_key'] }}",
                                        },
                                    },
                                    "record_selector": {
                                        "type": "RecordSelector",
                                        "extractor": {
                                            "type": "DpathExtractor",
                                            "field_path": ["data"],
                                        },
                                    },
                                },
                                "schema_loader": {
                                    "type": "InlineSchemaLoader",
                                    "schema": {
                                        "$schema": "http://json-schema.org/schema#",
                                        "properties": {"id": {"type": "integer"}},
                                        "type": "object",
                                    },
                                },
                            },
                        }
                    ],
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
def test_retriever_with_lazy_reading():
    """Test the lazy loading behavior of the SimpleRetriever with paginated substream data."""
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/parents"),
            HttpResponse(
                body=json.dumps(
                    {
                        "data": [
                            {
                                "id": 1,
                                "name": "parent_1",
                                "updated_at": "2024-07-13",
                                "items": {
                                    "data": [
                                        {"id": 1, "updated_at": "2024-07-13"},
                                        {"id": 2, "updated_at": "2024-07-13"},
                                    ],
                                    "has_more": True,
                                },
                            },
                            {
                                "id": 2,
                                "name": "parent_2",
                                "updated_at": "2024-07-13",
                                "items": {
                                    "data": [
                                        {"id": 3, "updated_at": "2024-07-13"},
                                        {"id": 4, "updated_at": "2024-07-13"},
                                    ],
                                    "has_more": False,
                                },
                            },
                        ],
                        "has_more": False,
                    }
                )
            ),
        )

        http_mocker.get(
            HttpRequest(
                url="https://api.test.com/parent/1/items?starting_after=2&start=2024-07-01&end=2024-07-15"
            ),
            HttpResponse(
                body=json.dumps(
                    {
                        "data": [
                            {"id": 5, "updated_at": "2024-07-13"},
                            {"id": 6, "updated_at": "2024-07-13"},
                        ],
                        "has_more": True,
                    }
                )
            ),
        )

        http_mocker.get(
            HttpRequest(
                url="https://api.test.com/parent/1/items?starting_after=6&start=2024-07-01&end=2024-07-15"
            ),
            HttpResponse(
                body=json.dumps(
                    {"data": [{"id": 7, "updated_at": "2024-07-13"}], "has_more": False}
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
            {"id": 1, "updated_at": "2024-07-13"},
            {"id": 2, "updated_at": "2024-07-13"},
            {"id": 3, "updated_at": "2024-07-13"},
            {"id": 4, "updated_at": "2024-07-13"},
            {"id": 5, "updated_at": "2024-07-13"},
            {"id": 6, "updated_at": "2024-07-13"},
            {"id": 7, "updated_at": "2024-07-13"},
        ]

        assert all(record in expected_full for record in full_records)


@freezegun.freeze_time("2024-07-15")
def test_incremental_sync_with_state():
    """Test incremental sync behavior using state to fetch only new records."""
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/parents"),
            HttpResponse(
                body=json.dumps(
                    {
                        "data": [
                            {
                                "id": 1,
                                "name": "parent_1",
                                "updated_at": "2024-07-13",
                                "items": {
                                    "data": [
                                        {"id": 1, "updated_at": "2024-07-13"},
                                        {"id": 2, "updated_at": "2024-07-13"},
                                    ],
                                    "has_more": False,
                                },
                            },
                            {
                                "id": 2,
                                "name": "parent_2",
                                "updated_at": "2024-07-13",
                                "items": {
                                    "data": [
                                        {"id": 3, "updated_at": "2024-07-13"},
                                        {"id": 4, "updated_at": "2024-07-13"},
                                    ],
                                    "has_more": False,
                                },
                            },
                        ],
                        "has_more": False,
                    }
                )
            ),
        )

        http_mocker.get(
            HttpRequest(url="https://api.test.com/parent/1/items?start=2024-07-13&end=2024-07-15"),
            HttpResponse(
                body=json.dumps(
                    {"data": [{"id": 10, "updated_at": "2024-07-13"}], "has_more": False}
                )
            ),
        )
        http_mocker.get(
            HttpRequest(url="https://api.test.com/parent/2/items?start=2024-07-13&end=2024-07-15"),
            HttpResponse(
                body=json.dumps(
                    {"data": [{"id": 11, "updated_at": "2024-07-13"}], "has_more": False}
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
            {"id": 10, "updated_at": "2024-07-13"},
            {"id": 11, "updated_at": "2024-07-13"},
        ]
        assert all(record in expected_incremental for record in incremental_records)
