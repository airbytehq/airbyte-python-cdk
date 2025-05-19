#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
from unittest.mock import MagicMock

from airbyte_cdk.models import (ConfiguredAirbyteCatalog,
                                ConfiguredAirbyteStream, DestinationSyncMode,
                                Type)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import \
    ConcurrentDeclarativeSource
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse


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


_CONFIG = {
    "start_date": "2024-07-01T00:00:00.000Z",
    "api_key": "dummy_api_key",
}


_MANIFEST = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["Rates"]},
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "test_stream",
            "primary_key": [],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "properties": {
                        "ABC": {"type": "number"},
                        "AED": {"type": "number"},
                    },
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
                    "authenticator": {
                        "type": "ApiKeyAuthenticator",
                        "header": "apikey",
                        "api_token": "{{ config['api_key'] }}",
                    },
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {
                        "type": "KeyValueExtractor",
                        "keys_extractor": {
                            "type": "DpathExtractor",
                            "field_path": ["dimensions", "names"],
                        },
                        "values_extractor": {
                            "type": "DpathExtractor",
                            "field_path": ["dimensions", "values"],
                        },
                    },
                },
                "paginator": {"type": "NoPagination"},
            },
        }
    ],
}


def test_key_value_extractor():
    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=None, state=None
    )

    actual_catalog = source.discover(logger=source.logger, config=_CONFIG)

    configured_streams = [
        to_configured_stream(stream, primary_key=stream.source_defined_primary_key)
        for stream in actual_catalog.streams
    ]
    configured_catalog = to_configured_catalog(configured_streams)

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items"),
            HttpResponse(
                body=json.dumps(
                    {
                        "dimensions": {
                            "names": ["customer_segment", "traffic_source"],
                            "values": ["enterprise", "organic_search"],
                        }
                    }
                )
            ),
        )

        records = [
            message.record
            for message in source.read(MagicMock(), _CONFIG, configured_catalog)
            if message.type == Type.RECORD
        ]

    assert len(records) == 1
    assert records[0].data == {"customer_segment": "enterprise", "traffic_source": "organic_search"}
