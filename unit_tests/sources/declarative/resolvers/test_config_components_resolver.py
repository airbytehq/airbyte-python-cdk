#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.models import (
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Type,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


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
    "custom_streams": [
        {"id": 1, "name": "item_1"},
        {"id": 2, "name": "item_2"},
    ],
}

_CONFIG_WITH_STREAM_DUPLICATION = {
    "start_date": "2024-07-01T00:00:00.000Z",
    "custom_streams": [
        {"id": 1, "name": "item_1"},
        {"id": 2, "name": "item_2"},
        {"id": 3, "name": "item_2"},
    ],
}

_MANIFEST = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["Rates"]},
    "dynamic_streams": [
        {
            "type": "DynamicDeclarativeStream",
            "stream_template": {
                "type": "DeclarativeStream",
                "name": "",
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
                        "$parameters": {"item_id": ""},
                        "url_base": "https://api.test.com",
                        "path": "/items/{{parameters['item_id']}}",
                        "http_method": "GET",
                        "authenticator": {
                            "type": "ApiKeyAuthenticator",
                            "header": "apikey",
                            "api_token": "{{ config['api_key'] }}",
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "paginator": {"type": "NoPagination"},
                },
            },
            "components_resolver": {
                "type": "ConfigComponentsResolver",
                "stream_config": {
                    "type": "StreamConfig",
                    "configs_pointer": ["custom_streams"],
                },
                "components_mapping": [
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": ["name"],
                        "value": "{{components_values['name']}}",
                    },
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": [
                            "retriever",
                            "requester",
                            "$parameters",
                            "item_id",
                        ],
                        "value": "{{components_values['id']}}",
                    },
                ],
            },
        }
    ],
}


@pytest.mark.parametrize(
    "config, expected_exception, expected_stream_names",
    [
        (_CONFIG, None, ["item_1", "item_2"]),
        (
            _CONFIG_WITH_STREAM_DUPLICATION,
            "Dynamic streams list contains a duplicate name: item_2. Please check your configuration.",
            None,
        ),
    ],
)
def test_dynamic_streams_read_with_config_components_resolver(
    config, expected_exception, expected_stream_names
):
    if expected_exception:
        with pytest.raises(AirbyteTracedException) as exc_info:
            source = ConcurrentDeclarativeSource(
                source_config=_MANIFEST, config=config, catalog=None, state=None
            )
            source.discover(logger=source.logger, config=config)
        assert str(exc_info.value) == expected_exception
    else:
        source = ConcurrentDeclarativeSource(
            source_config=_MANIFEST, config=config, catalog=None, state=None
        )

        actual_catalog = source.discover(logger=source.logger, config=config)

        configured_streams = [
            to_configured_stream(stream, primary_key=stream.source_defined_primary_key)
            for stream in actual_catalog.streams
        ]
        configured_catalog = to_configured_catalog(configured_streams)

        with HttpMocker() as http_mocker:
            http_mocker.get(
                HttpRequest(url="https://api.test.com/items/1"),
                HttpResponse(body=json.dumps({"id": "1", "name": "item_1"})),
            )
            http_mocker.get(
                HttpRequest(url="https://api.test.com/items/2"),
                HttpResponse(body=json.dumps({"id": "2", "name": "item_2"})),
            )

            records = [
                message.record
                for message in source.read(MagicMock(), config, configured_catalog)
                if message.type == Type.RECORD
            ]

        assert len(actual_catalog.streams) == len(expected_stream_names)
        assert [stream.name for stream in actual_catalog.streams] == expected_stream_names
        assert len(records) == len(expected_stream_names)
        assert [record.stream for record in records] == expected_stream_names
