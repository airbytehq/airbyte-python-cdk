#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
from copy import deepcopy
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

SCHEMA = {
    "$schema": "http://json-schema.org/schema#",
    "type": "object",
    "properties": {
        "ABC": {"type": "number"},
        "AED": {"type": "number"},
    },
}

REQUESTER = {
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
}

RETRIEVER = {
    "type": "SimpleRetriever",
    "requester": REQUESTER,
    "record_selector": {
        "type": "RecordSelector",
        "extractor": {"type": "DpathExtractor", "field_path": []},
    },
    "paginator": {"type": "NoPagination"},
}

STREAM_TEMPLATE = {
    "type": "DeclarativeStream",
    "primary_key": [],
    "schema_loader": {
        "type": "InlineSchemaLoader",
        "schema": SCHEMA,
    },
    "retriever": RETRIEVER,
}

COMPONENTS_MAPPING = [
    {
        "type": "ComponentMappingDefinition",
        "field_path": ["name"],
        "create_or_update": True,
        "value": "{{components_values['name']}}",
    },
    {
        "type": "ComponentMappingDefinition",
        "field_path": ["retriever", "requester", "$parameters", "item_id"],
        "value": "{{components_values['id']}}",
    },
]

STREAM_CONFIG = {
    "type": "StreamConfig",
    "configs_pointer": ["custom_streams"],
    "default_values": [{"id": 4, "name": "default_item"}],
}

_MANIFEST = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["Rates"]},
    "dynamic_streams": [
        {
            "type": "DynamicDeclarativeStream",
            "stream_template": STREAM_TEMPLATE,
            "components_resolver": {
                "type": "ConfigComponentsResolver",
                "stream_config": STREAM_CONFIG,
                "components_mapping": COMPONENTS_MAPPING,
            },
        }
    ],
}

# Manifest with a placeholder for custom stream config list override
_MANIFEST_WITH_STREAM_CONFIGS_LIST = deepcopy(_MANIFEST)
_MANIFEST_WITH_STREAM_CONFIGS_LIST["dynamic_streams"][0]["components_resolver"]["stream_config"] = [
    STREAM_CONFIG
]

# Manifest with component definition with value that is fails when trying
# to parse yaml in _parse_yaml_if_possible but generally contains valid string
_MANIFEST_WITH_SCANNER_ERROR = deepcopy(_MANIFEST)
_MANIFEST_WITH_SCANNER_ERROR["dynamic_streams"][0]["components_resolver"][
    "components_mapping"
].append(
    {
        "type": "ComponentMappingDefinition",
        "create_or_update": True,
        "field_path": ["retriever", "requester", "$parameters", "cursor_format"],
        "value": "{{ '%Y-%m-%d' if components_values['name'] == 'default_item' else '%Y-%m-%dT%H:%M:%S' }}",
    }
)


@pytest.mark.parametrize(
    "manifest, config, expected_exception, expected_stream_names",
    [
        (_MANIFEST, _CONFIG, None, ["item_1", "item_2", "default_item"]),
        (
            _MANIFEST,
            _CONFIG_WITH_STREAM_DUPLICATION,
            "Dynamic streams list contains a duplicate name: item_2. Please check your configuration.",
            None,
        ),
        (_MANIFEST_WITH_STREAM_CONFIGS_LIST, _CONFIG, None, ["item_1", "item_2", "default_item"]),
        (_MANIFEST_WITH_SCANNER_ERROR, _CONFIG, None, ["item_1", "item_2", "default_item"]),
    ],
)
def test_dynamic_streams_read_with_config_components_resolver(
    manifest, config, expected_exception, expected_stream_names
):
    if expected_exception:
        with pytest.raises(AirbyteTracedException) as exc_info:
            source = ConcurrentDeclarativeSource(
                source_config=manifest, config=config, catalog=None, state=None
            )
            source.discover(logger=source.logger, config=config)
        assert str(exc_info.value) == expected_exception
    else:
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=config, catalog=None, state=None
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
            http_mocker.get(
                HttpRequest(url="https://api.test.com/items/4"),
                HttpResponse(body=json.dumps({"id": "4", "name": "default_item"})),
            )

            records = [
                message.record
                for message in source.read(MagicMock(), config, configured_catalog)
                if message.type == Type.RECORD
            ]

        assert len(actual_catalog.streams) == len(expected_stream_names)
        # Use set comparison to avoid relying on deterministic ordering
        assert set(stream.name for stream in actual_catalog.streams) == set(expected_stream_names)
        assert len(records) == len(expected_stream_names)
        # Use set comparison to avoid relying on deterministic ordering
        assert set(record.stream for record in records) == set(expected_stream_names)
