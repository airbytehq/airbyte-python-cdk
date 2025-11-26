#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
from copy import deepcopy
from datetime import date as dt_date
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

# Manifest with component definition with value that fails when trying
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

# Manifest with component definition with value containing tab characters
# which would cause YAML ScannerError (tabs cannot start tokens in YAML)
_MANIFEST_WITH_TAB_SCANNER_ERROR = deepcopy(_MANIFEST)
_MANIFEST_WITH_TAB_SCANNER_ERROR["dynamic_streams"][0]["components_resolver"][
    "components_mapping"
].append(
    {
        "type": "ComponentMappingDefinition",
        "create_or_update": True,
        "field_path": ["retriever", "requester", "$parameters", "custom_query"],
        "value": "SELECT\n\tcampaign.name,\n\tcampaign.id\nFROM campaign",  # Contains tab characters
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
        (_MANIFEST_WITH_TAB_SCANNER_ERROR, _CONFIG, None, ["item_1", "item_2", "default_item"]),
    ],
    ids=["no_duplicates", "duplicates", "stream_configs_list", "scanner_error", "tab_scanner_error"],
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


# Manifest with condition that always evaluates to true
_MANIFEST_WITH_TRUE_CONDITION = deepcopy(_MANIFEST)
_MANIFEST_WITH_TRUE_CONDITION["dynamic_streams"][0]["components_resolver"][
    "components_mapping"
].append(
    {
        "type": "ComponentMappingDefinition",
        "field_path": ["retriever", "requester", "$parameters", "always_included"],
        "value": "true_condition_value",
        "create_or_update": True,
        "condition": "{{ True }}",
    }
)

# Manifest with condition that always evaluates to false
_MANIFEST_WITH_FALSE_CONDITION = deepcopy(_MANIFEST)
_MANIFEST_WITH_FALSE_CONDITION["dynamic_streams"][0]["components_resolver"][
    "components_mapping"
].append(
    {
        "type": "ComponentMappingDefinition",
        "field_path": ["retriever", "requester", "$parameters", "never_included"],
        "value": "false_condition_value",
        "create_or_update": True,
        "condition": "{{ False }}",
    }
)

# Manifest with condition using components_values that evaluates to true for some items
_MANIFEST_WITH_COMPONENTS_VALUES_TRUE_CONDITION = deepcopy(_MANIFEST)
_MANIFEST_WITH_COMPONENTS_VALUES_TRUE_CONDITION["dynamic_streams"][0]["components_resolver"][
    "components_mapping"
].append(
    {
        "type": "ComponentMappingDefinition",
        "field_path": ["retriever", "requester", "$parameters", "conditional_param"],
        "value": "item_1_special_value",
        "create_or_update": True,
        "condition": "{{ components_values['name'] == 'item_1' }}",
    }
)

# Manifest with condition using components_values that evaluates to false for all items
_MANIFEST_WITH_COMPONENTS_VALUES_FALSE_CONDITION = deepcopy(_MANIFEST)
_MANIFEST_WITH_COMPONENTS_VALUES_FALSE_CONDITION["dynamic_streams"][0]["components_resolver"][
    "components_mapping"
].append(
    {
        "type": "ComponentMappingDefinition",
        "field_path": ["retriever", "requester", "$parameters", "never_matching"],
        "value": "never_applied_value",
        "create_or_update": True,
        "condition": "{{ components_values['name'] == 'non_existent_item' }}",
    }
)

# Manifest with multiple conditions - some true, some false
_MANIFEST_WITH_MIXED_CONDITIONS = deepcopy(_MANIFEST)
_MANIFEST_WITH_MIXED_CONDITIONS["dynamic_streams"][0]["components_resolver"][
    "components_mapping"
].extend(
    [
        {
            "type": "ComponentMappingDefinition",
            "field_path": ["retriever", "requester", "$parameters", "always_true"],
            "value": "always_applied",
            "create_or_update": True,
            "condition": "{{ True }}",
        },
        {
            "type": "ComponentMappingDefinition",
            "field_path": ["retriever", "requester", "$parameters", "always_false"],
            "value": "never_applied",
            "create_or_update": True,
            "condition": "{{ False }}",
        },
        {
            "type": "ComponentMappingDefinition",
            "field_path": ["retriever", "requester", "$parameters", "item_specific"],
            "value": "applied_to_item_2",
            "create_or_update": True,
            "condition": "{{ components_values['id'] == 2 }}",
        },
    ]
)


@pytest.mark.parametrize(
    "manifest, config, expected_conditional_params",
    [
        (
            _MANIFEST_WITH_TRUE_CONDITION,
            _CONFIG,
            {
                "item_1": {"always_included": "true_condition_value", "item_id": 1},
                "item_2": {"always_included": "true_condition_value", "item_id": 2},
                "default_item": {"always_included": "true_condition_value", "item_id": 4},
            },
        ),
        (
            _MANIFEST_WITH_FALSE_CONDITION,
            _CONFIG,
            {
                "item_1": {"item_id": 1},  # never_included should not be present
                "item_2": {"item_id": 2},
                "default_item": {"item_id": 4},
            },
        ),
        (
            _MANIFEST_WITH_COMPONENTS_VALUES_TRUE_CONDITION,
            _CONFIG,
            {
                "item_1": {"conditional_param": "item_1_special_value", "item_id": 1},
                "item_2": {"item_id": 2},  # condition false for item_2
                "default_item": {"item_id": 4},  # condition false for default_item
            },
        ),
        (
            _MANIFEST_WITH_COMPONENTS_VALUES_FALSE_CONDITION,
            _CONFIG,
            {
                "item_1": {"item_id": 1},  # never_matching should not be present
                "item_2": {"item_id": 2},
                "default_item": {"item_id": 4},
            },
        ),
        (
            _MANIFEST_WITH_MIXED_CONDITIONS,
            _CONFIG,
            {
                "item_1": {"always_true": "always_applied", "item_id": 1},
                "item_2": {
                    "always_true": "always_applied",
                    "item_specific": "applied_to_item_2",
                    "item_id": 2,
                },
                "default_item": {"always_true": "always_applied", "item_id": 4},
            },
        ),
    ],
    ids=[
        "true_condition",
        "false_condition",
        "components_values_true_condition",
        "components_values_false_condition",
        "mixed_conditions",
    ],
)
def test_component_mapping_conditions(manifest, config, expected_conditional_params):
    """Test that ComponentMappingDefinition conditions work correctly for various scenarios."""
    source = ConcurrentDeclarativeSource(
        source_config=manifest, config=config, catalog=None, state=None
    )

    for stream in source.streams(config):
        if stream.name in expected_conditional_params:
            assert (
                stream._stream_partition_generator._partition_factory._retriever.requester._parameters
                == expected_conditional_params[stream.name]
            )


_MANIFEST_WITH_VALUE_TYPE_STR = deepcopy(_MANIFEST)
_MANIFEST_WITH_VALUE_TYPE_STR["dynamic_streams"][0]["components_resolver"][
    "components_mapping"
].extend(
    [
        {
            "type": "ComponentMappingDefinition",
            "field_path": ["retriever", "requester", "$parameters", "as_string"],
            "value": "true",  # valid YAML, but we want to keep it as *string*
            "value_type": "string",
            "create_or_update": True,
        },
        {
            "type": "ComponentMappingDefinition",
            "field_path": ["retriever", "requester", "$parameters", "as_yaml"],
            "value": "true",  # no value_type -> should be parsed to boolean True
            "create_or_update": True,
        },
        {
            "type": "ComponentMappingDefinition",
            "field_path": ["retriever", "requester", "$parameters", "json_string"],
            "value": "[1, 2]",  # valid YAML/JSON-looking text; keep as string
            "value_type": "string",
            "create_or_update": True,
        },
        {
            "type": "ComponentMappingDefinition",
            "field_path": ["retriever", "requester", "$parameters", "json_parsed"],
            "value": "[1, 2]",  # no value_type -> should parse to a list
            "create_or_update": True,
        },
        {
            "type": "ComponentMappingDefinition",
            "field_path": ["retriever", "requester", "$parameters", "date_as_string"],
            "value": "2024-07-10",  # date-like text that YAML would parse; force keep as string
            "value_type": "string",
            "create_or_update": True,
        },
        {
            "type": "ComponentMappingDefinition",
            "field_path": ["retriever", "requester", "$parameters", "date_yaml_parsed"],
            "value": "2024-07-10",  # no value_type -> YAML should parse to datetime.date
            "create_or_update": True,
        },
    ]
)


def test_value_type_str_avoids_yaml_parsing():
    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST_WITH_VALUE_TYPE_STR, config=_CONFIG, catalog=None, state=None
    )

    for stream in source.streams(_CONFIG):
        params = (
            stream._stream_partition_generator._partition_factory._retriever.requester._parameters
        )

        # Confirm the usual param is still present
        assert "item_id" in params

        # value_type="string" -> keep as string
        assert "as_string" in params
        assert isinstance(params["as_string"], str)
        assert params["as_string"] == "true"

        assert "json_string" in params
        assert isinstance(params["json_string"], str)
        assert params["json_string"] == "[1, 2]"

        # No value_type -> YAML parsed
        assert "as_yaml" in params
        assert isinstance(params["as_yaml"], bool)
        assert params["as_yaml"] is True

        assert "json_parsed" in params
        assert isinstance(params["json_parsed"], list)
        assert params["json_parsed"] == [1, 2]

        # value_type="string" -> remains a plain string
        assert "date_as_string" in params
        assert isinstance(params["date_as_string"], str)
        assert params["date_as_string"] == "2024-07-10"

        # no value_type -> YAML parses to datetime.date
        assert "date_yaml_parsed" in params
        assert isinstance(params["date_yaml_parsed"], dt_date)
        assert params["date_yaml_parsed"].isoformat() == "2024-07-10"
