import json
from unittest.mock import MagicMock

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.models import (
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Type,
    SyncMode,
)
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse

MANIFEST = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["Rates"]},
    "dynamic_streams": [
        {
            "type": "DynamicDeclarativeStream",
            "stream_template": {
                "type": "DeclarativeStream",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "http://json-schema.org/schema#",
                        "type": "object",
                        "properties": {
                            "field1": {"type": "string"},
                            "field2": {"type": "string"},
                        },
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://api.test.com",
                        "path": "/search/{{parameters['object_name']}}",
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
                "type": "ParametrizedComponentsResolver",
                "stream_parameters": {
                    "type": "StreamParametersDefinition",
                    "lisf_of_parameters_for_stream": [
                        {
                            "parameters": {"object_name": "Customers"},
                            "name": "Customers",
                            "transformations": [
                                {
                                    "type": "AddFields",
                                    "fields": [
                                        {
                                            "type": "AddedFieldDefinition",
                                            "path": ["field2"],
                                            "value": "Related to customers field",
                                        }
                                    ],
                                }
                            ],
                        },
                        {
                            "parameters": {"object_name": "Refunds"},
                            "name": "Refunds",
                            "transformations": [
                                {
                                    "type": "AddFields",
                                    "fields": [
                                        {
                                            "type": "AddedFieldDefinition",
                                            "path": ["field2"],
                                            "value": "Related to refunds field",
                                        }
                                    ],
                                }
                            ],
                        },
                        {
                            "parameters": {"object_name": "Orders"},
                            "name": "Orders",
                            "transformations": [
                                {
                                    "type": "AddFields",
                                    "fields": [
                                        {
                                            "type": "AddedFieldDefinition",
                                            "path": ["field2"],
                                            "value": "Related to orders field",
                                        }
                                    ],
                                }
                            ],
                        },
                    ],
                },
                "components_mapping": [
                    {
                        "type": "ComponentMappingDefinition",
                        "create_or_update": True,
                        "field_path": ["$parameters"],
                        "value": "{{components_values['parameters']}}",
                    },
                    {
                        "type": "ComponentMappingDefinition",
                        "create_or_update": True,
                        "field_path": ["name"],
                        "value": "{{components_values['name']}}",
                    },
                    {
                        "type": "ComponentMappingDefinition",
                        "create_or_update": True,
                        "field_path": ["transformations"],
                        "value": "{{components_values['transformations']}}",
                    },
                ],
            },
        }
    ],
}


def test_dynamic_streams_with_parametrized_components_resolver():
    config = {"api_key": "test"}
    source = ConcurrentDeclarativeSource(
        source_config=MANIFEST, config=config, catalog=None, state=None
    )
    discover_result = source.discover(config=config, logger=MagicMock())
    assert len(discover_result.streams) == 3

    configured_streams = []
    for stream in discover_result.streams:
        configured_stream = ConfiguredAirbyteStream(
            stream=stream,
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.overwrite,
        )
        configured_streams.append(configured_stream)
    configured_catalog = ConfiguredAirbyteCatalog(streams=configured_streams)

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/search/Customers"),
            HttpResponse(body=json.dumps({"field1": "Customers info"})),
        )
        http_mocker.get(
            HttpRequest(url="https://api.test.com/search/Orders"),
            HttpResponse(body=json.dumps({"field1": "Orders info"})),
        )
        http_mocker.get(
            HttpRequest(url="https://api.test.com/search/Refunds"),
            HttpResponse(body=json.dumps({"field1": "Refunds info"})),
        )

        records = [
            record
            for record in source.read(MagicMock(), config, configured_catalog)
            if record.type == Type.RECORD
        ]

    assert len(records) == 3
    assert [record.record.data for record in records] == [
        {"field1": "Customers info", "field2": "Related to customers field"},
        {"field1": "Refunds info", "field2": "Related to refunds field"},
        {"field1": "Orders info", "field2": "Related to orders field"},
    ]
