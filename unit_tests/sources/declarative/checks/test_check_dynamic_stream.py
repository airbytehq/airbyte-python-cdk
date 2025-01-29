#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
import logging
from copy import deepcopy

import pytest

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse

logger = logging.getLogger("test")

_CONFIG = {"start_date": "2024-07-01T00:00:00.000Z"}

_MANIFEST = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckDynamicStream", "stream_count": 1},
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
                "type": "HttpComponentsResolver",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://api.test.com",
                        "path": "items",
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
    "response_code, available_expectation, use_check_availability, expected_messages",
    [
        pytest.param(
            404,
            False,
            True,
            ["Not found. The requested resource was not found on the server."],
            id="test_stream_unavailable_unhandled_error",
        ),
        pytest.param(
            403,
            False,
            True,
            ["Forbidden. You don't have permission to access this resource."],
            id="test_stream_unavailable_handled_error",
        ),
        pytest.param(200, True, True, [], id="test_stream_available"),
        pytest.param(200, True, False, [], id="test_stream_available"),
        pytest.param(
            401,
            False,
            True,
            ["Unauthorized. Please ensure you are authenticated correctly."],
            id="test_stream_unauthorized_error",
        ),
    ],
)
def test_check_dynamic_stream(
    response_code, available_expectation, use_check_availability, expected_messages
):
    manifest = deepcopy(_MANIFEST)

    with HttpMocker() as http_mocker:
        items_request = HttpRequest(url="https://api.test.com/items")
        items_response = HttpResponse(
            body=json.dumps([{"id": 1, "name": "item_1"}, {"id": 2, "name": "item_2"}])
        )
        http_mocker.get(items_request, items_response)

        item_request = HttpRequest(url="https://api.test.com/items/1")
        item_response = HttpResponse(body=json.dumps(expected_messages), status_code=response_code)
        item_request_count = 1
        http_mocker.get(item_request, item_response)

        if not use_check_availability:
            manifest["check"]["use_check_availability"] = False
            item_request_count = 0  # stream only created and data request not called

        source = ConcurrentDeclarativeSource(
            source_config=manifest,
            config=_CONFIG,
            catalog=None,
            state=None,
        )

        stream_is_available, reason = source.check_connection(logger, _CONFIG)

        http_mocker.assert_number_of_calls(item_request, item_request_count)

    assert stream_is_available == available_expectation
    for message in expected_messages:
        assert message in reason
