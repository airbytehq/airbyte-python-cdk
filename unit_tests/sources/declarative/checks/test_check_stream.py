#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging
from copy import deepcopy
from typing import Any, Iterable, Mapping, Optional
from unittest.mock import MagicMock

import pytest
import requests
from jsonschema.exceptions import ValidationError

from airbyte_cdk.sources.declarative.checks.check_stream import CheckStream
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.core import Stream
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse

logger = logging.getLogger("test")
config = dict()

stream_names = ["s1"]
record = MagicMock()


@pytest.mark.parametrize(
    "test_name, record, streams_to_check, stream_slice, expectation",
    [
        ("test_success_check", record, stream_names, {}, (True, None)),
        (
            "test_success_check_stream_slice",
            record,
            stream_names,
            {"slice": "slice_value"},
            (True, None),
        ),
        ("test_fail_check", None, stream_names, {}, (True, None)),
        ("test_try_to_check_invalid stream", record, ["invalid_stream_name"], {}, None),
    ],
)
@pytest.mark.parametrize("slices_as_list", [True, False])
def test_check_stream_with_slices_as_list(
    test_name, record, streams_to_check, stream_slice, expectation, slices_as_list
):
    stream = MagicMock(spec=Stream)
    stream.name = "s1"
    stream.availability_strategy = None
    if slices_as_list:
        stream.stream_slices.return_value = [stream_slice]
    else:
        stream.stream_slices.return_value = iter([stream_slice])

    stream.read_records.side_effect = mock_read_records({frozenset(stream_slice): iter([record])})

    source = MagicMock()
    source.streams.return_value = [stream]

    check_stream = CheckStream(streams_to_check, parameters={})

    if expectation:
        actual = check_stream.check_connection(source, logger, config)
        assert actual == expectation
    else:
        with pytest.raises(ValueError):
            check_stream.check_connection(source, logger, config)


def mock_read_records(responses, default_response=None, **kwargs):
    return (
        lambda stream_slice, sync_mode: responses[frozenset(stream_slice)]
        if frozenset(stream_slice) in responses
        else default_response
    )


def test_check_empty_stream():
    stream = MagicMock(spec=Stream)
    stream.name = "s1"
    stream.read_records.return_value = iter([])
    stream.stream_slices.return_value = iter([None])

    source = MagicMock()
    source.streams.return_value = [stream]

    check_stream = CheckStream(["s1"], parameters={})
    stream_is_available, reason = check_stream.check_connection(source, logger, config)
    assert stream_is_available


def test_check_stream_with_no_stream_slices_aborts():
    stream = MagicMock(spec=Stream)
    stream.name = "s1"
    stream.stream_slices.return_value = iter([])

    source = MagicMock()
    source.streams.return_value = [stream]

    check_stream = CheckStream(["s1"], parameters={})
    stream_is_available, reason = check_stream.check_connection(source, logger, config)
    assert not stream_is_available
    assert "no stream slices were found, likely because the parent stream is empty" in reason


@pytest.mark.parametrize(
    "test_name, response_code, available_expectation, expected_messages",
    [
        (
            "test_stream_unavailable_unhandled_error",
            404,
            False,
            ["Not found. The requested resource was not found on the server."],
        ),
        (
            "test_stream_unavailable_handled_error",
            403,
            False,
            ["Forbidden. You don't have permission to access this resource."],
        ),
        ("test_stream_available", 200, True, []),
    ],
)
def test_check_http_stream_via_availability_strategy(
    mocker, test_name, response_code, available_expectation, expected_messages
):
    class MockHttpStream(HttpStream):
        url_base = "https://test_base_url.com"
        primary_key = ""

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.resp_counter = 1

        def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
            return None

        def path(self, **kwargs) -> str:
            return ""

        def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
            stub_resp = {"data": self.resp_counter}
            self.resp_counter += 1
            yield stub_resp

        pass

    http_stream = MockHttpStream()
    assert isinstance(http_stream, HttpStream)

    source = MagicMock()
    source.streams.return_value = [http_stream]

    check_stream = CheckStream(stream_names=["mock_http_stream"], parameters={})

    req = requests.Response()
    req.status_code = response_code
    mocker.patch.object(requests.Session, "send", return_value=req)

    logger = logging.getLogger(f"airbyte.{getattr(source, 'name', '')}")
    stream_is_available, reason = check_stream.check_connection(source, logger, config)

    assert stream_is_available == available_expectation
    for message in expected_messages:
        assert message in reason


_CONFIG = {
    "start_date": "2024-07-01T00:00:00.000Z",
    "custom_streams": [
        {"id": 3, "name": "item_3"},
        {"id": 4, "name": "item_4"},
    ],
}

_MANIFEST_WITHOUT_CHECK_COMPONENT = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "dynamic_streams": [
        {
            "type": "DynamicDeclarativeStream",
            "name": "http_dynamic_stream",
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
        },
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
        },
    ],
    "streams": [
        {
            "type": "DeclarativeStream",
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "$parameters": {"item_id": ""},
                    "url_base": "https://api.test.com",
                    "path": "/static",
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
            "name": "static_stream",
            "primary_key": "id",
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                    },
                    "type": "object",
                },
            },
        }
    ],
}


@pytest.mark.parametrize(
    "check_component, expected_result, expectation, response_code, expected_messages, request_count",
    [
        pytest.param(
            {"check": {"type": "CheckStream", "stream_names": ["static_stream"]}},
            True,
            False,
            200,
            [{"id": 1, "name": "static_1"}, {"id": 2, "name": "static_2"}],
            0,
            id="test_check_only_static_streams",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "stream_names": ["static_stream"],
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "http_dynamic_stream",
                            "stream_count": 1,
                        }
                    ],
                }
            },
            True,
            False,
            200,
            [],
            0,
            id="test_check_static_streams_and_http_dynamic_stream",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "stream_names": ["static_stream"],
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "dynamic_stream_1",
                            "stream_count": 1,
                        }
                    ],
                }
            },
            True,
            False,
            200,
            [],
            0,
            id="test_check_static_streams_and_config_dynamic_stream",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "dynamic_stream_1",
                            "stream_count": 1,
                        },
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "http_dynamic_stream",
                        },
                    ],
                }
            },
            True,
            False,
            200,
            [],
            0,
            id="test_check_http_dynamic_stream_and_config_dynamic_stream",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "stream_names": ["static_stream"],
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "dynamic_stream_1",
                            "stream_count": 1,
                        },
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "http_dynamic_stream",
                        },
                    ],
                }
            },
            True,
            False,
            200,
            [],
            0,
            id="test_check_static_streams_and_http_dynamic_stream_and_config_dynamic_stream",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "http_dynamic_stream",
                            "stream_count": 1000,
                        },
                    ],
                }
            },
            True,
            False,
            200,
            [],
            1,
            id="test_stream_count_gt_generated_streams",
        ),
        pytest.param(
            {"check": {"type": "CheckStream", "stream_names": ["non_existent_stream"]}},
            False,
            True,
            200,
            [],
            0,
            id="test_non_existent_static_stream",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "unknown_dynamic_stream",
                            "stream_count": 1,
                        }
                    ],
                }
            },
            False,
            False,
            200,
            [],
            0,
            id="test_non_existent_dynamic_stream",
        ),
        pytest.param(
            {"check": {"type": "CheckStream", "stream_names": ["static_stream"]}},
            False,
            False,
            404,
            ["Not found. The requested resource was not found on the server."],
            0,
            id="test_stream_unavailable_unhandled_error",
        ),
        pytest.param(
            {"check": {"type": "CheckStream", "stream_names": ["static_stream"]}},
            False,
            False,
            403,
            ["Forbidden. You don't have permission to access this resource."],
            0,
            id="test_stream_unavailable_handled_error",
        ),
        pytest.param(
            {"check": {"type": "CheckStream", "stream_names": ["static_stream"]}},
            False,
            False,
            401,
            ["Unauthorized. Please ensure you are authenticated correctly."],
            0,
            id="test_stream_unauthorized_error",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "dynamic_stream_1",
                            "stream_count": 1,
                        },
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "http_dynamic_stream",
                        },
                    ],
                }
            },
            False,
            False,
            404,
            ["Not found. The requested resource was not found on the server."],
            0,
            id="test_dynamic_stream_unavailable_unhandled_error",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "dynamic_stream_1",
                            "stream_count": 1,
                        },
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "http_dynamic_stream",
                        },
                    ],
                }
            },
            False,
            False,
            403,
            ["Forbidden. You don't have permission to access this resource."],
            0,
            id="test_dynamic_stream_unavailable_handled_error",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "dynamic_stream_1",
                            "stream_count": 1,
                        },
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "http_dynamic_stream",
                        },
                    ],
                }
            },
            False,
            False,
            401,
            ["Unauthorized. Please ensure you are authenticated correctly."],
            0,
            id="test_dynamic_stream_unauthorized_error",
        ),
    ],
)
def test_check_stream1(
    check_component, expected_result, expectation, response_code, expected_messages, request_count
):
    manifest = {**deepcopy(_MANIFEST_WITHOUT_CHECK_COMPONENT), **check_component}

    with HttpMocker() as http_mocker:
        static_stream_request = HttpRequest(url="https://api.test.com/static")
        static_stream_response = HttpResponse(
            body=json.dumps(expected_messages), status_code=response_code
        )
        http_mocker.get(static_stream_request, static_stream_response)

        items_request = HttpRequest(url="https://api.test.com/items")
        items_response = HttpResponse(
            body=json.dumps([{"id": 1, "name": "item_1"}, {"id": 2, "name": "item_2"}])
        )
        http_mocker.get(items_request, items_response)

        item_request_1 = HttpRequest(url="https://api.test.com/items/1")
        item_response = HttpResponse(body=json.dumps(expected_messages), status_code=response_code)
        http_mocker.get(item_request_1, item_response)

        item_request_2 = HttpRequest(url="https://api.test.com/items/2")
        item_response = HttpResponse(body=json.dumps(expected_messages), status_code=response_code)
        http_mocker.get(item_request_2, item_response)

        item_request_3 = HttpRequest(url="https://api.test.com/items/3")
        item_response = HttpResponse(body=json.dumps(expected_messages), status_code=response_code)
        http_mocker.get(item_request_3, item_response)

        source = ConcurrentDeclarativeSource(
            source_config=manifest,
            config=_CONFIG,
            catalog=None,
            state=None,
        )
        if expectation:
            with pytest.raises(ValueError):
                source.check_connection(logger, _CONFIG)
        else:
            stream_is_available, reason = source.check_connection(logger, _CONFIG)
            http_mocker.assert_number_of_calls(item_request_2, request_count)
            assert stream_is_available == expected_result


def test_check_stream_missing_fields():
    """Test if ValueError is raised when dynamic_streams_check_configs is missing required fields."""
    manifest = {
        **deepcopy(_MANIFEST_WITHOUT_CHECK_COMPONENT),
        **{
            "check": {
                "type": "CheckStream",
                "dynamic_streams_check_configs": [{"type": "DynamicStreamCheckConfig"}],
            }
        },
    }
    with pytest.raises(ValidationError):
        source = ConcurrentDeclarativeSource(
            source_config=manifest,
            config=_CONFIG,
            catalog=None,
            state=None,
        )


def test_check_stream_only_type_provided():
    manifest = {**deepcopy(_MANIFEST_WITHOUT_CHECK_COMPONENT), **{"check": {"type": "CheckStream"}}}
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG,
        catalog=None,
        state=None,
    )
    with pytest.raises(ValueError):
        source.check_connection(logger, _CONFIG)
