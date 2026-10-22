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

from airbyte_cdk.models import Status
from airbyte_cdk.sources.declarative.checks.check_stream import CheckStream
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.streams.core import Stream
from airbyte_cdk.sources.streams.http import HttpStream
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


def test_check_stream_names_can_be_overridden_from_config():
    static_stream = MagicMock(spec=Stream)
    static_stream.name = "static_stream"
    static_stream.availability_strategy = None
    selected_stream = MagicMock(spec=Stream)
    selected_stream.name = "selected_stream"
    selected_stream.availability_strategy = None
    selected_stream.read_records.return_value = iter([record])
    selected_stream.stream_slices.return_value = iter([{}])
    source = MagicMock()
    source.streams.return_value = [static_stream, selected_stream]

    check_stream = CheckStream(["static_stream"], parameters={})

    assert check_stream.check_connection(
        source, logger, {"__airbyte_check_stream_names": ["selected_stream"]}
    ) == (True, None)
    static_stream.stream_slices.assert_not_called()


def test_check_stream_names_override_empty_list_falls_back_to_manifest_streams():
    stream = MagicMock(spec=Stream)
    stream.name = "static_stream"
    stream.availability_strategy = None
    stream.read_records.return_value = iter([record])
    stream.stream_slices.return_value = iter([{}])
    source = MagicMock()
    source.streams.return_value = [stream]

    check_stream = CheckStream(["static_stream"], parameters={})

    assert check_stream.check_connection(source, logger, {"__airbyte_check_stream_names": []}) == (
        True,
        None,
    )
    stream.stream_slices.assert_called_once()


@pytest.mark.parametrize("override", ["selected_stream", [1], ["selected_stream", 1], None])
def test_check_stream_names_override_requires_list_of_strings(override):
    stream = MagicMock(spec=Stream)
    stream.name = "selected_stream"
    stream.availability_strategy = None
    source = MagicMock()
    source.streams.return_value = [stream]

    check_stream = CheckStream(["selected_stream"], parameters={})

    with pytest.raises(ValueError, match="__airbyte_check_stream_names must be a list of strings."):
        check_stream.check_connection(source, logger, {"__airbyte_check_stream_names": override})


def test_check_stream_names_override_rejects_unknown_stream():
    stream = MagicMock(spec=Stream)
    stream.name = "selected_stream"
    stream.availability_strategy = None
    source = MagicMock()
    source.streams.return_value = [stream]

    check_stream = CheckStream(["selected_stream"], parameters={})

    with pytest.raises(ValueError, match="unknown_stream is not part of the catalog."):
        check_stream.check_connection(
            source, logger, {"__airbyte_check_stream_names": ["unknown_stream"]}
        )


def test_check_stream_names_override_returns_unavailable_stream_message():
    stream = MagicMock(spec=Stream)
    stream.name = "selected_stream"
    stream.availability_strategy = None
    stream.stream_slices.return_value = iter([])
    source = MagicMock()
    source.streams.return_value = [stream]

    check_stream = CheckStream(["other_stream"], parameters={})

    stream_is_available, reason = check_stream.check_connection(
        source, logger, {"__airbyte_check_stream_names": ["selected_stream"]}
    )
    assert not stream_is_available
    assert "no stream slices were found, likely because the parent stream is empty" in reason


def test_check_stream_names_override_validates_before_stream_discovery():
    source = MagicMock()
    check_stream = CheckStream(["selected_stream"], parameters={})

    with pytest.raises(ValueError, match="__airbyte_check_stream_names must be a list of strings."):
        check_stream.check_connection(
            source, logger, {"__airbyte_check_stream_names": "selected_stream"}
        )

    source.streams.assert_not_called()


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
            Status.SUCCEEDED,
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
            Status.SUCCEEDED,
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
            Status.SUCCEEDED,
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
            Status.SUCCEEDED,
            False,
            200,
            [],
            1,
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
            Status.SUCCEEDED,
            False,
            200,
            [],
            1,
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
            Status.SUCCEEDED,
            False,
            200,
            [],
            1,
            id="test_stream_count_gt_generated_streams",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "http_dynamic_stream",
                        },
                    ],
                }
            },
            Status.SUCCEEDED,
            False,
            200,
            [],
            1,
            id="test_stream_count_unset_checks_all_streams",
        ),
        pytest.param(
            {
                "check": {
                    "type": "CheckStream",
                    "dynamic_streams_check_configs": [
                        {
                            "type": "DynamicStreamCheckConfig",
                            "dynamic_stream_name": "http_dynamic_stream",
                        },
                    ],
                }
            },
            Status.FAILED,
            False,
            404,
            ["Not found. The requested resource was not found on the server."],
            0,
            id="test_stream_count_unset_failed",
        ),
        pytest.param(
            {"check": {"type": "CheckStream", "stream_names": ["non_existent_stream"]}},
            Status.FAILED,
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
            Status.FAILED,
            False,
            200,
            [],
            0,
            id="test_non_existent_dynamic_stream",
        ),
        pytest.param(
            {"check": {"type": "CheckStream", "stream_names": ["static_stream"]}},
            Status.FAILED,
            False,
            404,
            ["Not found. The requested resource was not found on the server."],
            0,
            id="test_stream_unavailable_unhandled_error",
        ),
        pytest.param(
            {"check": {"type": "CheckStream", "stream_names": ["static_stream"]}},
            Status.FAILED,
            False,
            403,
            ["Forbidden. You don't have permission to access this resource."],
            0,
            id="test_stream_unavailable_handled_error",
        ),
        pytest.param(
            {"check": {"type": "CheckStream", "stream_names": ["static_stream"]}},
            Status.FAILED,
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
            Status.FAILED,
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
            Status.FAILED,
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
            Status.FAILED,
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
                source.check(logger, _CONFIG)
        else:
            connection_status = source.check(logger, _CONFIG)
            http_mocker.assert_number_of_calls(item_request_2, request_count)
            assert connection_status.status == expected_result


def test_check_empty_static_stream_override_falls_back_to_manifest_streams_and_checks_dynamic_streams():
    manifest = {
        **deepcopy(_MANIFEST_WITHOUT_CHECK_COMPONENT),
        **{
            "check": {
                "type": "CheckStream",
                "stream_names": ["static_stream"],
                "dynamic_streams_check_configs": [
                    {
                        "type": "DynamicStreamCheckConfig",
                        "dynamic_stream_name": "http_dynamic_stream",
                    },
                ],
            }
        },
    }
    check_config = {**_CONFIG, "__airbyte_check_stream_names": []}

    with HttpMocker() as http_mocker:
        static_stream_request = HttpRequest(url="https://api.test.com/static")
        static_stream_response = HttpResponse(body=json.dumps([]), status_code=500)
        http_mocker.get(static_stream_request, static_stream_response)

        items_request = HttpRequest(url="https://api.test.com/items")
        items_response = HttpResponse(
            body=json.dumps([{"id": 1, "name": "item_1"}, {"id": 2, "name": "item_2"}])
        )
        http_mocker.get(items_request, items_response)

        item_request_1 = HttpRequest(url="https://api.test.com/items/1")
        item_response = HttpResponse(body=json.dumps([]), status_code=200)
        http_mocker.get(item_request_1, item_response)

        item_request_2 = HttpRequest(url="https://api.test.com/items/2")
        item_response = HttpResponse(body=json.dumps([]), status_code=200)
        http_mocker.get(item_request_2, item_response)

        source = ConcurrentDeclarativeSource(
            source_config=manifest,
            config=check_config,
            catalog=None,
            state=None,
        )

        connection_status = source.check(logger, check_config)

        http_mocker.assert_number_of_calls(static_stream_request, 6)
        http_mocker.assert_number_of_calls(item_request_2, 0)
        assert connection_status.status == Status.FAILED


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


@pytest.mark.parametrize(
    "stream_count",
    [pytest.param(0, id="zero"), pytest.param(-1, id="negative")],
)
def test_check_stream_non_positive_stream_count(stream_count: int) -> None:
    """A ValidationError is raised when stream_count is less than 1."""
    manifest = {
        **deepcopy(_MANIFEST_WITHOUT_CHECK_COMPONENT),
        **{
            "check": {
                "type": "CheckStream",
                "dynamic_streams_check_configs": [
                    {
                        "type": "DynamicStreamCheckConfig",
                        "dynamic_stream_name": "http_dynamic_stream",
                        "stream_count": stream_count,
                    }
                ],
            }
        },
    }
    with pytest.raises(ValidationError):
        ConcurrentDeclarativeSource(
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
        source.check(logger, _CONFIG)


def _stream_config(name: str, path: Optional[str] = None) -> dict:
    return {
        "type": "DeclarativeStream",
        "name": name,
        "primary_key": [],
        "schema_loader": {
            "type": "InlineSchemaLoader",
            "schema": {"type": "object", "properties": {}},
        },
        "retriever": {
            "type": "SimpleRetriever",
            "requester": {
                "type": "HttpRequester",
                "url_base": "https://api.test.com",
                "path": path or f"/{name}",
                "http_method": "GET",
            },
            "record_selector": {
                "type": "RecordSelector",
                "extractor": {"type": "DpathExtractor", "field_path": []},
            },
            "paginator": {"type": "NoPagination"},
        },
    }


def _source_with_streams(
    stream_configs: list[dict],
    stream_names: list[str],
    config: Optional[dict] = None,
) -> ConcurrentDeclarativeSource:
    return ConcurrentDeclarativeSource(
        source_config={
            "version": "6.7.0",
            "type": "DeclarativeSource",
            "streams": deepcopy(stream_configs),
            "check": {"type": "CheckStream", "stream_names": stream_names},
        },
        config=config or {},
        catalog=None,
        state=None,
    )


def _created_stream_names(create_component_spy) -> list[str]:
    return [
        call.args[1]["name"]
        for call in create_component_spy.call_args_list
        if len(call.args) > 1
        and getattr(call.args[0], "__name__", "") in {"DeclarativeStream", "StateDelegatingStream"}
    ]


def test_check_builds_only_the_named_static_stream(mocker):
    source = _source_with_streams(
        [_stream_config("first"), _stream_config("selected"), _stream_config("third")],
        ["selected"],
    )
    create_component_spy = mocker.spy(source._constructor, "create_component")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/selected"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )
        assert source.check(logger, {}).status == Status.SUCCEEDED

    assert _created_stream_names(create_component_spy) == ["selected"]


def test_check_does_not_build_an_unread_stream_that_fails_to_build(mocker):
    streams = [_stream_config("checked"), _stream_config("unread_bad")]
    source = _source_with_streams(streams, ["checked"])

    def patch_bad_stream(source):
        original_create_component = source._constructor.create_component

        def create_component(model, component_config, *args, **kwargs):
            if (
                getattr(model, "__name__", "") in {"DeclarativeStream", "StateDelegatingStream"}
                and component_config["name"] == "unread_bad"
            ):
                raise ValueError("bad unread stream")
            return original_create_component(model, component_config, *args, **kwargs)

        mocker.patch.object(source._constructor, "create_component", side_effect=create_component)

    patch_bad_stream(source)
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/checked"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )
        assert source.check(logger, {}).status == Status.SUCCEEDED

    failing_source = _source_with_streams(streams, ["unread_bad"])
    patch_bad_stream(failing_source)
    status = failing_source.check(logger, {})
    assert status.status == Status.FAILED
    assert "Encountered an error while discovering streams" in status.message


@pytest.mark.parametrize("condition", [True, False])
def test_check_stream_name_inside_conditional_streams(condition, mocker):
    manifest = {
        "version": "6.7.0",
        "type": "DeclarativeSource",
        "streams": [
            {
                "type": "ConditionalStreams",
                "condition": "{{ config['include_conditional'] }}",
                "streams": [_stream_config("conditional_stream")],
            },
            _stream_config("other_stream"),
        ],
        "check": {"type": "CheckStream", "stream_names": ["conditional_stream"]},
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config={"include_conditional": condition},
        catalog=None,
        state=None,
    )

    if condition:
        create_component_spy = mocker.spy(source._constructor, "create_component")
        with HttpMocker() as http_mocker:
            http_mocker.get(
                HttpRequest(url="https://api.test.com/conditional_stream"),
                HttpResponse(body=json.dumps([{"id": 1}])),
            )
            assert source.check(logger, source._config).status == Status.SUCCEEDED
        assert _created_stream_names(create_component_spy) == ["conditional_stream"]
    else:
        with pytest.raises(ValueError, match="conditional_stream is not part of the catalog"):
            source.check(logger, source._config)


def test_check_stream_names_can_be_overridden_from_config_for_concurrent_source(mocker):
    source = _source_with_streams(
        [_stream_config("default_stream"), _stream_config("selected_stream")],
        ["default_stream"],
        config={"__airbyte_check_stream_names": ["selected_stream"]},
    )
    create_component_spy = mocker.spy(source._constructor, "create_component")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/selected_stream"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )
        assert source.check(logger, source._config).status == Status.SUCCEEDED

    assert _created_stream_names(create_component_spy) == ["selected_stream"]


def test_check_with_misspelled_stream_name_uses_catalog_error():
    source = _source_with_streams(
        [_stream_config("first"), _stream_config("second")], ["misspelled"]
    )

    with pytest.raises(
        ValueError,
        match=r"misspelled is not part of the catalog\. Expected one of \['first', 'second'\]\.",
    ):
        source.check(logger, {})


def test_check_for_dynamic_stream_name_falls_back_to_streams(mocker):
    manifest = deepcopy(_MANIFEST_WITHOUT_CHECK_COMPONENT)
    manifest["dynamic_streams"] = [manifest["dynamic_streams"][1]]
    manifest["check"] = {"type": "CheckStream", "stream_names": ["item_3"]}
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config={**_CONFIG, "api_key": "test"},
        catalog=None,
        state=None,
    )
    streams_spy = mocker.spy(source, "streams")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/static"),
            HttpResponse(body=json.dumps([])),
        )
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items/3"),
            HttpResponse(body=json.dumps([])),
        )
        assert source.check(logger, source._config).status == Status.SUCCEEDED

    streams_spy.assert_called_once()


def test_check_dynamic_stream_configs_are_evaluated_once_when_not_checked(mocker):
    manifest = deepcopy(_MANIFEST_WITHOUT_CHECK_COMPONENT)
    manifest["dynamic_streams"] = [manifest["dynamic_streams"][1]]
    manifest["check"] = {"type": "CheckStream", "stream_names": ["static_stream"]}
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config={**_CONFIG, "api_key": "test"},
        catalog=None,
        state=None,
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/static"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )
        dynamic_configs_spy = mocker.spy(source, "_dynamic_stream_configs")
        assert source.check(logger, source._config).status == Status.SUCCEEDED

    dynamic_configs_spy.assert_called_once()


def test_check_builds_a_named_state_delegating_stream(mocker):
    stream = _stream_config("state_stream")
    incremental_stream = deepcopy(stream)
    incremental_stream["incremental_sync"] = {
        "type": "DatetimeBasedCursor",
        "start_datetime": "2024-01-01T00:00:00Z",
        "end_datetime": "2024-01-02T00:00:00Z",
        "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
        "cursor_datetime_formats": ["%Y-%m-%dT%H:%M:%SZ"],
        "cursor_field": "updated_at",
    }
    state_delegating_stream = {
        "type": "StateDelegatingStream",
        "name": "state_stream",
        "full_refresh_stream": stream,
        "incremental_stream": incremental_stream,
    }
    source = _source_with_streams([state_delegating_stream], ["state_stream"])
    create_component_spy = mocker.spy(source._constructor, "create_component")

    streams = source.streams_by_name(["state_stream"])

    assert streams is not None
    assert [stream.name for stream in streams] == ["state_stream"]
    assert _created_stream_names(create_component_spy) == ["state_stream"]
    assert any(
        component_call.args
        and getattr(component_call.args[0], "__name__", "") == "StateDelegatingStream"
        for component_call in create_component_spy.call_args_list
    )


def test_check_still_fails_when_spec_config_validations_fail():
    source_config = {
        "version": "6.7.0",
        "type": "DeclarativeSource",
        "streams": [_stream_config("selected")],
        "check": {"type": "CheckStream", "stream_names": ["selected"]},
        "spec": {
            "type": "Spec",
            "connection_specification": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {"settings": {"type": "object"}},
            },
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "validations": [
                    {
                        "type": "DpathValidator",
                        "field_path": ["settings"],
                        "validation_strategy": {
                            "type": "ValidateAdheresToSchema",
                            "base_schema": {
                                "type": "object",
                                "properties": {"mode": {"type": "string", "enum": ["sync"]}},
                                "required": ["mode"],
                                "additionalProperties": False,
                            },
                        },
                    }
                ],
            },
        },
    }
    source = ConcurrentDeclarativeSource(
        source_config=source_config,
        config={"settings": {"mode": "invalid"}},
        catalog=None,
        state=None,
    )

    status = source.check(logger, source._config)

    assert status.status == Status.FAILED
    assert "Encountered an error while discovering streams" in status.message


def test_check_stream_uses_streams_for_legacy_manifest_source():
    from airbyte_cdk.legacy.sources.declarative.manifest_declarative_source import (
        ManifestDeclarativeSource,
    )

    source = MagicMock(spec=ManifestDeclarativeSource)
    stream = MagicMock(spec=Stream)
    stream.name = "selected"
    source.streams.return_value = [stream]
    checker = CheckStream(stream_names=["selected"], parameters={})

    assert checker._build_streams(source, {}, ["selected"]) == [stream]

    source.streams.assert_called_once_with(config={})
