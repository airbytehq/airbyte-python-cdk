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


_CONFIG_DRIVEN_PATH_CONFIG = {"resource": "sync"}

_MANIFEST_WITH_CONFIG_DRIVEN_PATH = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["items"]},
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "items",
            "primary_key": "id",
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url": "https://api.test.com/{{ config['resource'] }}",
                    "http_method": "GET",
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
                "paginator": {"type": "NoPagination"},
            },
        }
    ],
}


def _source_with_check_component(check_component):
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = check_component
    return ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )


def test_given_no_config_overrides_when_check_then_components_use_the_user_config():
    source = _source_with_check_component({"type": "CheckStream", "stream_names": ["items"]})

    with HttpMocker() as http_mocker:
        # Only the user-configured path is mocked, so a request to any other path fails the test.
        http_mocker.get(
            HttpRequest(url="https://api.test.com/sync"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED


def test_given_config_overrides_when_check_then_components_built_during_check_see_them():
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "check-only"},
        }
    )

    with HttpMocker() as http_mocker:
        # Only the overridden path is mocked. Reaching this endpoint proves the overlay was applied to
        # the stream the checker built, and not merely stored on the source.
        overridden_request = HttpRequest(url="https://api.test.com/check-only")
        http_mocker.get(overridden_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED
        http_mocker.assert_number_of_calls(overridden_request, 1)


def test_given_config_overrides_when_check_then_the_config_is_restored_afterwards():
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "check-only"},
        }
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/check-only"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED

    assert source._config == _CONFIG_DRIVEN_PATH_CONFIG

    # A sync that follows a check in the same process must go back to the user's value.
    with HttpMocker() as http_mocker:
        sync_request = HttpRequest(url="https://api.test.com/sync")
        http_mocker.get(sync_request, HttpResponse(body=json.dumps([{"id": 1}])))

        stream = source.streams(_CONFIG_DRIVEN_PATH_CONFIG)[0]
        assert stream.check_availability().is_available

        http_mocker.assert_number_of_calls(sync_request, 1)


def test_given_config_overrides_when_check_raises_then_the_config_is_restored():
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["not_in_the_catalog"],
            "config_overrides": {"resource": "check-only"},
        }
    )

    with pytest.raises(ValueError):
        source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)

    assert source._config == _CONFIG_DRIVEN_PATH_CONFIG


def test_given_config_overrides_when_check_then_config_validations_run_against_the_user_config():
    """An override is authored in the manifest, so it must not be held to validations written for the
    user's own input - the user has no way to satisfy them."""
    config = {"resource": "sync", "settings": {"mode": "sync"}}
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only", "settings": {"mode": "check-only"}},
    }
    manifest["spec"] = {
        "type": "Spec",
        "connection_specification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "resource": {"type": "string"},
                "settings": {"type": "object"},
            },
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
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "type": "object",
                            "properties": {"mode": {"type": "string", "enum": ["sync"]}},
                            "required": ["mode"],
                            "additionalProperties": False,
                        },
                    },
                }
            ],
        },
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=config,
        catalog=None,
        state=None,
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/check-only"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        # `settings.mode` is overridden to a value outside the validator's enum, so this would fail were
        # the overlay validated instead of the config the user supplied.
        assert source.check(logger, config).status == Status.SUCCEEDED


def test_given_config_overrides_when_check_then_values_are_not_interpolated():
    """Pins the verbatim contract. A value containing `{{ }}` reaches components as that literal string,
    so turning interpolation on later is a deliberate, test-breaking decision rather than a silent
    reinterpretation of overrides already written."""
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "{{config['resource']}}"},
        }
    )

    with HttpMocker() as http_mocker:
        literal_request = HttpRequest(url="https://api.test.com/%7B%7Bconfig%5B'resource'%5D%7D%7D")
        http_mocker.get(literal_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED
        http_mocker.assert_number_of_calls(literal_request, 1)


_MANIFEST_WITH_CONFIG_DRIVEN_DYNAMIC_STREAM = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckDynamicStream", "stream_count": 1},
    "streams": [],
    "dynamic_streams": [
        {
            "type": "DynamicDeclarativeStream",
            "name": "dynamic_items",
            "stream_template": {
                "type": "DeclarativeStream",
                "name": "",
                "primary_key": [],
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "http://json-schema.org/schema#",
                        "type": "object",
                        "properties": {"id": {"type": "integer"}},
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url": "https://api.test.com/{{ config['resource'] }}",
                        "http_method": "GET",
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
                    }
                ],
            },
        }
    ],
}


def test_given_config_overrides_on_check_dynamic_stream_then_components_see_them():
    """The overlay is read from the raw check definition, so it is checker-agnostic. Without this test a
    refactor moving the read into `create_check_stream` would silently drop `CheckDynamicStream`."""
    config = {"resource": "sync", "custom_streams": [{"name": "items"}]}
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_DYNAMIC_STREAM)
    manifest["check"] = {
        "type": "CheckDynamicStream",
        "stream_count": 1,
        "config_overrides": {"resource": "check-only"},
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=config,
        catalog=None,
        state=None,
    )

    with HttpMocker() as http_mocker:
        overridden_request = HttpRequest(url="https://api.test.com/check-only")
        http_mocker.get(overridden_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, config).status == Status.SUCCEEDED
        http_mocker.assert_number_of_calls(overridden_request, 1)


_OAUTH_WITH_REFRESH_TOKEN_UPDATER = {
    "type": "OAuthAuthenticator",
    "token_refresh_endpoint": "https://api.test.com/oauth/token",
    "client_id": "{{ config['credentials']['client_id'] }}",
    "client_secret": "{{ config['credentials']['client_secret'] }}",
    "refresh_token": "{{ config['credentials']['refresh_token'] }}",
    "refresh_token_updater": {"type": "RefreshTokenUpdater", "refresh_token_name": "refresh_token"},
}


def _manifest_with_refresh_token_updater(check_component, refresh_token_updater=None):
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = check_component
    authenticator = deepcopy(_OAUTH_WITH_REFRESH_TOKEN_UPDATER)
    if refresh_token_updater is not None:
        authenticator["refresh_token_updater"] = deepcopy(refresh_token_updater)
    manifest["streams"][0]["retriever"]["requester"]["authenticator"] = authenticator
    return manifest


@pytest.mark.parametrize(
    "refresh_token_updater",
    [
        pytest.param(
            {"type": "RefreshTokenUpdater", "refresh_token_name": "refresh_token"}, id="populated"
        ),
        # Every field of `RefreshTokenUpdater` has a default, so an empty mapping is a valid way to take
        # all of them. It builds the same single-use authenticator a populated one does, and the
        # transformer injects no `type` into it, so it stays falsy - a truthiness test would miss it.
        pytest.param({}, id="empty-taking-all-defaults"),
    ],
)
def test_given_refresh_token_updater_when_config_overrides_then_manifest_is_rejected(
    refresh_token_updater,
):
    """A `refresh_token_updater` emits the whole config it was handed as a CONNECTOR_CONFIG control
    message, which the platform persists - so a check-only override would become the connection's saved
    config. The restore cannot recall a message already on stdout, so the combination is refused."""
    source = ConcurrentDeclarativeSource(
        source_config=_manifest_with_refresh_token_updater(
            {
                "type": "CheckStream",
                "stream_names": ["items"],
                "config_overrides": {"resource": "check-only"},
            },
            refresh_token_updater=refresh_token_updater,
        ),
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    with pytest.raises(ValueError, match="refresh_token_updater"):
        source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)


def test_given_no_refresh_token_updater_when_config_overrides_then_manifest_is_accepted():
    """The scan must not reject every OAuth manifest. The same authenticator without the updater writes
    nothing back, so the overlay is allowed."""
    manifest = _manifest_with_refresh_token_updater(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "check-only"},
        }
    )
    del manifest["streams"][0]["retriever"]["requester"]["authenticator"]["refresh_token_updater"]

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert source._manifest_writes_back_config(source._source_config) is False


def test_given_spec_property_named_refresh_token_updater_then_overrides_are_allowed():
    """The scan walks the raw manifest looking for the key anywhere, because an authenticator reached
    through a `$ref` is only found under `definitions`. A connector whose spec happens to declare a
    config field by that name must not be caught by that net - nothing in `spec` is a component."""
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only"},
    }
    manifest["spec"] = {
        "type": "Spec",
        "connection_specification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "resource": {"type": "string"},
                "refresh_token_updater": {"type": "string"},
            },
        },
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert source._manifest_writes_back_config(source._source_config) is False

    with HttpMocker() as http_mocker:
        overridden_request = HttpRequest(url="https://api.test.com/check-only")
        http_mocker.get(overridden_request, HttpResponse(body=json.dumps([{"id": 1}])))

        assert source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG).status == Status.SUCCEEDED


def test_given_override_of_a_field_named_refresh_token_updater_then_it_is_allowed():
    """`config_overrides` holds config values, not components, so a key that collides with the
    authenticator field name is just a config field and must not trip the guard."""
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only", "refresh_token_updater": "check-only"},
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert source._manifest_writes_back_config(source._source_config) is False


def test_given_refresh_token_updater_behind_a_ref_then_manifest_is_rejected():
    """The coarse walk is what makes a `$ref`-ed authenticator detectable at all: the raw manifest holds
    only the reference, and the authenticator itself sits under `definitions`. Narrowing the walk must
    not lose that."""
    manifest = _manifest_with_refresh_token_updater(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "check-only"},
        }
    )
    manifest["definitions"] = {
        "authenticator": manifest["streams"][0]["retriever"]["requester"]["authenticator"]
    }
    manifest["streams"][0]["retriever"]["requester"]["authenticator"] = {
        "$ref": "#/definitions/authenticator"
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    with pytest.raises(ValueError, match="refresh_token_updater"):
        source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)


def test_given_refresh_token_updater_without_config_overrides_then_nothing_is_rejected():
    """The rejection is scoped to the feature. A manifest that does not use `config_overrides` keeps
    working with a `refresh_token_updater` exactly as before, even though the manifest scan detects it."""
    source = ConcurrentDeclarativeSource(
        source_config=_manifest_with_refresh_token_updater(
            {"type": "CheckStream", "stream_names": ["items"]}
        ),
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    assert source._manifest_writes_back_config(source.resolved_manifest) is True

    # No overrides means no overlay, so the guard must not fire and the config must not be copied.
    with source._config_overridden_for_check(None):
        assert source._config is _CONFIG_DRIVEN_PATH_CONFIG


def test_given_config_overrides_when_check_then_overridden_keys_are_logged_without_values(caplog):
    """An override may name a secret field, so the log records which keys were overridden but never what
    they were set to."""
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"resource": "s3cr3t-value"},
        }
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/s3cr3t-value"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        with caplog.at_level(logging.INFO):
            source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)

    assert "Overriding config keys for the check operation: resource" in caplog.text
    assert "s3cr3t-value" not in caplog.text


def test_given_override_key_absent_from_the_spec_then_a_warning_is_logged(caplog):
    manifest = deepcopy(_MANIFEST_WITH_CONFIG_DRIVEN_PATH)
    manifest["check"] = {
        "type": "CheckStream",
        "stream_names": ["items"],
        "config_overrides": {"resource": "check-only", "typoed_key": 1},
    }
    manifest["spec"] = {
        "type": "Spec",
        "connection_specification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"resource": {"type": "string"}},
        },
    }
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config=_CONFIG_DRIVEN_PATH_CONFIG,
        catalog=None,
        state=None,
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/check-only"),
            HttpResponse(body=json.dumps([{"id": 1}])),
        )

        with caplog.at_level(logging.WARNING):
            source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)

    assert "typoed_key" in caplog.text
    assert "not declared in the connector spec" in caplog.text
    assert "'resource'" not in caplog.text


def test_given_an_airbyte_reserved_override_key_then_the_manifest_is_rejected():
    """`__airbyte`-prefixed keys are the platform's channel into the config - `CheckStream` reads
    `__airbyte_check_stream_names` from the very config the overlay writes to. The feature refuses to
    touch that namespace rather than leaving it available."""
    source = _source_with_check_component(
        {
            "type": "CheckStream",
            "stream_names": ["items"],
            "config_overrides": {"__airbyte_check_stream_names": ["something_else"]},
        }
    )

    with pytest.raises(ValueError, match="__airbyte_check_stream_names"):
        source.check(logger, _CONFIG_DRIVEN_PATH_CONFIG)
