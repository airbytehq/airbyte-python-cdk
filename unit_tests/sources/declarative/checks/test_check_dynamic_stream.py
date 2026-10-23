#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
import logging
from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.models import Status
from airbyte_cdk.sources.declarative.checks.check_dynamic_stream import CheckDynamicStream
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
            Status.FAILED,
            True,
            ["Not found. The requested resource was not found on the server."],
            id="test_stream_unavailable_unhandled_error",
        ),
        pytest.param(
            403,
            Status.FAILED,
            True,
            ["Forbidden. You don't have permission to access this resource."],
            id="test_stream_unavailable_handled_error",
        ),
        pytest.param(200, Status.SUCCEEDED, True, [], id="test_stream_available"),
        pytest.param(200, Status.SUCCEEDED, False, [], id="test_stream_available"),
        pytest.param(
            401,
            Status.FAILED,
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

        connection_status = source.check(logger, _CONFIG)

        http_mocker.assert_number_of_calls(item_request, item_request_count)

    assert connection_status.status == available_expectation
    for message in expected_messages:
        assert message in connection_status.message


def _static_stream(name: str) -> dict:
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
                "path": f"/{name}",
                "http_method": "GET",
            },
            "record_selector": {
                "type": "RecordSelector",
                "extractor": {"type": "DpathExtractor", "field_path": []},
            },
            "paginator": {"type": "NoPagination"},
        },
    }


@pytest.mark.parametrize(
    "stream_count, expected_stream_names",
    [
        pytest.param(1, ["first_static"], id="one-stream"),
        pytest.param(3, ["first_static", "second_static", "item_1"], id="three-streams"),
    ],
)
def test_check_dynamic_stream_builds_only_the_first_stream_count_in_order(
    mocker, stream_count, expected_stream_names
):
    manifest = deepcopy(_MANIFEST)
    manifest["streams"] = [_static_stream("first_static"), _static_stream("second_static")]
    manifest["check"]["stream_count"] = stream_count
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config={**_CONFIG, "api_key": "test"},
        catalog=None,
        state=None,
    )
    create_component_spy = mocker.spy(source._constructor, "create_component")

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items"),
            HttpResponse(
                body=json.dumps([{"id": 1, "name": "item_1"}, {"id": 2, "name": "item_2"}])
            ),
        )
        for stream_name in ("first_static", "second_static"):
            http_mocker.get(
                HttpRequest(url=f"https://api.test.com/{stream_name}"),
                HttpResponse(body=json.dumps([])),
            )
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items/1"),
            HttpResponse(body=json.dumps([])),
        )

        assert source.check(logger, source._config).status == Status.SUCCEEDED

    created_stream_names = [
        component_call.args[1]["name"]
        for component_call in create_component_spy.call_args_list
        if len(component_call.args) > 1
        and getattr(component_call.args[0], "__name__", "")
        in {"DeclarativeStream", "StateDelegatingStream"}
    ]
    assert created_stream_names == expected_stream_names


def test_check_dynamic_stream_uses_streams_override_for_concurrent_source():
    class StreamsOverrideSource(ConcurrentDeclarativeSource):
        def __init__(self, *args, **kwargs):
            self.streams_call_count = 0
            super().__init__(*args, **kwargs)

        def streams(self, config):
            self.streams_call_count += 1
            if config.get("end_date") == "":
                config.pop("end_date")
            return super().streams(config=config)

    stream = _static_stream("selected")
    stream["incremental_sync"] = {
        "type": "DatetimeBasedCursor",
        "start_datetime": "2024-01-01",
        "end_datetime": "{{ config.get('end_date', today_utc()) }}",
        "datetime_format": "%Y-%m-%d",
        "cursor_datetime_formats": ["%Y-%m-%d"],
        "cursor_field": "updated_at",
    }
    source = StreamsOverrideSource(
        source_config={
            "version": "6.7.0",
            "type": "DeclarativeSource",
            "streams": [stream],
            "check": {"type": "CheckDynamicStream", "stream_count": 1},
        },
        config={"end_date": ""},
        catalog=None,
        state=None,
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/selected"),
            HttpResponse(body=json.dumps([])),
        )
        assert source.check(logger, source._config).status == Status.SUCCEEDED

    assert source.streams_call_count == 1


@pytest.mark.parametrize(
    "stream_count,use_check_availability",
    [(0, True), (1, False)],
)
def test_check_dynamic_stream_uses_streams_for_non_positive_count_or_disabled_availability(
    stream_count, use_check_availability
):
    source = MagicMock(spec=ConcurrentDeclarativeSource)
    source.streams.return_value = []
    checker = CheckDynamicStream(
        stream_count=stream_count,
        parameters={},
        use_check_availability=use_check_availability,
    )

    checker.check_connection(source, logger, {})

    source.streams.assert_called_once_with(config={})
    source._first_streams.assert_not_called()


def test_check_dynamic_stream_uses_streams_for_legacy_manifest_source():
    from airbyte_cdk.legacy.sources.declarative.manifest_declarative_source import (
        ManifestDeclarativeSource,
    )

    source = MagicMock(spec=ManifestDeclarativeSource)
    source.streams.return_value = []
    checker = CheckDynamicStream(stream_count=1, parameters={})

    checker.check_connection(source, logger, {})

    source.streams.assert_called_once_with(config={})
