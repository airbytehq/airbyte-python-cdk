#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from typing import Any, Iterator, List, Mapping
from unittest.mock import MagicMock, Mock, patch

import orjson
import pytest

from airbyte_cdk.connector_builder.models import (
    HttpRequest,
    HttpResponse,
    LogMessage,
    StreamRead,
    StreamReadPages,
)
from airbyte_cdk.connector_builder.test_reader import TestReader
from airbyte_cdk.connector_builder.test_reader.helpers import create_response_from_log_message
from airbyte_cdk.models import (
    AirbyteControlConnectorConfigMessage,
    AirbyteControlMessage,
    AirbyteLogMessage,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStreamState,
    Level,
    OrchestratorType,
    StreamDescriptor,
)
from airbyte_cdk.models import Type as MessageType
from unit_tests.connector_builder.utils import create_configured_catalog

_NO_PK = [[]]
_NO_CURSOR_FIELD = []

MAX_PAGES_PER_SLICE = 4
MAX_SLICES = 3

_NO_STATE = []

MANIFEST = {
    "version": "0.30.0",
    "type": "DeclarativeSource",
    "definitions": {
        "selector": {
            "extractor": {"field_path": ["items"], "type": "DpathExtractor"},
            "type": "RecordSelector",
        },
        "requester": {
            "url_base": "https://demonslayers.com/api/v1/",
            "http_method": "GET",
            "type": "DeclarativeSource",
        },
        "retriever": {
            "type": "DeclarativeSource",
            "record_selector": {
                "extractor": {"field_path": ["items"], "type": "DpathExtractor"},
                "type": "RecordSelector",
            },
            "paginator": {"type": "NoPagination"},
            "requester": {
                "url_base": "https://demonslayers.com/api/v1/",
                "http_method": "GET",
                "type": "HttpRequester",
            },
        },
        "hashiras_stream": {
            "retriever": {
                "type": "DeclarativeSource",
                "record_selector": {
                    "extractor": {"field_path": ["items"], "type": "DpathExtractor"},
                    "type": "RecordSelector",
                },
                "paginator": {"type": "NoPagination"},
                "requester": {
                    "url_base": "https://demonslayers.com/api/v1/",
                    "http_method": "GET",
                    "type": "HttpRequester",
                },
            },
            "$parameters": {"name": "hashiras", "path": "/hashiras"},
        },
        "breathing_techniques_stream": {
            "retriever": {
                "type": "DeclarativeSource",
                "record_selector": {
                    "extractor": {"field_path": ["items"], "type": "DpathExtractor"},
                    "type": "RecordSelector",
                },
                "paginator": {"type": "NoPagination"},
                "requester": {
                    "url_base": "https://demonslayers.com/api/v1/",
                    "http_method": "GET",
                    "type": "HttpRequester",
                },
            },
            "$parameters": {"name": "breathing-techniques", "path": "/breathing_techniques"},
        },
    },
    "streams": [
        {
            "type": "DeclarativeStream",
            "retriever": {
                "type": "SimpleRetriever",
                "record_selector": {
                    "extractor": {"field_path": ["items"], "type": "DpathExtractor"},
                    "type": "RecordSelector",
                },
                "paginator": {"type": "NoPagination"},
                "requester": {
                    "url_base": "https://demonslayers.com/api/v1/",
                    "http_method": "GET",
                    "type": "HttpRequester",
                },
            },
            "$parameters": {"name": "hashiras", "path": "/hashiras"},
        },
        {
            "type": "DeclarativeStream",
            "retriever": {
                "type": "SimpleRetriever",
                "record_selector": {
                    "extractor": {"field_path": ["items"], "type": "DpathExtractor"},
                    "type": "RecordSelector",
                },
                "paginator": {"type": "NoPagination"},
                "requester": {
                    "url_base": "https://demonslayers.com/api/v1/",
                    "http_method": "GET",
                    "type": "HttpRequester",
                },
            },
            "$parameters": {"name": "breathing-techniques", "path": "/breathing_techniques"},
        },
    ],
    "check": {"stream_names": ["hashiras"], "type": "CheckStream"},
}

CONFIG = {"rank": "upper-six"}

A_SOURCE = MagicMock()


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_get_grouped_messages(mock_entrypoint_read: Mock) -> None:
    stream_name = "hashiras"
    url = "https://demonslayers.com/api/v1/hashiras?era=taisho"
    request = {
        "headers": {"Content-Type": "application/json"},
        "method": "GET",
        "body": {"content": '{"custom": "field"}'},
    }
    response = {
        "status_code": 200,
        "headers": {"field": "value"},
        "body": {"content": '{"name": "field"}'},
    }
    expected_schema = {
        "$schema": "http://json-schema.org/schema#",
        "properties": {"name": {"type": ["string", "null"]}, "date": {"type": ["string", "null"]}},
        "type": "object",
    }
    expected_datetime_fields = {"date": "%Y-%m-%d"}
    expected_pages = [
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras?era=taisho",
                headers={"Content-Type": "application/json"},
                body='{"custom": "field"}',
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[
                {"name": "Shinobu Kocho", "date": "2023-03-03"},
                {"name": "Muichiro Tokito", "date": "2023-03-04"},
            ],
        ),
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras?era=taisho",
                headers={"Content-Type": "application/json"},
                body='{"custom": "field"}',
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[{"name": "Mitsuri Kanroji", "date": "2023-03-05"}],
        ),
    ]

    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            [
                request_response_log_message(request, response, url, stream_name),
                record_message(stream_name, {"name": "Shinobu Kocho", "date": "2023-03-03"}),
                record_message(stream_name, {"name": "Muichiro Tokito", "date": "2023-03-04"}),
                request_response_log_message(request, response, url, stream_name),
                record_message(stream_name, {"name": "Mitsuri Kanroji", "date": "2023-03-05"}),
            ]
        ),
    )

    connector_builder_handler = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)
    actual_response: StreamRead = connector_builder_handler.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert actual_response.inferred_schema == expected_schema
    assert actual_response.inferred_datetime_formats == expected_datetime_fields

    single_slice = actual_response.slices[0]
    for i, actual_page in enumerate(single_slice.pages):
        assert actual_page == expected_pages[i]


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_get_grouped_messages_with_logs(mock_entrypoint_read: Mock) -> None:
    stream_name = "hashiras"
    url = "https://demonslayers.com/api/v1/hashiras?era=taisho"
    request = {
        "headers": {"Content-Type": "application/json"},
        "method": "GET",
        "body": {"content": '{"custom": "field"}'},
    }
    response = {
        "status_code": 200,
        "headers": {"field": "value"},
        "body": {"content": '{"name": "field"}'},
    }
    expected_pages = [
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras?era=taisho",
                headers={"Content-Type": "application/json"},
                body='{"custom": "field"}',
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[{"name": "Shinobu Kocho"}, {"name": "Muichiro Tokito"}],
        ),
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras?era=taisho",
                headers={"Content-Type": "application/json"},
                body='{"custom": "field"}',
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[{"name": "Mitsuri Kanroji"}],
        ),
    ]
    expected_logs = [
        LogMessage(**{"message": "log message before the request", "level": "INFO"}),
        LogMessage(**{"message": "log message during the page", "level": "INFO"}),
        LogMessage(**{"message": "log message after the response", "level": "INFO"}),
    ]

    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            [
                AirbyteMessage(
                    type=MessageType.LOG,
                    log=AirbyteLogMessage(
                        level=Level.INFO, message="log message before the request"
                    ),
                ),
                request_response_log_message(request, response, url, stream_name),
                record_message(stream_name, {"name": "Shinobu Kocho"}),
                AirbyteMessage(
                    type=MessageType.LOG,
                    log=AirbyteLogMessage(level=Level.INFO, message="log message during the page"),
                ),
                record_message(stream_name, {"name": "Muichiro Tokito"}),
                AirbyteMessage(
                    type=MessageType.LOG,
                    log=AirbyteLogMessage(
                        level=Level.INFO, message="log message after the response"
                    ),
                ),
            ]
        ),
    )

    connector_builder_handler = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)

    actual_response: StreamRead = connector_builder_handler.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )
    single_slice = actual_response.slices[0]
    for i, actual_page in enumerate(single_slice.pages):
        assert actual_page == expected_pages[i]

    for i, actual_log in enumerate(actual_response.logs):
        assert actual_log == expected_logs[i]


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_get_grouped_messages_limit_0(mock_entrypoint_read: Mock) -> None:
    stream_name = "hashiras"
    url = "https://demonslayers.com/api/v1/hashiras?era=taisho"
    request = {
        "headers": {"Content-Type": "application/json"},
        "method": "GET",
        "body": {"content": '{"custom": "field"}'},
    }
    response = {
        "status_code": 200,
        "headers": {"field": "value"},
        "body": {"content": '{"name": "field"}'},
    }
    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            [
                request_response_log_message(request, response, url, stream_name),
                record_message(stream_name, {"name": "Shinobu Kocho"}),
                record_message(stream_name, {"name": "Muichiro Tokito"}),
                request_response_log_message(request, response, url, stream_name),
                record_message(stream_name, {"name": "Mitsuri Kanroji"}),
            ]
        ),
    )
    api = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)

    with pytest.raises(ValueError):
        api.run_test_read(
            source=mock_source,
            config=CONFIG,
            configured_catalog=create_configured_catalog(stream_name),
            stream_name=stream_name,
            state=_NO_STATE,
            record_limit=0,
        )


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_get_grouped_messages_no_records(mock_entrypoint_read: Mock) -> None:
    stream_name = "hashiras"
    url = "https://demonslayers.com/api/v1/hashiras?era=taisho"
    request = {
        "headers": {"Content-Type": "application/json"},
        "method": "GET",
        "body": {"content": '{"custom": "field"}'},
    }
    response = {
        "status_code": 200,
        "headers": {"field": "value"},
        "body": {"content": '{"name": "field"}'},
    }
    expected_pages = [
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras?era=taisho",
                headers={"Content-Type": "application/json"},
                body='{"custom": "field"}',
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[],
        ),
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras?era=taisho",
                headers={"Content-Type": "application/json"},
                body='{"custom": "field"}',
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[],
        ),
    ]

    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            [
                request_response_log_message(request, response, url, stream_name),
                request_response_log_message(request, response, url, stream_name),
            ]
        ),
    )

    message_grouper = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)

    actual_response: StreamRead = message_grouper.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    single_slice = actual_response.slices[0]
    for i, actual_page in enumerate(single_slice.pages):
        assert actual_page == expected_pages[i]


@pytest.mark.parametrize(
    "log_message, expected_response",
    [
        pytest.param(
            {
                "http": {
                    "response": {
                        "status_code": 200,
                        "headers": {"field": "name"},
                        "body": {"content": '{"id": "fire", "owner": "kyojuro_rengoku"}'},
                    }
                }
            },
            HttpResponse(
                status=200,
                headers={"field": "name"},
                body='{"id": "fire", "owner": "kyojuro_rengoku"}',
            ),
            id="test_create_response_with_all_fields",
        ),
        pytest.param(
            {"http": {"response": {"status_code": 200, "headers": {"field": "name"}}}},
            HttpResponse(status=200, headers={"field": "name"}, body=""),
            id="test_create_response_with_no_body",
        ),
        pytest.param(
            {
                "http": {
                    "response": {
                        "status_code": 200,
                        "body": {"content": '{"id": "fire", "owner": "kyojuro_rengoku"}'},
                    }
                }
            },
            HttpResponse(status=200, body='{"id": "fire", "owner": "kyojuro_rengoku"}'),
            id="test_create_response_with_no_headers",
        ),
        pytest.param(
            {
                "http": {
                    "response": {
                        "status_code": 200,
                        "headers": {"field": "name"},
                        "body": {
                            "content": '[{"id": "fire", "owner": "kyojuro_rengoku"}, {"id": "mist", "owner": "muichiro_tokito"}]'
                        },
                    }
                }
            },
            HttpResponse(
                status=200,
                headers={"field": "name"},
                body='[{"id": "fire", "owner": "kyojuro_rengoku"}, {"id": "mist", "owner": "muichiro_tokito"}]',
            ),
            id="test_create_response_with_array",
        ),
        pytest.param(
            {"http": {"response": {"status_code": 200, "body": {"content": "tomioka"}}}},
            HttpResponse(status=200, body="tomioka"),
            id="test_create_response_with_string",
        ),
    ],
)
def test_create_response_from_log_message(
    log_message: str, expected_response: HttpResponse
) -> None:
    if isinstance(log_message, str):
        response_message = json.loads(log_message)
    else:
        response_message = log_message

    assert create_response_from_log_message(response_message) == expected_response


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_get_grouped_messages_with_many_slices(mock_entrypoint_read: Mock) -> None:
    stream_name = "hashiras"
    url = "http://a-url.com"
    request: Mapping[str, Any] = {}
    response = {"status_code": 200}

    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            [
                slice_message('{"descriptor": "first_slice"}'),
                request_response_log_message(request, response, url, stream_name),
                record_message(stream_name, {"name": "Muichiro Tokito"}),
                slice_message('{"descriptor": "second_slice"}'),
                request_response_log_message(request, response, url, stream_name),
                record_message(stream_name, {"name": "Shinobu Kocho"}),
                record_message(stream_name, {"name": "Mitsuri Kanroji"}),
                request_response_log_message(request, response, url, stream_name),
                record_message(stream_name, {"name": "Obanai Iguro"}),
                request_response_log_message(request, response, url, stream_name),
                state_message(stream_name, {"a_timestamp": 123}),
            ]
        ),
    )

    connector_builder_handler = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)

    stream_read: StreamRead = connector_builder_handler.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert not stream_read.test_read_limit_reached
    assert len(stream_read.slices) == 2

    assert stream_read.slices[0].slice_descriptor == {"descriptor": "first_slice"}
    assert len(stream_read.slices[0].pages) == 1
    assert len(stream_read.slices[0].pages[0].records) == 1
    assert stream_read.slices[0].state == []

    assert stream_read.slices[1].slice_descriptor == {"descriptor": "second_slice"}
    assert len(stream_read.slices[1].pages) == 3
    assert len(stream_read.slices[1].pages[0].records) == 2
    assert len(stream_read.slices[1].pages[1].records) == 1
    assert len(stream_read.slices[1].pages[2].records) == 0

    assert (
        orjson.dumps(stream_read.slices[1].state[0].stream.stream_state).decode()
        == orjson.dumps(AirbyteStateBlob(a_timestamp=123)).decode()
    )


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_get_grouped_messages_given_maximum_number_of_slices_then_test_read_limit_reached(
    mock_entrypoint_read: Mock,
) -> None:
    stream_name = "hashiras"
    maximum_number_of_slices = 5
    request: Mapping[str, Any] = {}
    response = {"status_code": 200}
    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            [slice_message(), request_response_log_message(request, response, "a_url", stream_name)]
            * maximum_number_of_slices
        ),
    )

    api = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)

    stream_read: StreamRead = api.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert stream_read.test_read_limit_reached


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_get_grouped_messages_given_maximum_number_of_pages_then_test_read_limit_reached(
    mock_entrypoint_read: Mock,
) -> None:
    stream_name = "hashiras"
    maximum_number_of_pages_per_slice = 5
    request: Mapping[str, Any] = {}
    response = {"status_code": 200}
    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            [slice_message()]
            + [request_response_log_message(request, response, "a_url", stream_name)]
            * maximum_number_of_pages_per_slice
        ),
    )

    api = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)

    stream_read: StreamRead = api.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert stream_read.test_read_limit_reached


def test_read_stream_returns_error_if_stream_does_not_exist() -> None:
    mock_source = MagicMock()
    mock_source.read.side_effect = ValueError("error")
    mock_source.streams.return_value = [make_mock_stream()]

    full_config: Mapping[str, Any] = {**CONFIG, **{"__injected_declarative_manifest": MANIFEST}}

    message_grouper = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)
    actual_response = message_grouper.run_test_read(
        source=mock_source,
        config=full_config,
        configured_catalog=create_configured_catalog("not_in_manifest"),
        stream_name="not_in_manifest",
        state=_NO_STATE,
    )

    assert len(actual_response.logs) == 1
    assert "Traceback" in actual_response.logs[0].stacktrace
    assert "ERROR" in actual_response.logs[0].level


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_given_control_message_then_stream_read_has_config_update(
    mock_entrypoint_read: Mock,
) -> None:
    stream_name = "hashiras"
    updated_config = {"x": 1}
    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            any_request_and_response_with_a_record()
            + [connector_configuration_control_message(1, updated_config)]
        ),
    )
    connector_builder_handler = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)
    stream_read: StreamRead = connector_builder_handler.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert stream_read.latest_config_update == updated_config


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_given_multiple_control_messages_then_stream_read_has_latest_based_on_emitted_at(
    mock_entrypoint_read: Mock,
) -> None:
    stream_name = "hashiras"
    earliest = 0
    earliest_config = {"earliest": 0}
    latest = 1
    latest_config = {"latest": 1}
    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            any_request_and_response_with_a_record()
            + [
                # here, we test that even if messages are emitted in a different order, we still rely on `emitted_at`
                connector_configuration_control_message(latest, latest_config),
                connector_configuration_control_message(earliest, earliest_config),
            ]
        ),
    )
    connector_builder_handler = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)
    stream_read: StreamRead = connector_builder_handler.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert stream_read.latest_config_update == latest_config


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_given_multiple_control_messages_with_same_timestamp_then_stream_read_has_latest_based_on_message_order(
    mock_entrypoint_read: Mock,
) -> None:
    stream_name = "hashiras"
    emitted_at = 0
    earliest_config = {"earliest": 0}
    latest_config = {"latest": 1}
    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            any_request_and_response_with_a_record()
            + [
                connector_configuration_control_message(emitted_at, earliest_config),
                connector_configuration_control_message(emitted_at, latest_config),
            ]
        ),
    )
    connector_builder_handler = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)
    stream_read: StreamRead = connector_builder_handler.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert stream_read.latest_config_update == latest_config


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_given_auxiliary_requests_then_return_auxiliary_request(mock_entrypoint_read: Mock) -> None:
    stream_name = "hashiras"
    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(any_request_and_response_with_a_record() + [auxiliary_request_log_message()]),
    )
    connector_builder_handler = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)
    stream_read: StreamRead = connector_builder_handler.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert len(stream_read.auxiliary_requests) == 1


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_given_no_slices_then_return_empty_slices(mock_entrypoint_read: Mock) -> None:
    stream_name = "hashiras"
    mock_source = make_mock_source(mock_entrypoint_read, iter([auxiliary_request_log_message()]))
    connector_builder_handler = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)
    stream_read: StreamRead = connector_builder_handler.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert len(stream_read.slices) == 0


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_given_pk_then_ensure_pk_is_pass_to_schema_inferrence(mock_entrypoint_read: Mock) -> None:
    stream_name = "hashiras"
    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            [
                request_response_log_message(
                    {"request": 1}, {"response": 2}, "http://any_url.com", stream_name
                ),
                record_message(stream_name, {"id": "Shinobu Kocho", "date": "2023-03-03"}),
                record_message(stream_name, {"id": "Muichiro Tokito", "date": "2023-03-04"}),
            ]
        ),
    )
    mock_source.streams.return_value = [Mock()]
    mock_source.streams.return_value[0].name = stream_name
    mock_source.streams.return_value[0].primary_key = [["id"]]
    mock_source.streams.return_value[0].cursor_field = _NO_CURSOR_FIELD
    connector_builder_handler = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)

    stream_read: StreamRead = connector_builder_handler.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert stream_read.inferred_schema["required"] == ["id"]


@patch("airbyte_cdk.connector_builder.test_reader.reader.AirbyteEntrypoint.read")
def test_given_cursor_field_then_ensure_cursor_field_is_pass_to_schema_inferrence(
    mock_entrypoint_read: Mock,
) -> None:
    stream_name = "hashiras"
    mock_source = make_mock_source(
        mock_entrypoint_read,
        iter(
            [
                request_response_log_message(
                    {"request": 1}, {"response": 2}, "http://any_url.com", stream_name
                ),
                record_message(stream_name, {"id": "Shinobu Kocho", "date": "2023-03-03"}),
                record_message(stream_name, {"id": "Muichiro Tokito", "date": "2023-03-04"}),
            ]
        ),
    )
    mock_source.streams.return_value = [Mock()]
    mock_source.streams.return_value[0].name = stream_name
    mock_source.streams.return_value[0].primary_key = _NO_PK
    mock_source.streams.return_value[0].cursor_field = ["date"]
    connector_builder_handler = TestReader(MAX_PAGES_PER_SLICE, MAX_SLICES)

    stream_read: StreamRead = connector_builder_handler.run_test_read(
        source=mock_source,
        config=CONFIG,
        configured_catalog=create_configured_catalog(stream_name),
        stream_name=stream_name,
        state=_NO_STATE,
    )

    assert stream_read.inferred_schema["required"] == ["date"]


def make_mock_source(
    mock_entrypoint_read: Mock, return_value: Iterator[AirbyteMessage]
) -> MagicMock:
    mock_source = MagicMock()
    mock_entrypoint_read.return_value = return_value
    mock_source.streams.return_value = [make_mock_stream()]
    return mock_source


def make_mock_stream():
    mock_stream = MagicMock()
    mock_stream.primary_key = []
    mock_stream.cursor_field = []
    return mock_stream


def request_log_message(request: Mapping[str, Any]) -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.LOG,
        log=AirbyteLogMessage(level=Level.INFO, message=f"request:{json.dumps(request)}"),
    )


def response_log_message(response: Mapping[str, Any]) -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.LOG,
        log=AirbyteLogMessage(level=Level.INFO, message=f"response:{json.dumps(response)}"),
    )


def record_message(stream: str, data: Mapping[str, Any]) -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.RECORD,
        record=AirbyteRecordMessage(stream=stream, data=data, emitted_at=1234),
    )


def state_message(stream: str, data: Mapping[str, Any]) -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.STATE,
        state=AirbyteStateMessage(
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name=stream), stream_state=data
            )
        ),
    )


def slice_message(slice_descriptor: str = '{"key": "value"}') -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.LOG,
        log=AirbyteLogMessage(level=Level.INFO, message="slice:" + slice_descriptor),
    )


def connector_configuration_control_message(
    emitted_at: float, config: Mapping[str, Any]
) -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.CONTROL,
        control=AirbyteControlMessage(
            type=OrchestratorType.CONNECTOR_CONFIG,
            emitted_at=emitted_at,
            connectorConfig=AirbyteControlConnectorConfigMessage(config=config),
        ),
    )


def auxiliary_request_log_message() -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.LOG,
        log=AirbyteLogMessage(
            level=Level.INFO,
            message=json.dumps(
                {
                    "http": {
                        "is_auxiliary": True,
                        "title": "a title",
                        "description": "a description",
                        "request": {},
                        "response": {},
                    },
                    "url": {"full": "https://a-url.com"},
                }
            ),
        ),
    )


def request_response_log_message(
    request: Mapping[str, Any], response: Mapping[str, Any], url: str, stream_name: str
) -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.LOG,
        log=AirbyteLogMessage(
            level=Level.INFO,
            message=json.dumps(
                {
                    "airbyte_cdk": {"stream": {"name": stream_name}},
                    "http": {
                        "title": "a title",
                        "description": "a description",
                        "request": request,
                        "response": response,
                    },
                    "url": {"full": url},
                }
            ),
        ),
    )


def any_request_and_response_with_a_record() -> List[AirbyteMessage]:
    return [
        request_response_log_message(
            {"request": 1}, {"response": 2}, "http://any_url.com", "hashiras"
        ),
        record_message("hashiras", {"name": "Shinobu Kocho"}),
    ]
