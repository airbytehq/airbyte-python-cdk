#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import copy
import json
import logging
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import PageCompleted
from airbyte_cdk.sources.types import StreamSlice
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from unit_tests.sources.declarative.incremental.test_sync_token_cursor import (
    _cursor,
    _partition,
    _response,
)
from unit_tests.sources.declarative.incremental.test_sync_token_cursor_integration import (
    _URL,
    _manifest,
    _read,
    _source_and_catalog,
    _state,
)

NO_RETRY = {"type": "DefaultErrorHandler", "max_retries": 0, "backoff_strategies": []}
STREAMING_DECODERS = [
    {"type": "GzipDecoder", "decoder": {"type": "JsonDecoder"}},
    {"type": "JsonlDecoder"},
    {"type": "JsonItemsDecoder", "items_path": "items.item"},
    {"type": "CsvDecoder"},
    {"type": "IterableDecoder"},
]


@pytest.mark.parametrize(
    "emit_connector_builder_messages", [False, True], ids=["platform", "builder"]
)
@pytest.mark.parametrize("decoder", STREAMING_DECODERS, ids=lambda decoder: decoder["type"])
def test_streaming_decoder_is_rejected_when_the_stream_is_built(
    decoder, emit_connector_builder_messages
):
    manifest = _manifest()
    manifest["streams"][0]["retriever"]["decoder"] = decoder
    manifest["streams"][0]["retriever"]["paginator"] = {"type": "NoPagination"}
    source = ConcurrentDeclarativeSource(
        source_config=manifest,
        config={},
        catalog=None,
        emit_connector_builder_messages=emit_connector_builder_messages,
    )
    with pytest.raises(
        AirbyteTracedException, match="SyncTokenCursor is not supported with a streaming decoder"
    ) as error:
        source.discover(logger=MagicMock(), config={})
    assert error.value.failure_type == FailureType.config_error


def test_api_retention_period_is_rejected_with_sync_token_cursor():
    incremental_stream = _manifest()["streams"][0]
    full_refresh_stream = copy.deepcopy(incremental_stream)
    full_refresh_stream.pop("incremental_sync")
    manifest = _manifest()
    manifest["streams"] = [
        {
            "type": "StateDelegatingStream",
            "name": "events",
            "api_retention_period": "P21D",
            "full_refresh_stream": full_refresh_stream,
            "incremental_stream": incremental_stream,
        }
    ]
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)
    with pytest.raises(
        ValueError, match="SyncTokenCursor which is not supported with api_retention_period"
    ):
        source.discover(logger=MagicMock(), config={})


@pytest.mark.parametrize("initial_token", ["0", "True", "None", "[]"])
def test_initial_token_is_sent_as_a_string(initial_token):
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": initial_token}),
            HttpResponse(json.dumps({"items": [], "syncToken": "T1"})),
        )
        source, catalog = _source_and_catalog(
            _manifest(initial_token=initial_token, error_handler=NO_RETRY)
        )
        messages = _read(source, catalog)
    assert _state(messages)[-1].stream.stream_state.__dict__.get("token") == "T1"


def test_cursor_field_is_interpolated():
    manifest = _manifest()
    stream = manifest["streams"][0]
    stream["$parameters"] = {"cursor_field": "updatedAt"}
    stream["incremental_sync"]["cursor_field"] = "{{ parameters.cursor_field }}"
    discovered = (
        ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)
        .discover(logger=MagicMock(), config={})
        .streams[0]
    )
    assert discovered.default_cursor_field == ["updatedAt"]


def test_custom_retriever_decoder_mapping_warns_without_crashing(caplog):
    manifest = _manifest()
    retriever = manifest["streams"][0]["retriever"]
    retriever["type"] = "CustomRetriever"
    retriever["class_name"] = (
        "unit_tests.sources.declarative.parsers.testing_components.TestingCustomRetriever"
    )
    retriever["decoder"] = {"type": "JsonDecoder"}
    retriever["paginator"] = {"type": "NoPagination"}

    with caplog.at_level(logging.WARNING):
        ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None).discover(
            logger=MagicMock(), config={}
        )

    assert "no sync token will be captured" in caplog.text


def test_incomplete_page_with_non_string_token_keeps_stored_token_and_warns(caplog):
    cursor = _cursor(
        state={"state_type": "sync_token", "token": "T1"},
        completion_condition="{{ response.get('complete') }}",
    )
    stream_slice = StreamSlice(partition={}, cursor_slice={"token": "T1"})

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        cursor.on_page_completed(
            PageCompleted(
                stream_slice,
                _response({"complete": False, "syncToken": {"invalid": "object"}}),
                True,
            )
        )
        cursor.close_partition(_partition({}))

    assert cursor.state["token"] == "T1"
    assert "pagination ended without a new sync token" in caplog.text
