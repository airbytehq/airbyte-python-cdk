#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import copy
import json
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.models import FailureType, SyncMode
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import (
    SubstreamPartitionRouter,
)
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import PageCompleted
from airbyte_cdk.sources.types import StreamSlice
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from unit_tests.sources.declarative.incremental.test_sync_token_cursor import (
    Router,
    _cursor,
    _partition,
    _response,
)
from unit_tests.sources.declarative.incremental.test_sync_token_cursor_integration import (
    _URL,
    _incoming_state,
    _manifest,
    _read,
    _source_and_catalog,
    _state,
)

NO_RETRY = {"type": "DefaultErrorHandler", "max_retries": 0, "backoff_strategies": []}
TOKEN_CURSOR = {
    "type": "SyncTokenCursor",
    "token_path": ["syncToken"],
    "token_option": {
        "type": "RequestOption",
        "field_name": "syncToken",
        "inject_into": "body_json",
    },
}
DATETIME_CURSOR = {
    "type": "DatetimeBasedCursor",
    "cursor_field": "created_at",
    "start_datetime": "2020-01-01T00:00:00Z",
    "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
}


def _token(messages):
    return _state(messages)[-1].stream.stream_state.__dict__.get("token")


# A mid-pass page's token must not commit when the terminal page carries none.
def test_intermediate_page_token_is_not_committed_when_terminal_page_has_none():
    state = _incoming_state({"state_type": "sync_token", "token": "T1"})
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "T1"}),
            HttpResponse(
                json.dumps({"items": [{"id": "1"}], "nextCursor": "p2", "syncToken": "MID"})
            ),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "T1", "nextCursor": "p2"}),
            HttpResponse(json.dumps({"items": [{"id": "2"}]})),
        )
        source, catalog = _source_and_catalog(_manifest(error_handler=NO_RETRY), state)
        messages = _read(source, catalog, state)
    assert _token(messages) == "T1"


@pytest.mark.parametrize(
    "inject_into,request_kwargs",
    [
        ("request_parameter", {"query_params": {"syncToken": "T1"}}),
        ("header", {"headers": {"syncToken": "T1"}}),
        ("body_data", {"body": "syncToken=T1"}),
    ],
)
def test_stored_token_is_sent_through_each_injection_target(inject_into, request_kwargs):
    manifest = _manifest(error_handler=NO_RETRY)
    manifest["streams"][0]["incremental_sync"]["token_option"]["inject_into"] = inject_into
    state = _incoming_state({"state_type": "sync_token", "token": "T1"})
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, **request_kwargs),
            HttpResponse(json.dumps({"items": [{"id": "1"}], "syncToken": "T2"})),
        )
        source, catalog = _source_and_catalog(manifest, state)
        messages = _read(source, catalog, state)
    assert _token(messages) == "T2"


def test_initial_token_is_not_sent_when_a_token_is_stored():
    state = _incoming_state({"state_type": "sync_token", "token": "T1"})
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "T1"}),
            HttpResponse(json.dumps({"items": [], "syncToken": "T2"})),
        )
        source, catalog = _source_and_catalog(
            _manifest(initial_token="*", error_handler=NO_RETRY), state
        )
        messages = _read(source, catalog, state)
    assert _token(messages) == "T2"


# Partitioned state is emitted at most every 600 s, plus one final unthrottled emission.
def test_partitioned_state_emission_is_throttled(mocker):
    router = Router([StreamSlice(partition={"account": a}, cursor_slice={}) for a in "abc"])
    cursor = _cursor(router=router)
    clock = mocker.patch("time.time")
    for now, stream_slice in zip((1000, 1100, 1700), router.slices):
        clock.return_value = now
        token = stream_slice.partition["account"].upper()
        cursor.on_page_completed(PageCompleted(stream_slice, _response({"syncToken": token}), True))
        cursor.close_partition(_partition(stream_slice.partition))
    assert len(list(cursor._message_repository.consume_queue())) == 2
    cursor.ensure_at_least_one_state_emitted()
    (final,) = list(cursor._message_repository.consume_queue())
    states = final.state.stream.stream_state.__dict__["states"]
    assert {(s["partition"]["account"], s["cursor"]["token"]) for s in states} == {
        ("a", "A"),
        ("b", "B"),
        ("c", "C"),
    }


def test_stored_partition_replaces_its_own_token_at_the_partition_cap():
    router = Router([StreamSlice(partition={"account": a}, cursor_slice={}) for a in "ab"])
    cursor = _cursor(
        router=router,
        max_partitions=1,
        state={
            "state_type": "sync_token",
            "states": [
                {
                    "partition": {"account": "a"},
                    "cursor": {"token": "A0", "committed_at": "2026-01-01T00:00:00Z"},
                }
            ],
        },
    )
    list(cursor.stream_slices())
    for stream_slice, token in zip(router.slices, ("A1", "B1")):
        cursor.on_page_completed(PageCompleted(stream_slice, _response({"syncToken": token}), True))
        cursor.close_partition(_partition(stream_slice.partition))
    assert [(s["partition"]["account"], s["cursor"]["token"]) for s in cursor.state["states"]] == [
        ("a", "A1")
    ]


def test_partition_key_named_token_is_rejected_at_runtime():
    cursor = _cursor(router=Router([StreamSlice(partition={"token": "x"}, cursor_slice={})]))
    with pytest.raises(AirbyteTracedException, match="reserved key 'token'") as error:
        list(cursor.stream_slices())
    assert error.value.failure_type == FailureType.config_error


def _parent_child_manifest(*, parent_sync=None, child_sync=None, incremental_dependency=False):
    manifest = _manifest()
    child = manifest["streams"][0]
    parent = copy.deepcopy(child)
    parent["name"] = "accounts"
    parent["retriever"]["requester"]["path"] = "/accounts"
    if parent_sync is None:
        parent.pop("incremental_sync")
    else:
        parent["incremental_sync"] = parent_sync
    if child_sync is not None:
        child["incremental_sync"] = child_sync
    child["retriever"]["partition_router"] = {
        "type": "SubstreamPartitionRouter",
        "parent_stream_configs": [
            {
                "type": "ParentStreamConfig",
                "stream": parent,
                "parent_key": "id",
                "partition_field": "account",
                "incremental_dependency": incremental_dependency,
            }
        ],
    }
    manifest["streams"] = [parent, child]
    return manifest


def test_get_partition_router_returns_substream_router_for_token_child():
    source = ConcurrentDeclarativeSource(
        source_config=_parent_child_manifest(), config={}, catalog=None
    )
    child = {s.name: s for s in source.streams(config={})}["events"]
    assert isinstance(child.get_partition_router(), SubstreamPartitionRouter)


@pytest.mark.parametrize("cursor_field,expected", [(None, []), ("updatedAt", ["updatedAt"])])
def test_discovery_advertises_the_documented_shape(cursor_field, expected):
    manifest = _manifest()
    if cursor_field:
        manifest["streams"][0]["incremental_sync"]["cursor_field"] = cursor_field
    stream = (
        ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)
        .discover(logger=MagicMock(), config={})
        .streams[0]
    )
    assert stream.supported_sync_modes == [SyncMode.full_refresh, SyncMode.incremental]
    assert stream.source_defined_cursor is True
    assert stream.is_resumable is True
    assert stream.default_cursor_field == expected


def test_manifest_completion_condition_blocks_commit():
    state = _incoming_state({"state_type": "sync_token", "token": "T1"})
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "T1"}),
            HttpResponse(json.dumps({"items": [{"id": "1"}], "syncToken": "T2", "success": False})),
        )
        source, catalog = _source_and_catalog(
            _manifest(
                completion_condition="{{ response.get('success', true) }}", error_handler=NO_RETRY
            ),
            state,
        )
        messages = _read(source, catalog, state)
    assert _token(messages) == "T1"


# A page-limited read stops on a page that carries a token; completion_condition must block it.
def test_page_limited_read_does_not_commit_token_on_truncated_page():
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body=None),
            HttpResponse(
                json.dumps(
                    {
                        "items": [{"id": "1"}],
                        "nextCursor": "p2",
                        "syncToken": "MID",
                        "moreDataAvailable": True,
                    }
                )
            ),
        )
        source, catalog = _source_and_catalog(
            _manifest(completion_condition="{{ not response.get('moreDataAvailable') }}"),
            limit_pages_fetched_per_slice=1,
        )
        messages = _read(source, catalog)
    assert not any("token" in m.stream.stream_state.__dict__ for m in _state(messages))


def test_early_exit_on_terminal_page_does_not_commit():
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body=None),
            HttpResponse(
                json.dumps({"items": [{"id": "1"}, {"id": "2"}], "syncToken": "must-not-commit"})
            ),
        )
        source, _ = _source_and_catalog(_manifest())
        stream = source.streams(config={})[0]
        partition = next(iter(stream.generate_partitions()))
        record_iterator = iter(partition.read())
        assert next(record_iterator).data == {"id": "1"}
        record_iterator.close()
        stream.cursor.close_partition(partition)
    assert "token" not in stream.cursor.state


def test_xml_decoder_reaches_records_and_token():
    manifest = _manifest(error_handler=NO_RETRY)
    stream = manifest["streams"][0]
    stream["retriever"]["decoder"] = {"type": "XmlDecoder"}
    stream["retriever"]["record_selector"]["extractor"]["field_path"] = ["root", "items"]
    stream["retriever"]["paginator"] = {"type": "NoPagination"}
    stream["incremental_sync"]["token_path"] = ["root", "syncToken"]
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body=None),
            HttpResponse("<root><items><id>1</id></items><syncToken>X1</syncToken></root>"),
        )
        source, catalog = _source_and_catalog(manifest)
        messages = _read(source, catalog)
    assert any(m.type.value == "RECORD" for m in messages)
    assert _token(messages) == "X1"


def test_datetime_child_cannot_depend_on_token_parent():
    manifest = _parent_child_manifest(
        parent_sync=TOKEN_CURSOR, child_sync=DATETIME_CURSOR, incremental_dependency=True
    )
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)
    with pytest.raises(ValueError, match="SyncTokenCursor parent"):
        source.discover(logger=MagicMock(), config={})


def test_datetime_child_cannot_depend_on_state_delegating_token_parent():
    manifest = _parent_child_manifest(
        parent_sync=TOKEN_CURSOR, child_sync=DATETIME_CURSOR, incremental_dependency=True
    )
    parent_config = manifest["streams"][1]["retriever"]["partition_router"][
        "parent_stream_configs"
    ][0]
    full_refresh = copy.deepcopy(parent_config["stream"])
    full_refresh.pop("incremental_sync")
    parent_config["stream"] = {
        "type": "StateDelegatingStream",
        "name": "accounts",
        "full_refresh_stream": full_refresh,
        "incremental_stream": parent_config["stream"],
    }
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)
    with pytest.raises(ValueError, match="SyncTokenCursor parent"):
        source.discover(logger=MagicMock(), config={})


def test_manifest_property_chunking_is_rejected():
    manifest = _manifest()
    manifest["streams"][0]["retriever"]["requester"]["request_parameters"] = {
        "properties": {
            "type": "QueryProperties",
            "property_list": ["a", "b"],
            "property_chunking": {
                "type": "PropertyChunking",
                "property_limit_type": "property_count",
                "property_limit": 1,
            },
        }
    }
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)
    with pytest.raises(ValueError, match="query property chunking"):
        source.discover(logger=MagicMock(), config={})


def test_datetime_only_setting_from_stream_parameters_is_ignored():
    manifest = _manifest()
    manifest["streams"][0]["$parameters"] = {"datetime_format": "%Y-%m-%d"}
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)
    source.discover(logger=MagicMock(), config={})
