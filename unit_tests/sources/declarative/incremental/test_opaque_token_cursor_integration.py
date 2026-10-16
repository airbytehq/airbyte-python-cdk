#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
import logging
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.models import (
    AirbyteStateType,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    AsyncRetriever as AsyncRetrieverModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    CustomRetriever as CustomRetrieverModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    DeclarativeStream as DeclarativeStreamModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    GroupingPartitionRouter as GroupingPartitionRouterModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    ListPartitionRouter as ListPartitionRouterModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    OpaqueTokenCursor as OpaqueTokenCursorModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    PaginationReset as PaginationResetModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    PaginationResetLimits as PaginationResetLimitsModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    ParentStreamConfig as ParentStreamConfigModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    SubstreamPartitionRouter as SubstreamPartitionRouterModel,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import SimpleRetriever
from airbyte_cdk.sources.declarative.stream_slicers.declarative_partition_generator import (
    StreamSlicerPartitionGenerator,
)
from airbyte_cdk.sources.streams.concurrent.default_stream import DefaultStream
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

_URL = "https://api.example.com/events"


def _manifest(
    *,
    initial_token=None,
    partitioned=False,
    partition_values=("a", "b"),
    completion_condition=None,
    error_handler=None,
):
    stream = {
        "type": "DeclarativeStream",
        "name": "events",
        "primary_key": [],
        "schema_loader": {
            "type": "InlineSchemaLoader",
            "schema": {
                "$schema": "http://json-schema.org/schema#",
                "properties": {"id": {"type": "string"}},
                "type": "object",
            },
        },
        "retriever": {
            "type": "SimpleRetriever",
            "requester": {
                "type": "HttpRequester",
                "url_base": "https://api.example.com",
                "path": "/events",
                "http_method": "POST",
            },
            "record_selector": {
                "type": "RecordSelector",
                "extractor": {"type": "DpathExtractor", "field_path": ["items"]},
            },
            "paginator": {
                "type": "DefaultPaginator",
                "page_token_option": {
                    "type": "RequestOption",
                    "field_name": "nextCursor",
                    "inject_into": "body_json",
                },
                "pagination_strategy": {
                    "type": "CursorPagination",
                    "cursor_value": "{{ response.get('nextCursor') }}",
                    "stop_condition": "{{ not response.get('nextCursor') }}",
                },
            },
        },
        "incremental_sync": {
            "type": "OpaqueTokenCursor",
            "token_path": ["syncToken"],
            "token_option": {
                "type": "RequestOption",
                "field_name": "syncToken",
                "inject_into": "body_json",
            },
        },
    }
    if completion_condition is not None:
        stream["incremental_sync"]["completion_condition"] = completion_condition
    if error_handler is not None:
        stream["retriever"]["requester"]["error_handler"] = error_handler
    if initial_token is not None:
        stream["incremental_sync"]["initial_token"] = initial_token
    if partitioned:
        stream["retriever"]["partition_router"] = {
            "type": "ListPartitionRouter",
            "cursor_field": "account",
            "values": list(partition_values),
            "request_option": {
                "type": "RequestOption",
                "field_name": "account",
                "inject_into": "body_json",
            },
        }
    return {
        "version": "0.34.2",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["events"]},
        "streams": [stream],
        "spec": {
            "connection_specification": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": [],
                "properties": {},
                "additionalProperties": True,
            },
            "documentation_url": "https://example.org",
            "type": "Spec",
        },
    }


def _source_and_catalog(
    manifest,
    state=None,
    *,
    limit_pages_fetched_per_slice=None,
    sync_mode=SyncMode.incremental,
):
    source = ConcurrentDeclarativeSource(
        source_config=manifest, config={}, catalog=None, state=state
    )
    discovered = source.discover(logger=MagicMock(), config={})
    stream = discovered.streams[0]
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=stream,
                sync_mode=sync_mode,
                destination_sync_mode=DestinationSyncMode.append,
                cursor_field=stream.default_cursor_field,
                primary_key=stream.source_defined_primary_key,
            )
        ]
    )
    if limit_pages_fetched_per_slice is not None:
        source._constructor = ModelToComponentFactory(
            message_repository=source._message_repository,
            connector_state_manager=source._connector_state_manager,
            configured_catalog=catalog,
            limit_pages_fetched_per_slice=limit_pages_fetched_per_slice,
        )
    return source, catalog


def _read(source, catalog, state=None):
    return list(source.read(logger=MagicMock(), config={}, catalog=catalog, state=state))


def _read_until_error(source, catalog, state):
    messages = []
    with pytest.raises(AirbyteTracedException):
        for message in source.read(logger=MagicMock(), config={}, catalog=catalog, state=state):
            messages.append(message)
    return messages


def _state(messages):
    return [
        message.state
        for message in messages
        if message.type == Type.STATE and message.state.type == AirbyteStateType.STREAM
    ]


def _incoming_state(stream_state):
    state_manager = ConnectorStateManager()
    state_manager.update_state_for_stream("events", None, stream_state)
    return [state_manager.create_state_message("events", None).state]


def test_t_boot_commits_after_three_pages_and_records():
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body=None),
            HttpResponse(json.dumps({"items": [{"id": "1"}], "nextCursor": "p2"})),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"nextCursor": "p2"}),
            HttpResponse(json.dumps({"items": [{"id": "2"}], "nextCursor": "p3"})),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"nextCursor": "p3"}),
            HttpResponse(json.dumps({"items": [{"id": "3"}], "syncToken": "T1"})),
        )
        source, catalog = _source_and_catalog(_manifest())
        messages = _read(source, catalog)

    records = [index for index, message in enumerate(messages) if message.type == Type.RECORD]
    state_messages = _state(messages)
    assert len(state_messages) == 1
    assert state_messages[0].stream.stream_state.__dict__["token"] == "T1"
    state_index = next(
        index for index, message in enumerate(messages) if message.type == Type.STATE
    )
    assert records and max(records) < state_index


@pytest.mark.parametrize("echo_initial_token", [True, False], ids=["incremental", "no-echo"])
def test_t_inc_and_t_noecho_never_commit_intermediate_token(echo_initial_token):
    first_response = {"items": [{"id": "1"}], "nextCursor": "p2"}
    if echo_initial_token:
        first_response["syncToken"] = "T1"
    state = _incoming_state({"state_type": "opaque_token", "token": "T1"})
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "T1"}),
            HttpResponse(json.dumps(first_response)),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "T1", "nextCursor": "p2"}),
            HttpResponse(json.dumps({"items": [{"id": "2"}], "syncToken": "T2"})),
        )
        source, catalog = _source_and_catalog(_manifest(), state)
        messages = _read(source, catalog, state)

    assert _state(messages)[-1].stream.stream_state.__dict__["token"] == "T2"


def test_t_empty_pages_can_commit_terminal_token():
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body=None),
            HttpResponse(json.dumps({"items": [], "nextCursor": "p2"})),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"nextCursor": "p2"}),
            HttpResponse(json.dumps({"items": [], "syncToken": "T2"})),
        )
        source, catalog = _source_and_catalog(_manifest())
        messages = _read(source, catalog)

    assert not any(message.type == Type.RECORD for message in messages)
    assert _state(messages)[-1].stream.stream_state.__dict__["token"] == "T2"


def test_t_fail_preserves_committed_token_and_emits_incomplete_status():
    state = _incoming_state({"state_type": "opaque_token", "token": "T1"})
    manifest = _manifest(
        error_handler={
            "type": "DefaultErrorHandler",
            "max_retries": 0,
            "backoff_strategies": [],
        }
    )
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "T1"}),
            HttpResponse(json.dumps({"items": [{"id": "1"}], "nextCursor": "p2"})),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "T1", "nextCursor": "p2"}),
            HttpResponse("{}", status_code=500),
        )
        source, catalog = _source_and_catalog(manifest, state)
        messages = _read_until_error(source, catalog, state)

    assert any(message.type == Type.RECORD for message in messages)
    assert _state(messages)[-1].stream.stream_state.__dict__["token"] == "T1"
    assert any(
        message.type == Type.TRACE
        and message.trace.stream_status
        and message.trace.stream_status.status.value == "INCOMPLETE"
        for message in messages
    )


def test_t_builder_page_limit_and_max_records_do_not_commit_tokens():
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body=None),
            HttpResponse(json.dumps({"items": [{"id": "1"}], "nextCursor": "p2"})),
        )
        source, catalog = _source_and_catalog(_manifest(), limit_pages_fetched_per_slice=1)
        messages = _read(source, catalog)
    assert not any("token" in message.stream.stream_state.__dict__ for message in _state(messages))

    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body=None),
            HttpResponse(
                json.dumps(
                    {
                        "items": [{"id": "1"}, {"id": "2"}],
                        "syncToken": "must-not-commit",
                        "nextCursor": "p2",
                    }
                )
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


def test_t_resume_replays_existing_partition_and_bootstraps_missing_partition():
    state = _incoming_state(
        {
            "state_type": "opaque_token",
            "states": [
                {
                    "partition": {"account": "a"},
                    "cursor": {"token": "A1", "committed_at": "2024-01-01T00:00:00Z"},
                }
            ],
        }
    )
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"account": "a", "syncToken": "A1"}),
            HttpResponse(json.dumps({"items": [], "syncToken": "A2"})),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"account": "b"}),
            HttpResponse(json.dumps({"items": [], "syncToken": "B1"})),
        )
        source, catalog = _source_and_catalog(_manifest(partitioned=True), state)
        messages = _read(source, catalog, state)
    committed = {
        item["partition"]["account"]: item["cursor"]["token"]
        for item in _state(messages)[-1].stream.stream_state.__dict__["states"]
    }
    assert committed == {"a": "A2", "b": "B1"}


def test_t_par_keeps_successful_partition_commits_when_another_fails():
    state = _incoming_state(
        {
            "state_type": "opaque_token",
            "states": [
                {
                    "partition": {"account": "c"},
                    "cursor": {"token": "C0", "committed_at": "2024-01-01T00:00:00Z"},
                }
            ],
        }
    )
    manifest = _manifest(
        partitioned=True,
        partition_values=("b", "a", "c"),
        error_handler={
            "type": "DefaultErrorHandler",
            "max_retries": 0,
            "backoff_strategies": [],
        },
    )
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"account": "b"}),
            HttpResponse(json.dumps({"items": [], "syncToken": "B1"})),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"account": "a"}),
            HttpResponse(json.dumps({"items": [], "syncToken": "A1"})),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"account": "c", "syncToken": "C0"}),
            HttpResponse("{}", status_code=500),
        )
        source, catalog = _source_and_catalog(manifest, state)
        messages = _read_until_error(source, catalog, state)
    committed = {
        item["partition"]["account"]: item["cursor"]["token"]
        for item in _state(messages)[-1].stream.stream_state.__dict__["states"]
    }
    assert committed == {"a": "A1", "b": "B1", "c": "C0"}
    assert any(
        message.type == Type.TRACE
        and message.trace.stream_status
        and message.trace.stream_status.status.value == "INCOMPLETE"
        for message in messages
    )


def test_t_boot_inc_builder_and_resume_send_exact_request_bodies():
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL),
            HttpResponse(json.dumps({"items": [{"id": "1"}], "nextCursor": "p2"})),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"nextCursor": "p2"}),
            HttpResponse(json.dumps({"items": [{"id": "2"}], "syncToken": "sync-1"})),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "sync-1"}),
            HttpResponse(json.dumps({"items": [{"id": "3"}], "syncToken": "sync-2"})),
        )

        source, catalog = _source_and_catalog(_manifest())
        first_read = _read(source, catalog)
        first_state = _state(first_read)
        assert any(message.type == Type.RECORD for message in first_read)
        first_stream_state = first_state[0].stream.stream_state.__dict__
        assert first_stream_state["state_type"] == "opaque_token"
        assert first_stream_state["token"] == "sync-1"
        assert first_stream_state["committed_at"].endswith("Z")

        resumed_source, resumed_catalog = _source_and_catalog(_manifest(), first_state)
        resumed = _read(resumed_source, resumed_catalog, first_state)
        assert _state(resumed)[0].stream.stream_state.__dict__["token"] == "sync-2"


def test_t_init_sends_initial_token_and_t_par_keeps_partition_state_separate():
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "*"}),
            HttpResponse(json.dumps({"items": [], "syncToken": "initialized"})),
        )
        source, catalog = _source_and_catalog(_manifest(initial_token="*"))
        messages = _read(source, catalog)
        assert _state(messages)[0].stream.stream_state.__dict__["token"] == "initialized"

    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"account": "a"}),
            HttpResponse(json.dumps({"items": [], "syncToken": "token-a"})),
        )
        http_mocker.post(
            HttpRequest(_URL, body={"account": "b"}),
            HttpResponse(json.dumps({"items": [], "syncToken": "token-b"})),
        )
        source, catalog = _source_and_catalog(_manifest(partitioned=True))
        messages = _read(source, catalog)
        states = _state(messages)[-1].stream.stream_state.__dict__["states"]

    assert {(item["partition"]["account"], item["cursor"]["token"]) for item in states} == {
        ("a", "token-a"),
        ("b", "token-b"),
    }


def test_t_disc_advertises_source_defined_incremental_without_cursor_field():
    source, _ = _source_and_catalog(_manifest())

    discovered = source.discover(logger=MagicMock(), config={})
    stream = discovered.streams[0]
    assert stream.source_defined_cursor is True
    assert stream.default_cursor_field == []


def test_t_disc_datetime_cursor_has_non_none_default_cursor_field():
    manifest = _manifest()
    manifest["streams"][0]["incremental_sync"] = {
        "type": "DatetimeBasedCursor",
        "cursor_field": "created_at",
        "start_datetime": "2020-01-01T00:00:00Z",
        "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
    }
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)

    stream = source.discover(logger=MagicMock(), config={}).streams[0]

    assert stream.default_cursor_field is not None
    assert stream.default_cursor_field == ["created_at"]


@pytest.mark.parametrize("setting", ["start_datetime", "lookback_window"])
def test_t_schema_rejects_datetime_only_extra_settings(setting):
    manifest = _manifest()
    manifest["streams"][0]["incremental_sync"][setting] = "2024-01-01"
    source = ConcurrentDeclarativeSource(
        source_config=manifest, config={}, catalog=None, state=None
    )

    with pytest.raises(ValueError, match=setting):
        source.discover(logger=MagicMock(), config={})


def test_t_schema_accepts_stream_parameters_and_builds_stream():
    manifest = _manifest()
    manifest["streams"][0]["$parameters"] = {"parameter": "value"}
    source, _ = _source_and_catalog(manifest)

    assert isinstance(source.streams(config={})[0], DefaultStream)


def _stream_model():
    return DeclarativeStreamModel.parse_obj(_manifest()["streams"][0])


def test_t_valid_rejects_grouping_async_lazy_dependency_and_reserved_partition():
    stream = _stream_model()
    stream.retriever.partition_router = GroupingPartitionRouterModel.construct(
        type="GroupingPartitionRouter"
    )
    with pytest.raises(ValueError, match="GroupingPartitionRouter"):
        ModelToComponentFactory._validate_opaque_token_stream(stream)

    stream = _stream_model()
    stream.retriever = AsyncRetrieverModel.construct(type="AsyncRetriever")
    with pytest.raises(ValueError, match="AsyncRetriever"):
        ModelToComponentFactory._validate_opaque_token_stream(stream)

    for parent_config, error in (
        ({"lazy_read_pointer": ["items"]}, "lazy_read_pointer"),
        ({"incremental_dependency": True}, "incremental_dependency"),
        ({"partition_field": "token"}, "partition field 'token'"),
    ):
        stream = _stream_model()
        parent_fields = {
            "type": "ParentStreamConfig",
            "stream": None,
            "parent_key": "id",
            "partition_field": "account",
            "incremental_dependency": False,
            "lazy_read_pointer": [],
        }
        parent_fields.update(parent_config)
        parent = ParentStreamConfigModel.construct(**parent_fields)
        stream.retriever.partition_router = SubstreamPartitionRouterModel.construct(
            type="SubstreamPartitionRouter", parent_stream_configs=[parent]
        )
        with pytest.raises(ValueError, match=error):
            ModelToComponentFactory._validate_opaque_token_stream(stream)

    stream = _stream_model()
    stream.retriever.partition_router = ListPartitionRouterModel.construct(
        type="ListPartitionRouter", cursor_field="token"
    )
    with pytest.raises(ValueError, match="partition router field 'token'"):
        ModelToComponentFactory._validate_opaque_token_stream(stream)


def test_t_valid_rejects_opaque_token_parent_dependency():
    stream = _stream_model()
    parent_stream = DeclarativeStreamModel.construct(
        type="DeclarativeStream",
        incremental_sync=OpaqueTokenCursorModel.construct(type="OpaqueTokenCursor"),
    )
    parent = ParentStreamConfigModel.construct(
        type="ParentStreamConfig",
        stream=parent_stream,
        parent_key="id",
        partition_field="account",
        incremental_dependency=True,
    )
    stream.retriever.partition_router = SubstreamPartitionRouterModel.construct(
        type="SubstreamPartitionRouter", parent_stream_configs=[parent]
    )

    with pytest.raises(ValueError, match="OpaqueTokenCursor parent"):
        ModelToComponentFactory._validate_opaque_token_stream(stream)


def test_t_valid_warns_for_custom_retriever(caplog):
    stream = _stream_model()
    stream.retriever = CustomRetrieverModel.construct(type="CustomRetriever")

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        ModelToComponentFactory._validate_opaque_token_stream(stream)

    assert "CustomRetriever does not expose response pages" in caplog.text


def test_t_valid_rejects_pagination_reset_limits_and_query_property_chunking():
    stream = _stream_model()
    stream.retriever.pagination_reset = PaginationResetModel.construct(
        type="PaginationReset",
        limits=PaginationResetLimitsModel.construct(type="PaginationResetLimits"),
    )
    with pytest.raises(ValueError, match="pagination reset `limits`"):
        ModelToComponentFactory._validate_opaque_token_stream(stream)

    stream = _stream_model()
    stream.retriever.requester.query_properties = {
        "type": "QueryProperties",
        "property_chunking": {"type": "PropertyChunking"},
    }
    with pytest.raises(ValueError, match="query property chunking"):
        ModelToComponentFactory._validate_opaque_token_stream(stream)

    stream = _stream_model()
    stream.retriever.requester.request_parameters = {
        "properties": {
            "type": "QueryProperties",
            "property_chunking": {"type": "PropertyChunking"},
        }
    }
    with pytest.raises(ValueError, match="query property chunking"):
        ModelToComponentFactory._validate_opaque_token_stream(stream)


def test_t_inert_preserves_initial_token_when_response_has_no_token():
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body={"syncToken": "*"}),
            HttpResponse(json.dumps({"items": []})),
        )
        source, catalog = _source_and_catalog(_manifest(initial_token="*"))
        messages = _read(source, catalog)

    assert all("token" not in message.stream.stream_state.__dict__ for message in _state(messages))


def test_t_inert_leaves_page_observer_unset_for_datetime_cursor():
    manifest = _manifest()
    manifest["streams"][0]["incremental_sync"] = {
        "type": "DatetimeBasedCursor",
        "cursor_field": "created_at",
        "start_datetime": "2020-01-01T00:00:00Z",
        "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
    }
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)

    stream = source.streams(config={})[0]
    assert isinstance(stream, DefaultStream)
    generator = stream._stream_partition_generator
    assert isinstance(generator, StreamSlicerPartitionGenerator)
    retriever = generator._partition_factory._retriever
    assert isinstance(retriever, SimpleRetriever)
    assert retriever.page_observer is None


@pytest.mark.parametrize("cursor_kind", ["datetime", "count", "none"])
def test_t_inert_cursor_types_read_without_opaque_page_observer(cursor_kind):
    manifest = _manifest()
    if cursor_kind == "datetime":
        manifest["streams"][0]["incremental_sync"] = {
            "type": "DatetimeBasedCursor",
            "cursor_field": "created_at",
            "start_datetime": "2020-01-01T00:00:00Z",
            "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
        }
    elif cursor_kind == "count":
        manifest["streams"][0]["incremental_sync"] = {
            "type": "IncrementingCountCursor",
            "cursor_field": "id",
            "start_value": 0,
        }
    else:
        manifest["streams"][0].pop("incremental_sync")

    sync_mode = SyncMode.full_refresh if cursor_kind == "none" else SyncMode.incremental
    with HttpMocker() as http_mocker:
        http_mocker.post(
            HttpRequest(_URL, body=None),
            HttpResponse(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": 1 if cursor_kind == "count" else "1",
                                "created_at": "2020-01-02T00:00:00Z",
                            }
                        ]
                    }
                )
            ),
        )
        source, catalog = _source_and_catalog(manifest, sync_mode=sync_mode)
        messages = _read(source, catalog)

    assert any(message.type == Type.RECORD for message in messages)
    stream = source.streams(config={})[0]
    assert isinstance(stream, DefaultStream)
    generator = stream._stream_partition_generator
    assert isinstance(generator, StreamSlicerPartitionGenerator)
    retriever = generator._partition_factory._retriever
    assert isinstance(retriever, SimpleRetriever)
    assert retriever.page_observer is None
