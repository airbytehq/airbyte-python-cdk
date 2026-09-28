# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
from typing import Any, List, Mapping, MutableMapping, Optional

import pytest
import requests_mock

from airbyte_cdk.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    StreamDescriptor,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.catalog_builder import CatalogBuilder, ConfiguredAirbyteStreamBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read

_STATE_VALUE = 1714000000
_DAY = 86400
_START = _STATE_VALUE - 30 * _DAY
_CONFIG = {"start_date": str(_START)}
_URL_BASE = "https://api.example.com"
_STREAM_NAME = "conversation_parts"

_CURSOR: Mapping[str, Any] = {
    "type": "DatetimeBasedCursor",
    "cursor_field": "updated_at",
    "datetime_format": "%s",
    "cursor_datetime_formats": ["%s"],
    "start_datetime": {
        "type": "MinMaxDatetime",
        "datetime": "{{ config['start_date'] }}",
        "datetime_format": "%s",
    },
    "lookback_window": "P1D",
    "is_client_side_incremental": True,
}

_IN_LOOKBACK = {"id": "in_lookback", "updated_at": _STATE_VALUE - 3600}
_NEWER = {"id": "newer", "updated_at": _STATE_VALUE + 10}
_OLDER_THAN_LOOKBACK = {"id": "older_than_lookback", "updated_at": _STATE_VALUE - 2 * _DAY}
_BEFORE_START = {"id": "before_start", "updated_at": _START - 3600}


def _retriever(path: str, field_path: str) -> MutableMapping[str, Any]:
    return {
        "type": "SimpleRetriever",
        "requester": {
            "type": "HttpRequester",
            "url": f"{_URL_BASE}{path}",
            "http_method": "GET",
        },
        "record_selector": {
            "type": "RecordSelector",
            "extractor": {"type": "DpathExtractor", "field_path": [field_path]},
        },
    }


def _manifest(kind: str, incremental_parent: bool = False) -> Mapping[str, Any]:
    child_cursor = dict(_CURSOR)
    if kind == "global":
        child_cursor["global_substream_cursor"] = True

    if kind == "plain":
        retriever = _retriever("/parts", "parts")
    else:
        parent: MutableMapping[str, Any] = {
            "type": "DeclarativeStream",
            "name": "conversations",
            "primary_key": ["id"],
            "schema_loader": {"type": "InlineSchemaLoader", "schema": {}},
            "retriever": _retriever("/conversations", "data"),
        }
        parent_config: MutableMapping[str, Any] = {
            "type": "ParentStreamConfig",
            "stream": parent,
            "parent_key": "id",
            "partition_field": "id",
        }
        if incremental_parent:
            parent["incremental_sync"] = dict(_CURSOR)
            parent_config["incremental_dependency"] = True
        retriever = _retriever("/conversations/{{ stream_slice.id }}", "parts")
        retriever["partition_router"] = {
            "type": "SubstreamPartitionRouter",
            "parent_stream_configs": [parent_config],
        }

    return {
        "version": "6.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": [_STREAM_NAME]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": _STREAM_NAME,
                "primary_key": ["id"],
                "schema_loader": {"type": "InlineSchemaLoader", "schema": {}},
                "retriever": retriever,
                "incremental_sync": child_cursor,
            }
        ],
        "spec": {
            "type": "Spec",
            "connection_specification": {
                "type": "object",
                "properties": {"start_date": {"type": "string"}},
            },
        },
    }


def _state(kind: str) -> MutableMapping[str, Any]:
    if kind == "plain":
        return {"updated_at": str(_STATE_VALUE)}
    state: MutableMapping[str, Any] = {
        "use_global_cursor": kind == "global",
        "state": {"updated_at": str(_STATE_VALUE)},
        "lookback_window": 60,
    }
    if kind == "per_partition":
        state["states"] = [
            {
                "partition": {"id": 1, "parent_slice": {}},
                "cursor": {"updated_at": str(_STATE_VALUE)},
            }
        ]
    return state


def _read(
    manifest: Mapping[str, Any],
    state: Optional[Mapping[str, Any]],
    responses: Mapping[str, Any],
) -> tuple[EntrypointOutput, List[str]]:
    state_messages = (
        [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name=_STREAM_NAME),
                    stream_state=AirbyteStateBlob(state),
                ),
            )
        ]
        if state
        else None
    )
    catalog = (
        CatalogBuilder()
        .with_stream(ConfiguredAirbyteStreamBuilder().with_name(_STREAM_NAME))
        .build()
    )
    with requests_mock.Mocker() as m:
        for path, body in responses.items():
            m.get(f"{_URL_BASE}{path}", json=body)
        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=_CONFIG, catalog=catalog, state=state_messages
        )
        output = read(source, _CONFIG, catalog, state_messages)
        requested_paths = sorted({request.path for request in m.request_history})
    assert not output.errors, output.errors
    return output, requested_paths


def _record_ids(output: EntrypointOutput) -> List[str]:
    return sorted(message.record.data["id"] for message in output.records)


def _final_state(output: EntrypointOutput) -> Mapping[str, Any]:
    return output.state_messages[-1].state.stream.stream_state.__dict__


def _parts_responses(parts: List[Mapping[str, Any]]) -> Mapping[str, Any]:
    return {
        "/parts": {"parts": parts},
        "/conversations": {"data": [{"id": 1}]},
        "/conversations/1": {"parts": parts},
    }


@pytest.mark.parametrize("kind", ["plain", "per_partition", "global"])
@pytest.mark.parametrize(
    "with_newer_record, expected_state_value",
    [(True, _STATE_VALUE + 10), (False, _STATE_VALUE)],
    ids=["with_newer_record", "only_older_records"],
)
def test_given_state_and_lookback_window_when_read_then_emit_records_within_lookback(
    kind: str, with_newer_record: bool, expected_state_value: int
) -> None:
    parts = [_IN_LOOKBACK, _OLDER_THAN_LOOKBACK, _BEFORE_START] + (
        [_NEWER] if with_newer_record else []
    )

    output, _ = _read(_manifest(kind), _state(kind), _parts_responses(parts))

    assert _record_ids(output) == sorted(["in_lookback"] + (["newer"] if with_newer_record else []))
    final_state = _final_state(output)
    cursor_state = final_state if kind == "plain" else final_state["state"]
    assert int(cursor_state["updated_at"]) == expected_state_value
    if kind == "per_partition":
        assert int(final_state["states"][0]["cursor"]["updated_at"]) == expected_state_value


@pytest.mark.parametrize("kind", ["plain", "per_partition", "global"])
def test_given_no_state_and_lookback_window_when_read_then_drop_records_before_start(
    kind: str,
) -> None:
    parts = [_IN_LOOKBACK, _NEWER, _OLDER_THAN_LOOKBACK, _BEFORE_START]

    output, _ = _read(_manifest(kind), None, _parts_responses(parts))

    assert _record_ids(output) == ["in_lookback", "newer", "older_than_lookback"]


@pytest.mark.parametrize("kind", ["global", "per_partition"])
@pytest.mark.parametrize(
    "with_newer_parent", [True, False], ids=["with_newer_parent", "only_parent_in_lookback"]
)
def test_given_incremental_parent_with_lookback_when_read_then_parent_record_within_lookback_produces_child_records(
    kind: str, with_newer_parent: bool
) -> None:
    conversations = [{"id": 1, "updated_at": _STATE_VALUE - 3600}] + (
        [{"id": 2, "updated_at": _STATE_VALUE + 10}] if with_newer_parent else []
    )
    state = _state(kind)
    state["parent_state"] = {"conversations": {"updated_at": str(_STATE_VALUE)}}

    output, requested_paths = _read(
        _manifest(kind, incremental_parent=True),
        state,
        {
            "/conversations": {"data": conversations},
            "/conversations/1": {"parts": [{"id": "p1", "updated_at": _STATE_VALUE - 3600}]},
            "/conversations/2": {"parts": [{"id": "p2", "updated_at": _STATE_VALUE + 10}]},
        },
    )

    assert "/conversations/1" in requested_paths
    assert "p1" in _record_ids(output)
    final_state = _final_state(output)
    assert int(final_state["state"]["updated_at"]) >= _STATE_VALUE
    assert int(final_state["parent_state"]["conversations"]["updated_at"]) >= _STATE_VALUE
    if kind == "per_partition":
        partition_cursor = next(
            partition_state["cursor"]
            for partition_state in final_state["states"]
            if partition_state["partition"]["id"] == 1
        )
        assert int(partition_cursor["updated_at"]) >= _STATE_VALUE
