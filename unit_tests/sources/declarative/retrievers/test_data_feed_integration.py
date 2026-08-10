#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
End-to-end coverage for data feed streams (`is_data_feed: true`).

A data feed returns records in descending cursor order, so the sync must stop paginating on the
first page that contains a record the cursor considers already synced, and must not emit the
already-synced records sitting at the tail of that page.
"""

import json
import logging
from typing import Any, List, Mapping, Optional, Tuple

import pytest
import requests_mock

from airbyte_cdk.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    ConfiguredAirbyteCatalogSerializer,
    StreamDescriptor,
    Type,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)

_STREAM_NAME = "items"


def _manifest(
    is_client_side_incremental: bool = False, partition_router: Optional[Mapping[str, Any]] = None
) -> Mapping[str, Any]:
    incremental_sync: dict[str, Any] = {
        "type": "DatetimeBasedCursor",
        "cursor_field": "updated_at",
        "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
        "start_datetime": {
            "type": "MinMaxDatetime",
            "datetime": "2019-01-01T00:00:00Z",
            "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
        },
        "is_data_feed": True,
    }
    if is_client_side_incremental:
        incremental_sync["is_client_side_incremental"] = True

    retriever: dict[str, Any] = {
        "type": "SimpleRetriever",
        "requester": {
            "type": "HttpRequester",
            "url_base": "https://api.example.com",
            "path": "/items" if partition_router is None else "/{{ stream_partition.owner }}/items",
            "http_method": "GET",
        },
        "record_selector": {
            "type": "RecordSelector",
            "extractor": {"type": "DpathExtractor", "field_path": []},
        },
        "paginator": {
            "type": "DefaultPaginator",
            "pagination_strategy": {
                "type": "PageIncrement",
                "page_size": 4,
                "start_from_page": 1,
                "inject_on_first_request": True,
            },
            "page_token_option": {
                "type": "RequestOption",
                "inject_into": "request_parameter",
                "field_name": "page",
            },
        },
    }
    if partition_router:
        retriever["partition_router"] = partition_router

    return {
        "version": "7.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": [_STREAM_NAME]},
        "spec": {
            "type": "Spec",
            "connection_specification": {"type": "object", "properties": {}},
        },
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": _STREAM_NAME,
                "primary_key": ["id"],
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "updated_at": {"type": "string"},
                        },
                    },
                },
                "retriever": retriever,
                "incremental_sync": incremental_sync,
            }
        ],
    }


# Records are sorted in descending order of updated_at, as expected from a data feed. With a cursor
# of 2021-01-01, page 1 holds two fresh records followed by two already-synced ones.
_PAGE_1 = [
    {"id": "4", "updated_at": "2022-06-01T00:00:00Z"},
    {"id": "3", "updated_at": "2022-05-01T00:00:00Z"},
    {"id": "2", "updated_at": "2020-06-01T00:00:00Z"},
    {"id": "1", "updated_at": "2020-05-01T00:00:00Z"},
]
_PAGE_2 = [
    {"id": "0", "updated_at": "2020-04-01T00:00:00Z"},
]

_CATALOG = ConfiguredAirbyteCatalogSerializer.load(
    {
        "streams": [
            {
                "stream": {
                    "name": _STREAM_NAME,
                    "json_schema": {},
                    "supported_sync_modes": ["full_refresh", "incremental"],
                },
                "sync_mode": "incremental",
                "destination_sync_mode": "append",
            }
        ]
    }
)


def _state(stream_state: Mapping[str, Any]) -> List[AirbyteStateMessage]:
    return [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name=_STREAM_NAME),
                stream_state=AirbyteStateBlob(stream_state),
            ),
        )
    ]


def _read(
    manifest: Mapping[str, Any],
    state: Optional[List[AirbyteStateMessage]],
    pages_per_partition: Optional[Mapping[Tuple[str, str], List[Mapping[str, Any]]]] = None,
    pages: Optional[Mapping[str, List[Mapping[str, Any]]]] = None,
) -> Tuple[List[str], List[str]]:
    pages_fetched = []

    def paged_response(request: Any, context: Any) -> str:
        page = request.qs.get("page", ["1"])[0]
        if pages_per_partition is None:
            pages_fetched.append(page)
            if pages is not None:
                return json.dumps(pages.get(page, []))
            return json.dumps(_PAGE_1 if page == "1" else _PAGE_2)
        owner = request.path.strip("/").split("/")[0]
        pages_fetched.append(f"{owner}:{page}")
        return json.dumps(pages_per_partition.get((owner, page), []))

    source = ConcurrentDeclarativeSource(
        source_config=manifest, config={}, catalog=_CATALOG, state=state
    )
    with requests_mock.Mocker() as http_mocker:
        http_mocker.get(requests_mock.ANY, text=paged_response)
        records = [
            message.record.data
            for message in source.read(logging.getLogger("test"), {}, _CATALOG, state)
            if message.type == Type.RECORD
        ]
    return sorted(pages_fetched), sorted(record["id"] for record in records)


@pytest.mark.parametrize("is_client_side_incremental", [False, True])
def test_given_already_synced_records_on_page_then_stop_paginating_and_filter_them_out(
    is_client_side_incremental: bool,
) -> None:
    pages_fetched, record_ids = _read(
        _manifest(is_client_side_incremental=is_client_side_incremental),
        _state({"updated_at": "2021-01-01T00:00:00Z"}),
    )

    assert pages_fetched == ["1"]
    assert record_ids == ["3", "4"]


@pytest.mark.parametrize("is_client_side_incremental", [False, True])
def test_given_no_already_synced_records_then_paginate_until_the_end(
    is_client_side_incremental: bool,
) -> None:
    pages_fetched, record_ids = _read(
        _manifest(is_client_side_incremental=is_client_side_incremental), None
    )

    assert pages_fetched == ["1", "2"]
    assert record_ids == ["0", "1", "2", "3", "4"]


def test_given_record_dated_in_the_future_then_filter_it_out() -> None:
    """
    The retriever drops the records the cursor would not sync, and `should_be_synced` is bounded on both ends: with no
    `end_datetime` the upper bound is `now()`, so records dated ahead of the connector's clock are dropped too. This is
    the behaviour `is_client_side_incremental` has always had, and a data feed now matches it.
    """
    pages_fetched, record_ids = _read(
        _manifest(),
        _state({"updated_at": "2021-01-01T00:00:00Z"}),
        pages={
            "1": [
                {"id": "future", "updated_at": "2099-01-01T00:00:00Z"},
                {"id": "fresh", "updated_at": "2022-06-01T00:00:00Z"},
                {"id": "already_synced", "updated_at": "2020-06-01T00:00:00Z"},
            ]
        },
    )

    # the forward-dated record does not stop the pagination, it is only left out of the emitted records
    assert pages_fetched == ["1"]
    assert record_ids == ["fresh"]


def test_given_multiple_partitions_then_each_partition_stops_on_its_own_cursor() -> None:
    """
    A single retriever instance is shared by every partition and partitions are read concurrently,
    so the boundary of one partition must not influence another.
    """
    pages_per_partition = {
        ("a", "1"): [{**record, "id": f"a{record['id']}"} for record in _PAGE_1],
        ("a", "2"): [{**record, "id": f"a{record['id']}"} for record in _PAGE_2],
        ("b", "1"): [{**record, "id": f"b{record['id']}"} for record in _PAGE_1],
        ("b", "2"): [{**record, "id": f"b{record['id']}"} for record in _PAGE_2],
    }
    manifest = _manifest(
        partition_router={
            "type": "ListPartitionRouter",
            "values": ["a", "b"],
            "cursor_field": "owner",
        }
    )
    state = _state(
        {
            "use_global_cursor": False,
            "states": [
                # partition "a" has already synced everything before 2021 hence it stops on page 1
                {"partition": {"owner": "a"}, "cursor": {"updated_at": "2021-01-01T00:00:00Z"}},
                # partition "b" has nothing already synced hence it reads both pages
                {"partition": {"owner": "b"}, "cursor": {"updated_at": "2019-01-01T00:00:00Z"}},
            ],
        }
    )

    pages_fetched, record_ids = _read(manifest, state, pages_per_partition)

    assert pages_fetched == ["a:1", "b:1", "b:2"]
    assert record_ids == ["a3", "a4", "b0", "b1", "b2", "b3", "b4"]
