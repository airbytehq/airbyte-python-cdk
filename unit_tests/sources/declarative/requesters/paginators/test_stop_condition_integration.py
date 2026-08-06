#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
End-to-end coverage for the pagination stop condition of data feed streams that also use
client-side incremental filtering. The client-side filter drops records older than the cursor
before the paginator can observe them, so the stop condition is driven by the filter itself:
pagination must stop on the first page containing a record older than the cursor, while still
emitting only the records newer than the cursor.
"""

import json
import logging
from typing import Any, List, Mapping, Optional

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

_MANIFEST = {
    "version": "7.0.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["items"]},
    "spec": {
        "type": "Spec",
        "connection_specification": {"type": "object", "properties": {}},
    },
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "items",
            "primary_key": ["id"],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "updated_at": {"type": "string"},
                    },
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.com",
                    "path": "/items",
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
            },
            "incremental_sync": {
                "type": "DatetimeBasedCursor",
                "cursor_field": "updated_at",
                "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
                "start_datetime": {
                    "type": "MinMaxDatetime",
                    "datetime": "2019-01-01T00:00:00Z",
                    "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
                },
                "is_data_feed": True,
                "is_client_side_incremental": True,
            },
        }
    ],
}

# Records are sorted in descending order of updated_at, as expected from a data feed
_PAGE_1 = [
    {"id": 4, "updated_at": "2022-06-01T00:00:00Z"},
    {"id": 3, "updated_at": "2022-05-01T00:00:00Z"},
    {"id": 2, "updated_at": "2020-06-01T00:00:00Z"},
    {"id": 1, "updated_at": "2020-05-01T00:00:00Z"},
]
_PAGE_2 = [
    {"id": 0, "updated_at": "2020-04-01T00:00:00Z"},
]

_CATALOG = ConfiguredAirbyteCatalogSerializer.load(
    {
        "streams": [
            {
                "stream": {
                    "name": "items",
                    "json_schema": {},
                    "supported_sync_modes": ["full_refresh", "incremental"],
                },
                "sync_mode": "incremental",
                "destination_sync_mode": "append",
            }
        ]
    }
)


def _read(state: Optional[List[AirbyteStateMessage]]) -> tuple[List[str], List[Mapping[str, Any]]]:
    pages_fetched = []

    def paged_response(request: Any, context: Any) -> str:
        page = request.qs.get("page", ["1"])[0]
        pages_fetched.append(page)
        return json.dumps(_PAGE_1 if page == "1" else _PAGE_2)

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config={}, catalog=_CATALOG, state=state
    )
    with requests_mock.Mocker() as http_mocker:
        http_mocker.get("https://api.example.com/items", text=paged_response)
        records = [
            message.record.data
            for message in source.read(logging.getLogger("test"), {}, _CATALOG, state)
            if message.type == Type.RECORD
        ]
    return pages_fetched, records


def test_given_stale_records_on_page_when_client_side_incremental_then_stop_pagination():
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="items"),
                stream_state=AirbyteStateBlob({"updated_at": "2021-01-01T00:00:00Z"}),
            ),
        )
    ]

    pages_fetched, records = _read(state)

    assert sorted(record["id"] for record in records) == [3, 4]
    assert pages_fetched == ["1"]


def test_given_no_stale_records_when_client_side_incremental_then_paginate_until_the_end():
    pages_fetched, records = _read(None)

    assert sorted(record["id"] for record in records) == [0, 1, 2, 3, 4]
    assert pages_fetched == ["1", "2"]
