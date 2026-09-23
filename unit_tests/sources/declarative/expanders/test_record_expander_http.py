#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import json
from copy import deepcopy
from typing import Any, Dict, List
from unittest.mock import MagicMock

from airbyte_cdk.models import ConfiguredAirbyteCatalog, ConfiguredAirbyteStream, Type
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse

_CONFIG: Dict[str, Any] = {}
_URL_BASE = "https://api.test.com/v1"

_MANIFEST: Dict[str, Any] = {
    "version": "6.0.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["invoice_line_items"]},
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "invoice_line_items",
            "primary_key": ["id"],
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "http://json-schema.org/schema#",
                    "type": "object",
                    "properties": {"id": {"type": "string"}},
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": _URL_BASE,
                    "path": "events",
                    "http_method": "GET",
                    "request_parameters": {"types[]": "invoice.created"},
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {
                        "type": "DpathExtractor",
                        "field_path": ["data"],
                        "record_expander": {
                            "type": "RecordExpander",
                            "expand_records_from_field": ["data", "object", "lines", "data"],
                            "truncation_indicator_path": ["data", "object", "lines", "has_more"],
                            "truncated_list_retriever": {
                                "type": "SimpleRetriever",
                                "requester": {
                                    "type": "HttpRequester",
                                    "url_base": _URL_BASE,
                                    "path": "invoices/{{ stream_slice['parent_record']['data']['object']['id'] }}/lines",
                                    "http_method": "GET",
                                },
                                "record_selector": {
                                    "type": "RecordSelector",
                                    "extractor": {"type": "DpathExtractor", "field_path": ["data"]},
                                },
                                "paginator": {
                                    "type": "DefaultPaginator",
                                    "page_token_option": {
                                        "type": "RequestOption",
                                        "inject_into": "request_parameter",
                                        "field_name": "starting_after",
                                    },
                                    "pagination_strategy": {
                                        "type": "CursorPagination",
                                        "cursor_value": '{{ response["data"][-1]["id"] }}',
                                        "stop_condition": '{{ not response.get("has_more", False) }}',
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
    ],
    "spec": {
        "type": "Spec",
        "connection_specification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {},
            "additionalProperties": True,
        },
    },
}


def _invoice_event(invoice_id: str, embedded: List[str], has_more: bool, total_count: int):
    return {
        "id": f"evt_{invoice_id}",
        "type": "invoice.created",
        "data": {
            "object": {
                "id": invoice_id,
                "object": "invoice",
                "lines": {
                    "object": "list",
                    "data": [{"id": line_id, "object": "line_item"} for line_id in embedded],
                    "has_more": has_more,
                    "total_count": total_count,
                    "url": f"/v1/invoices/{invoice_id}/lines",
                },
            }
        },
    }


def _lines_page(line_ids: List[str], has_more: bool):
    return HttpResponse(
        body=json.dumps(
            {
                "object": "list",
                "data": [{"id": line_id, "object": "line_item"} for line_id in line_ids],
                "has_more": has_more,
            }
        )
    )


def _read_records(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    source = ConcurrentDeclarativeSource(
        source_config=manifest, config=_CONFIG, catalog=None, state=None
    )
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=stream, sync_mode="full_refresh", destination_sync_mode="overwrite"
            )
            for stream in source.discover(logger=source.logger, config=_CONFIG).streams
        ]
    )
    return [
        message.record.data
        for message in source.read(logger=MagicMock(), config=_CONFIG, catalog=catalog, state=None)
        if message.type == Type.RECORD
    ]


def test_truncated_nested_list_is_fetched_over_http_with_pagination():
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url=f"{_URL_BASE}/events", query_params={"types[]": "invoice.created"}),
            HttpResponse(
                body=json.dumps(
                    {
                        "object": "list",
                        "data": [
                            _invoice_event("in_1", ["il_1", "il_2"], has_more=True, total_count=5),
                            _invoice_event("in_2", ["il_6"], has_more=False, total_count=1),
                        ],
                        "has_more": False,
                    }
                )
            ),
        )
        first_page_request = HttpRequest(url=f"{_URL_BASE}/invoices/in_1/lines")
        second_page_request = HttpRequest(
            url=f"{_URL_BASE}/invoices/in_1/lines", query_params={"starting_after": "il_3"}
        )
        http_mocker.get(first_page_request, _lines_page(["il_1", "il_2", "il_3"], has_more=True))
        http_mocker.get(second_page_request, _lines_page(["il_4", "il_5"], has_more=False))

        records = _read_records(_MANIFEST)

        assert [record["id"] for record in records] == [
            "il_1",
            "il_2",
            "il_3",
            "il_4",
            "il_5",
            "il_6",
        ]
        http_mocker.assert_number_of_calls(first_page_request, 1)
        http_mocker.assert_number_of_calls(second_page_request, 1)


def test_stream_parameters_propagate_into_truncated_list_requester():
    """Request-shaping options declared in the stream's `$parameters` also reach the nested requester,
    which is why the docs recommend declaring them on the outer requester's `request_parameters`."""
    with HttpMocker() as http_mocker:
        manifest = deepcopy(_MANIFEST)
        stream = manifest["streams"][0]
        del stream["retriever"]["requester"]["request_parameters"]
        stream["$parameters"] = {"request_parameters": {"types[]": "invoice.created"}}

        http_mocker.get(
            HttpRequest(url=f"{_URL_BASE}/events", query_params={"types[]": "invoice.created"}),
            HttpResponse(
                body=json.dumps(
                    {
                        "object": "list",
                        "data": [_invoice_event("in_1", ["il_1"], has_more=True, total_count=2)],
                        "has_more": False,
                    }
                )
            ),
        )
        http_mocker.get(
            HttpRequest(
                url=f"{_URL_BASE}/invoices/in_1/lines", query_params={"types[]": "invoice.created"}
            ),
            _lines_page(["il_1", "il_2"], has_more=False),
        )

        records = _read_records(manifest)

        assert [record["id"] for record in records] == ["il_1", "il_2"]
