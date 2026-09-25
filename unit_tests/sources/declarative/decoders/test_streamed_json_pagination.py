#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import io
import json

import pytest
import requests
import urllib3

from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import (
    CompositeRawDecoder,
    GzipParser,
    JsonItemsParser,
)
from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from airbyte_cdk.sources.declarative.decoders.pagination_decoder_decorator import (
    PaginationDecoderDecorator,
)
from airbyte_cdk.sources.streams.http.streamed_response import (
    get_document_remainder,
    set_document_remainder,
)


def _streamed_response(body: bytes, headers=None) -> requests.Response:
    raw = urllib3.HTTPResponse(
        body=io.BytesIO(body),
        headers=headers or {"Content-Type": "application/json"},
        status=200,
        preload_content=False,
    )
    response = requests.Response()
    response.status_code = 200
    response.raw = raw
    response.url = "https://airbyte.io/"
    return response


def test_json_items_parser_reports_document_remainder():
    body = b'{"a":1,"tickets":[{"id":1},{"id":2}],"after_url":"x","end_of_stream":false}'
    remainders = []
    records = list(
        JsonItemsParser(items_path="tickets").parse(
            io.BytesIO(body), on_document_remainder=remainders.append
        )
    )
    assert records == [{"id": 1}, {"id": 2}]
    assert remainders == [{"a": 1, "tickets": [], "after_url": "x", "end_of_stream": False}]


def test_json_items_parser_remainder_nested_path():
    body = b'{"data":{"items":[{"id":1}],"meta":{"c":2}},"z":3}'
    remainders = []
    records = list(
        JsonItemsParser(items_path="data.items").parse(
            io.BytesIO(body), on_document_remainder=remainders.append
        )
    )
    assert records == [{"id": 1}]
    assert remainders == [{"data": {"items": [], "meta": {"c": 2}}, "z": 3}]


def test_gzip_parser_forwards_remainder_kwarg():
    import gzip

    body = gzip.compress(b'{"data":[{"id":1}],"after_url":"x"}')
    remainders = []
    records = list(
        GzipParser(inner_parser=JsonItemsParser(items_path="data")).parse(
            io.BytesIO(body), on_document_remainder=remainders.append
        )
    )
    assert records == [{"id": 1}]
    assert remainders == [{"data": [], "after_url": "x"}]


def test_json_items_parser_without_remainder_kwarg_unchanged():
    body = b'{"data":[{"id":1},{"id":2}]}'
    assert list(JsonItemsParser(items_path="data").parse(io.BytesIO(body))) == [
        {"id": 1},
        {"id": 2},
    ]


def test_composite_raw_decoder_sets_remainder_on_streamed_response():
    body = json.dumps(
        {"tickets": [{"id": 1}, {"id": 2}], "after_url": "x", "end_of_stream": False}
    ).encode()
    response = _streamed_response(body)
    decoder = CompositeRawDecoder(parser=JsonItemsParser(items_path="tickets"))
    assert list(decoder.decode(response)) == [{"id": 1}, {"id": 2}]
    assert get_document_remainder(response) == {
        "tickets": [],
        "after_url": "x",
        "end_of_stream": False,
    }


class _StreamingJsonDecoder(JsonDecoder):
    def is_stream_response(self) -> bool:
        return True


def test_pagination_decoder_decorator_yields_remainder_when_present():
    response = _streamed_response(b"{}")
    set_document_remainder(response, {"after_url": "x", "end_of_stream": False})
    decorator = PaginationDecoderDecorator(decoder=_StreamingJsonDecoder(parameters={}))
    assert next(decorator.decode(response)) == {"after_url": "x", "end_of_stream": False}


def test_pagination_decoder_decorator_yields_empty_and_warns_without_remainder(caplog):
    response = _streamed_response(b"{}")
    decorator = PaginationDecoderDecorator(decoder=_StreamingJsonDecoder(parameters={}))
    with caplog.at_level("WARNING"):
        assert next(decorator.decode(response)) == {}
    assert "will not be decoded for pagination" in caplog.text


def test_pagination_decoder_decorator_delegates_when_not_streamed():
    body = b'{"data":[{"id":1}]}'
    response = requests.Response()
    response.status_code = 200
    response._content = body
    decorator = PaginationDecoderDecorator(decoder=JsonDecoder(parameters={}))
    assert next(decorator.decode(response)) == {"data": [{"id": 1}]}
