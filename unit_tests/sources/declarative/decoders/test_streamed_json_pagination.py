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


import gzip
import tempfile

from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import (
    CsvParser,
    JsonLineParser,
    JsonParser,
)
from airbyte_cdk.sources.streams.http.streamed_response import SpooledResponseBody


def _spooled_body(body: bytes, max_size: int) -> SpooledResponseBody:
    spool = tempfile.SpooledTemporaryFile(max_size=max_size)
    spool.write(body)
    spool.seek(0)
    return SpooledResponseBody(spool)


_JSON_BODY = json.dumps({"items": [{"id": 1}, {"id": 2}], "after_url": "x"}).encode()


@pytest.mark.parametrize(
    "parser, body, expected",
    [
        (JsonParser(), _JSON_BODY, [_JSON_BODY]),
        (JsonItemsParser(items_path="items"), _JSON_BODY, [{"id": 1}, {"id": 2}]),
        (JsonLineParser(), b'{"id":1}\n{"id":2}\n', [{"id": 1}, {"id": 2}]),
        (CsvParser(), b"a,b\n1,2\n", [{"a": "1", "b": "2"}]),
        (
            GzipParser(inner_parser=JsonItemsParser(items_path="items")),
            gzip.compress(_JSON_BODY),
            [{"id": 1}, {"id": 2}],
        ),
    ],
    ids=["json", "json_items", "json_lines", "csv", "gzip_json_items"],
)
@pytest.mark.parametrize("max_size", [64 << 10, 1], ids=["in_memory", "rolled_to_disk"])
def test_parsers_read_from_spooled_response_body(parser, body, expected, max_size):
    raw = _spooled_body(body, max_size=max_size)
    if parser.__class__ is JsonParser:
        assert list(parser.parse(raw)) == [json.loads(body)]
    else:
        assert list(parser.parse(raw)) == expected
    raw.close()


def test_spooled_response_body_small_body_stays_in_memory():
    spool = tempfile.SpooledTemporaryFile(max_size=8 << 20)
    spool.write(b"hello")
    spool.seek(0)
    raw = SpooledResponseBody(spool)
    assert raw.read() == b"hello"
    assert spool._rolled is False


def test_streamed_decode_applies_decode_content_for_transport_gzip():
    body = gzip.compress(_JSON_BODY)
    response = _streamed_response(body, headers={"Content-Encoding": "gzip"})
    decoder = CompositeRawDecoder(parser=JsonItemsParser(items_path="items"))
    assert list(decoder.decode(response)) == [{"id": 1}, {"id": 2}]


def test_streamed_gzip_decoder_over_double_compressed_body():
    body = gzip.compress(gzip.compress(_JSON_BODY))
    response = _streamed_response(body, headers={"Content-Encoding": "gzip"})
    decoder = CompositeRawDecoder(
        parser=GzipParser(inner_parser=JsonItemsParser(items_path="items"))
    )
    assert list(decoder.decode(response)) == [{"id": 1}, {"id": 2}]


def test_streamed_gzip_decoder_over_plain_gzip_payload_unchanged():
    body = gzip.compress(_JSON_BODY)
    response = _streamed_response(body)
    decoder = CompositeRawDecoder(
        parser=GzipParser(inner_parser=JsonItemsParser(items_path="items"))
    )
    assert list(decoder.decode(response)) == [{"id": 1}, {"id": 2}]
