#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import csv
import gzip
import json
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO, StringIO
from threading import Thread
from typing import ClassVar, Iterable, List
from unittest.mock import Mock, patch

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import (
    CompositeRawDecoder,
    CsvParser,
    GzipParser,
    JsonLineParser,
    JsonParser,
)
from airbyte_cdk.utils import AirbyteTracedException


def find_available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]  # type: ignore  # this should return a int


def compress_with_gzip(data: str, encoding: str = "utf-8"):
    """
    Compress the data using Gzip.
    """
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(data.encode(encoding))
    return buf.getvalue()


def generate_csv(
    encoding: str = "utf-8",
    delimiter: str = ",",
    should_compress: bool = False,
    add_extra_column: bool = False,
    extra_column_value: str = "",
) -> bytes:
    data = [
        {"id": "1", "name": "John", "age": "28"},
        {"id": "2", "name": "Alice", "age": "34"},
        {"id": "3", "name": "Bob", "age": "25"},
    ]
    fieldnames = ["id", "name", "age"]
    if add_extra_column:
        for row in data:
            row["gender"] = extra_column_value
        fieldnames.append("gender")

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=delimiter)
    writer.writeheader()
    for row in data:
        writer.writerow(row)

    output.seek(0)
    csv_data = output.read()

    if should_compress:
        return compress_with_gzip(csv_data, encoding=encoding)
    return csv_data.encode(encoding)


@pytest.mark.parametrize("encoding", ["utf-8", "utf", "iso-8859-1"])
def test_composite_raw_decoder_gzip_csv_parser(requests_mock, encoding: str):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        content=generate_csv(encoding=encoding, delimiter="\t", should_compress=True),
        headers={"Content-Encoding": "gzip"},
    )
    response = requests.get("https://airbyte.io/", stream=True)

    # the delimiter is set to `\\t` intentionally to test the parsing logic here
    parser = GzipParser(inner_parser=CsvParser(encoding=encoding, delimiter="\\t"))

    composite_raw_decoder = CompositeRawDecoder(parser=parser)
    counter = 0
    for _ in composite_raw_decoder.decode(response):
        counter += 1
    assert counter == 3


def generate_jsonlines() -> Iterable[str]:
    """
    Generator function to yield data in JSON Lines format.
    This is useful for streaming large datasets.
    """
    data = [
        {"id": 1, "message": "Hello, World!"},
        {"id": 2, "message": "Welcome to JSON Lines"},
        {"id": 3, "message": "Streaming data is fun!"},
    ]
    for item in data:
        yield json.dumps(item) + "\n"  # Serialize as JSON Lines


def generate_compressed_jsonlines(encoding: str = "utf-8") -> bytes:
    """
    Generator to compress the entire response content with Gzip and encode it.
    """
    json_lines_content = "".join(generate_jsonlines())
    compressed_data = compress_with_gzip(json_lines_content, encoding=encoding)
    return compressed_data


@pytest.mark.parametrize("encoding", ["utf-8", "utf", "iso-8859-1"])
def test_composite_raw_decoder_gzip_jsonline_parser(requests_mock, encoding: str):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        content=generate_compressed_jsonlines(encoding=encoding),
    )
    response = requests.get("https://airbyte.io/", stream=True)

    parser = GzipParser(inner_parser=JsonLineParser(encoding=encoding))
    composite_raw_decoder = CompositeRawDecoder(parser)
    counter = 0
    for _ in composite_raw_decoder.decode(response):
        counter += 1
    assert counter == 3


def test_given_header_match_when_decode_then_select_parser(requests_mock):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        content=generate_compressed_jsonlines(),
        headers={"Content-Encoding": "gzip"},
    )
    response = requests.get("https://airbyte.io/", stream=True)

    parser = GzipParser(inner_parser=JsonLineParser())
    unused_parser = Mock()
    composite_raw_decoder = CompositeRawDecoder.by_headers(
        [({"Content-Encoding"}, {"gzip"}, parser)],
        stream_response=True,
        fallback_parser=unused_parser,
    )
    counter = 0
    for _ in composite_raw_decoder.decode(response):
        counter += 1
    assert counter == 3


def test_given_header_does_not_match_when_decode_then_select_fallback_parser(requests_mock):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        content="".join(generate_jsonlines()).encode("utf-8"),
        headers={"Content-Encoding": "not gzip in order to expect fallback"},
    )
    response = requests.get("https://airbyte.io/", stream=True)

    unused_parser = GzipParser(inner_parser=Mock())
    composite_raw_decoder = CompositeRawDecoder.by_headers(
        [({"Content-Encoding"}, {"gzip"}, unused_parser)],
        stream_response=True,
        fallback_parser=JsonLineParser(),
    )
    counter = 0
    for _ in composite_raw_decoder.decode(response):
        counter += 1
    assert counter == 3


@pytest.mark.parametrize("encoding", ["utf-8", "utf", "iso-8859-1"])
def test_composite_raw_decoder_jsonline_parser(requests_mock, encoding: str):
    response_content = "".join(generate_jsonlines())
    requests_mock.register_uri(
        "GET", "https://airbyte.io/", content=response_content.encode(encoding=encoding)
    )
    response = requests.get("https://airbyte.io/", stream=True)
    composite_raw_decoder = CompositeRawDecoder(parser=JsonLineParser(encoding=encoding))
    counter = 0
    for _ in composite_raw_decoder.decode(response):
        counter += 1
    assert counter == 3


@pytest.mark.parametrize(
    "test_data",
    [
        ({"data-type": "string"}),
        ([{"id": "1"}, {"id": "2"}]),
        ({"id": "170141183460469231731687303715884105727"}),
        ({}),
        ({"nested": {"foo": {"bar": "baz"}}}),
    ],
    ids=[
        "valid_dict",
        "list_of_dicts",
        "int128",
        "empty_object",
        "nested_structure",
    ],
)
def test_composite_raw_decoder_json_parser(requests_mock, test_data):
    encodings = ["utf-8", "utf", "iso-8859-1"]
    for encoding in encodings:
        raw_data = json.dumps(test_data).encode(encoding=encoding)
        requests_mock.register_uri("GET", "https://airbyte.io/", content=raw_data)
        response = requests.get("https://airbyte.io/", stream=True)
        composite_raw_decoder = CompositeRawDecoder(parser=JsonParser(encoding=encoding))
        actual = list(composite_raw_decoder.decode(response))
        if isinstance(test_data, list):
            assert actual == test_data
        else:
            assert actual == [test_data]


def test_composite_raw_decoder_orjson_parser_error(requests_mock):
    raw_data = json.dumps({"test": "test"}).encode("utf-8")
    requests_mock.register_uri("GET", "https://airbyte.io/", content=raw_data)
    response = requests.get("https://airbyte.io/", stream=True)

    composite_raw_decoder = CompositeRawDecoder(parser=JsonParser(encoding="utf-8"))

    with patch("orjson.loads", side_effect=Exception("test")):
        assert [{"test": "test"}] == list(composite_raw_decoder.decode(response))


def test_composite_raw_decoder_raises_traced_exception_when_both_parsers_fail(requests_mock):
    raw_data = json.dumps({"test": "test"}).encode("utf-8")
    requests_mock.register_uri("GET", "https://airbyte.io/", content=raw_data)
    response = requests.get("https://airbyte.io/", stream=True)

    composite_raw_decoder = CompositeRawDecoder(parser=JsonParser(encoding="utf-8"))

    with patch("orjson.loads", side_effect=Exception("test")):
        with patch("json.loads", side_effect=Exception("test")):
            with pytest.raises(AirbyteTracedException):
                list(composite_raw_decoder.decode(response))


@pytest.mark.parametrize("encoding", ["utf-8", "utf", "iso-8859-1"])
@pytest.mark.parametrize("delimiter", [",", "\t", ";"])
def test_composite_raw_decoder_csv_parser_values(requests_mock, encoding: str, delimiter: str):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        content=generate_csv(encoding=encoding, delimiter=delimiter, should_compress=False),
    )
    response = requests.get("https://airbyte.io/", stream=True)

    parser = CsvParser(encoding=encoding, delimiter=delimiter)
    composite_raw_decoder = CompositeRawDecoder(parser=parser)

    expected_data = [
        {"id": "1", "name": "John", "age": "28"},
        {"id": "2", "name": "Alice", "age": "34"},
        {"id": "3", "name": "Bob", "age": "25"},
    ]

    parsed_records = list(composite_raw_decoder.decode(response))
    assert parsed_records == expected_data


@pytest.mark.parametrize("set_values_to_none", [None, [""], ["--"]])
def test_composite_raw_decoder_parse_empty_strings(
    requests_mock, set_values_to_none: List[str] | None
):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        content=generate_csv(
            should_compress=False,
            add_extra_column=True,
            extra_column_value=set_values_to_none[0] if set_values_to_none else "random_value",
        ),
    )
    response = requests.get("https://airbyte.io/", stream=True)

    parser = CsvParser(set_values_to_none=set_values_to_none)
    composite_raw_decoder = CompositeRawDecoder(parser=parser)

    expected_data = [
        {"id": "1", "name": "John", "age": "28"},
        {"id": "2", "name": "Alice", "age": "34"},
        {"id": "3", "name": "Bob", "age": "25"},
    ]
    for expected_record in expected_data:
        expected_record["gender"] = None if set_values_to_none else "random_value"

    parsed_records = list(composite_raw_decoder.decode(response))
    assert parsed_records == expected_data


class TestServer(BaseHTTPRequestHandler):
    __test__: ClassVar[bool] = False  # Tell Pytest this is not a Pytest class, despite its name

    def do_GET(self) -> None:
        self.send_response(200)
        self.end_headers()
        self.wfile.write(bytes("col1,col2\nval1,val2", "utf-8"))


def test_composite_raw_decoder_csv_parser_without_mocked_response():
    """
    This test reproduce a `ValueError: I/O operation on closed file` error we had with CSV parsing. We could not catch this with other tests because the closing of the mocked response from requests_mock was not the same as the one in requests.

    We first identified this issue while working with the sample defined https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv.
    This should be reproducible by having the test server return the `self.wfile.write` statement as a comment below but it does not. However, it wasn't reproducible.

    Currently we use `self.wfile.write(bytes("col1,col2\nval1,val2", "utf-8"))` to reproduce which we know is not a valid csv as it does not end with a newline character. However, this is the only we were able to reproduce locally.
    """
    # self.wfile.write(bytes('John,Doe,120 jefferson st.,Riverside, NJ, 08075\nJack,McGinnis,220 hobo Av.,Phila, PA,09119\n"John ""Da Man""",Repici,120 Jefferson St.,Riverside, NJ,08075\nStephen,Tyler,"7452 Terrace ""At the Plaza"" road",SomeTown,SD, 91234\n,Blankman,,SomeTown, SD, 00298\n"Joan ""the bone"", Anne",Jet,"9th, at Terrace plc",Desert City,CO,00123\n', "utf-8"))

    # start server
    port = find_available_port()
    httpd = HTTPServer(("localhost", port), TestServer)
    thread = Thread(target=httpd.serve_forever, args=())
    thread.start()
    try:
        response = requests.get(f"http://localhost:{port}", stream=True)
        result = list(CompositeRawDecoder(parser=CsvParser()).decode(response))

        assert len(result) == 1
    finally:
        httpd.shutdown()  # release port and kill the thread
        thread.join(timeout=5)  # ensure thread is cleaned up


def test_given_response_already_consumed_when_decode_then_no_data_is_returned(requests_mock):
    requests_mock.register_uri(
        "GET", "https://airbyte.io/", content=json.dumps({"test": "test"}).encode()
    )
    response = requests.get("https://airbyte.io/", stream=True)
    composite_raw_decoder = CompositeRawDecoder(parser=JsonParser(encoding="utf-8"))

    content = list(composite_raw_decoder.decode(response))
    assert content

    with pytest.raises(Exception):
        list(composite_raw_decoder.decode(response))


def test_given_response_is_not_streamed_when_decode_then_can_be_called_multiple_times(
    requests_mock,
):
    requests_mock.register_uri(
        "GET", "https://airbyte.io/", content=json.dumps({"test": "test"}).encode()
    )
    response = requests.get("https://airbyte.io/")
    composite_raw_decoder = CompositeRawDecoder(
        parser=JsonParser(encoding="utf-8"),
        stream_response=False,
    )

    content = list(composite_raw_decoder.decode(response))
    content_second_time = list(composite_raw_decoder.decode(response))

    assert content == content_second_time
