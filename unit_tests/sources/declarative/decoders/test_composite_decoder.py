#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import csv
import gzip
import json
from io import BytesIO, StringIO
from unittest.mock import patch

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


def compress_with_gzip(data: str, encoding: str = "utf-8"):
    """
    Compress the data using Gzip.
    """
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(data.encode(encoding))
    return buf.getvalue()


def generate_csv(
    encoding: str = "utf-8", delimiter: str = ",", should_compress: bool = False
) -> bytes:
    data = [
        {"id": "1", "name": "John", "age": "28"},
        {"id": "2", "name": "Alice", "age": "34"},
        {"id": "3", "name": "Bob", "age": "25"},
    ]

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=["id", "name", "age"], delimiter=delimiter)
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
    )
    response = requests.get("https://airbyte.io/", stream=True)

    # the delimiter is set to `\\t` intentionally to test the parsing logic here
    parser = GzipParser(inner_parser=CsvParser(encoding=encoding, delimiter="\\t"))

    composite_raw_decoder = CompositeRawDecoder(parser=parser)
    counter = 0
    for _ in composite_raw_decoder.decode(response):
        counter += 1
    assert counter == 3


def generate_jsonlines():
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
        "GET", "https://airbyte.io/", content=generate_compressed_jsonlines(encoding=encoding)
    )
    response = requests.get("https://airbyte.io/", stream=True)

    parser = GzipParser(inner_parser=JsonLineParser(encoding=encoding))
    composite_raw_decoder = CompositeRawDecoder(parser=parser)
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
        parser=JsonParser(encoding="utf-8"), stream_response=False
    )

    content = list(composite_raw_decoder.decode(response))
    content_second_time = list(composite_raw_decoder.decode(response))

    assert content == content_second_time
