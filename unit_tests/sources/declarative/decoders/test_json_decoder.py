#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import gzip
import json
import os

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders import CompositeRawDecoder
from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import JsonLineParser
from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder


@pytest.mark.parametrize(
    "response_body, first_element",
    [
        ("", {}),
        ("[]", {}),
        ('{"healthcheck": {"status": "ok"}}', {"healthcheck": {"status": "ok"}}),
    ],
)
def test_json_decoder(requests_mock, response_body, first_element):
    requests_mock.register_uri("GET", "https://airbyte.io/", text=response_body)
    response = requests.get("https://airbyte.io/")
    assert next(JsonDecoder(parameters={}).decode(response)) == first_element


@pytest.mark.parametrize(
    "response_body, expected_json",
    [
        ("", []),
        ('{"id": 1, "name": "test1"}', [{"id": 1, "name": "test1"}]),
        (
            '{"id": 1, "name": "test1"}\n{"id": 2, "name": "test2"}',
            [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}],
        ),
    ],
    ids=["empty_response", "one_line_json", "multi_line_json"],
)
def test_jsonl_decoder(requests_mock, response_body, expected_json):
    requests_mock.register_uri("GET", "https://airbyte.io/", text=response_body)
    response = requests.get("https://airbyte.io/", stream=True)
    assert (
        list(CompositeRawDecoder(parser=JsonLineParser(), stream_response=True).decode(response))
        == expected_json
    )


@pytest.fixture(name="large_events_response")
def large_event_response_fixture():
    data = {"email": "email1@example.com"}
    jsonl_string = f"{json.dumps(data)}\n"
    lines_in_response = 2_000_000  # ≈ 58 MB of response
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = f"{dir_path}/test_response.txt"
    with open(file_path, "w") as file:
        for _ in range(lines_in_response):
            file.write(jsonl_string)
    yield (lines_in_response, file_path)
    os.remove(file_path)
