#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import gzip
import json
import logging
import os

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders import CompositeRawDecoder
from airbyte_cdk.sources.declarative.decoders import json_decoder as json_decoder_module
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
    ("response_body", "content_type"),
    [
        ("", "application/json"),
        ("<html>error</html>", "text/html"),
    ],
    ids=["empty_body", "html_body"],
)
def test_json_decoder_logs_response_details_for_invalid_json(
    requests_mock, caplog, response_body, content_type
):
    url = "https://airbyte.io/orders?api_key=secret"
    requests_mock.register_uri(
        "GET",
        url,
        text=response_body,
        status_code=200,
        headers={"Content-Type": content_type},
    )
    response = requests.get(url)

    with caplog.at_level(logging.ERROR, logger="airbyte"):
        assert all(element == {} for element in JsonDecoder(parameters={}).decode(response))

    messages = [record.message for record in caplog.records]
    message = next(message for message in messages if "Failed to decode JSON response" in message)
    assert "method=GET" in message
    assert f"url={url}" in message
    assert "status_code=200" in message
    assert f"content_type={content_type}" in message
    assert f"body_length={len(response_body.encode())}" in message
    assert f"body_preview={response_body!r}" in message
    assert "Response JSON data failed to be parsed" in message


def test_json_decoder_does_not_log_for_empty_json_array(requests_mock, caplog):
    requests_mock.register_uri(
        "GET",
        "https://airbyte.io/",
        text="[]",
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    response = requests.get("https://airbyte.io/")

    with caplog.at_level(logging.ERROR, logger="airbyte"):
        assert list(JsonDecoder(parameters={}).decode(response)) == [{}]

    assert not caplog.records


def test_json_decoder_filters_secrets_before_logging(requests_mock, caplog, monkeypatch):
    url = "https://airbyte.io/orders?api_key=secret"
    requests_mock.register_uri("GET", url, text="<secret>error</secret>", status_code=200)
    response = requests.get(url)
    monkeypatch.setattr(
        json_decoder_module,
        "filter_secrets",
        lambda message: message.replace("secret", "****"),
    )

    with caplog.at_level(logging.ERROR, logger="airbyte"):
        list(JsonDecoder(parameters={}).decode(response))

    message = next(
        record.message
        for record in caplog.records
        if "Failed to decode JSON response" in record.message
    )
    assert "secret" not in message
    assert "****" in message


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


@pytest.mark.slow
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
