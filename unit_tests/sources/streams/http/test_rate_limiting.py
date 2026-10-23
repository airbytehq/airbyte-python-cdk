# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import logging
from types import SimpleNamespace

import pytest
import requests

from airbyte_cdk.sources.streams.http.exceptions import UserDefinedBackoffException
from airbyte_cdk.sources.streams.http.rate_limiting import user_defined_backoff_handler


def _make_request_and_response(
    status_code: int,
) -> tuple[requests.PreparedRequest, requests.Response]:
    request = requests.Request("GET", "https://example.com/api").prepare()
    response = requests.Response()
    response.status_code = status_code
    return request, response


@pytest.mark.parametrize(
    ("status_code", "backoff", "request_error", "expected_log", "expect_rate_limit"),
    [
        (
            500,
            2910.8324723243713,
            False,
            "Backing off for 2911.8 seconds after HTTP 500 response (attempt 1), then retrying.",
            False,
        ),
        (
            429,
            59,
            False,
            "Rate limit exceeded (HTTP 429). Backing off for 60.0 seconds (attempt 1), then retrying.",
            True,
        ),
        (
            500,
            4,
            True,
            "Backing off for 5.0 seconds after a request error (attempt 1), then retrying.",
            False,
        ),
    ],
)
def test_user_defined_backoff_handler_logs_retry_reason(
    mocker, caplog, status_code, backoff, request_error, expected_log, expect_rate_limit
):
    request, response = _make_request_and_response(status_code)
    response_or_error = requests.exceptions.ReadTimeout("timed out") if request_error else response
    sentinel = object()
    calls = 0

    def send_request():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise UserDefinedBackoffException(
                backoff=backoff, request=request, response=response_or_error
            )
        return sentinel

    sleep = mocker.patch("airbyte_cdk.sources.streams.http.rate_limiting.time.sleep")
    mocker.patch("backoff._sync.time", SimpleNamespace(sleep=lambda _: None))
    wrapped = user_defined_backoff_handler(max_tries=2)(send_request)

    with caplog.at_level(logging.INFO, logger="airbyte"):
        result = wrapped()

    messages = [record.getMessage() for record in caplog.records]
    assert messages == [expected_log]
    assert ("Rate limit" in messages[0]) is expect_rate_limit
    assert sleep.call_count == 1
    sleep.assert_called_once_with(backoff + 1)
    assert result is sentinel
