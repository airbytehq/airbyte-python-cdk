#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json

import pytest
import requests
from freezegun import freeze_time

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.wait_time_from_response_backoff_strategy import (
    WaitTimeFromResponseBackoffStrategy,
)
from airbyte_cdk.utils import AirbyteTracedException

GMAIL_WAIT_TIME_EXPRESSION = r"{{ max(1, timestamp(response.error.message | regex_search('Retry after (\\S+Z)')) - now_utc().timestamp()) if (response.get('error', {}).get('message', '') | regex_search('Retry after (\\S+Z)')) else '' }}"


def test_wait_time_from_response_interpolates_json_body():
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.retry_in }}", parameters={}, config={}
    )

    assert strategy.backoff_time(_response({"retry_in": "12.5"}), 1) == 12.5


def test_wait_time_from_response_interpolates_headers():
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ headers['Retry-After'] }}", parameters={}, config={}
    )

    assert strategy.backoff_time(_response({}, headers={"Retry-After": "15"}), 1) == 15.0


@pytest.mark.parametrize("empty_value", ["", "   "])
def test_wait_time_from_response_returns_none_for_empty_result(empty_value):
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.get('retry_in', '') }}", parameters={}, config={}
    )

    assert strategy.backoff_time(_response({"retry_in": empty_value}), 1) is None


def test_wait_time_from_response_returns_none_for_missing_key():
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.get('retry_in', '') }}", parameters={}, config={}
    )

    assert strategy.backoff_time(_response({}), 1) is None


def test_wait_time_from_response_returns_none_for_non_json_body():
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.get('retry_in', '') }}", parameters={}, config={}
    )

    assert strategy.backoff_time(_response_body(b"not JSON"), 1) is None


@pytest.mark.parametrize(
    "response_or_exception",
    [None, requests.RequestException()],
    ids=["none", "exception"],
)
def test_wait_time_from_response_returns_none_for_non_response_input(response_or_exception):
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.get('retry_in', '') }}", parameters={}, config={}
    )

    assert strategy.backoff_time(response_or_exception, 1) is None


@pytest.mark.parametrize("wait_time", [10, 11], ids=["equal_to_cap", "above_cap"])
def test_wait_time_from_response_at_or_above_cap_raises_transient_error(wait_time):
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.retry_in }}",
        parameters={},
        config={},
        max_waiting_time_in_seconds=10,
    )

    with pytest.raises(AirbyteTracedException) as exception:
        strategy.backoff_time(_response({"retry_in": wait_time}), 1)

    assert exception.value.failure_type == FailureType.transient_error
    assert (
        exception.value.message
        == "The rate limit wait time is longer than the connector is allowed to wait."
    )
    assert "greater than or equal to the maximum of 10.0s" in exception.value.internal_message


def test_wait_time_from_response_below_cap_is_returned():
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.retry_in }}",
        parameters={},
        config={},
        max_waiting_time_in_seconds=10,
    )

    assert strategy.backoff_time(_response({"retry_in": 9}), 1) == 9.0


def test_wait_time_from_response_cap_is_interpolated_from_config():
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.retry_in }}",
        parameters={},
        config={"max_waiting_time": 20},
        max_waiting_time_in_seconds="{{ config['max_waiting_time'] }}",
    )

    assert strategy.backoff_time(_response({"retry_in": 10}), 1) == 10.0


def test_wait_time_from_response_invalid_cap_fails_at_construction():
    with pytest.raises(AirbyteTracedException) as exception:
        WaitTimeFromResponseBackoffStrategy(
            wait_time_in_seconds="10",
            parameters={},
            config={"max_waiting_time": "not a number"},
            max_waiting_time_in_seconds="{{ config['max_waiting_time'] }}",
        )

    assert exception.value.failure_type == FailureType.system_error


def test_wait_time_from_response_zero_wait_is_not_refused_by_zero_cap():
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.retry_in }}",
        parameters={},
        config={},
        max_waiting_time_in_seconds=0,
    )

    assert strategy.backoff_time(_response({"retry_in": 0}), 1) == 0


@freeze_time("2026-10-15T13:00:00Z")
@pytest.mark.parametrize(
    "message, expected_wait",
    [
        pytest.param(
            "User-rate limit exceeded.  Retry after 2026-10-15T13:15:00Z",
            900,
            id="future_timestamp",
        ),
        pytest.param(
            "User-rate limit exceeded.  Retry after 2026-10-15T12:59:59Z",
            1,
            id="past_timestamp",
        ),
        pytest.param("User-rate limit exceeded.", None, id="missing_timestamp"),
    ],
)
def test_wait_time_from_response_gmail_expression(message, expected_wait):
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds=GMAIL_WAIT_TIME_EXPRESSION, parameters={}, config={}
    )

    actual_wait = strategy.backoff_time(_response({"error": {"message": message}}), 1)

    if expected_wait is None:
        assert actual_wait is None
    else:
        assert actual_wait == pytest.approx(expected_wait)


def _response(json_body, headers=None):
    return _response_body(json.dumps(json_body).encode(), headers=headers)


def _response_body(body, headers=None):
    response = requests.Response()
    response.status_code = 429
    response.headers = headers or {}
    response._content = body
    response.encoding = "utf-8"
    return response
