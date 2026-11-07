#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import json
import logging
from unittest.mock import patch

import pytest
import requests
from freezegun import freeze_time

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.wait_time_from_response_backoff_strategy import (
    WaitTimeFromResponseBackoffStrategy,
)
from airbyte_cdk.sources.declarative.requesters.error_handlers.default_error_handler import (
    DefaultErrorHandler,
)
from airbyte_cdk.sources.streams.http.http_client import HttpClient
from airbyte_cdk.utils import AirbyteTracedException

GMAIL_WAIT_TIME_EXPRESSION = r"{{ max(1, timestamp(response.error.message | regex_search('Retry after (\\S+Z)')) - now_utc().timestamp()) if (response is mapping and response.get('error') is mapping and ((response.error.message or '') | string | regex_search('Retry after (\\S+Z)'))) else '' }}"


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


@pytest.mark.parametrize(
    "retry_in",
    [
        pytest.param(-0.5, id="negative_fraction"),
        pytest.param(-30, id="negative"),
        pytest.param("nan", id="not_a_number"),
        pytest.param("inf", id="infinite"),
    ],
)
def test_wait_time_from_response_returns_none_for_negative_or_non_finite_result(retry_in):
    """A negative wait would reach `time.sleep` and fail the stream; the next strategy decides."""
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.retry_in }}", parameters={}, config={}
    )

    assert strategy.backoff_time(_response({"retry_in": retry_in}), 1) is None


def test_wait_time_from_response_returns_none_for_non_numeric_result(caplog):
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.retry_in }}", parameters={}, config={}
    )

    assert strategy.backoff_time(_response({"retry_in": "soon"}), 1) is None
    assert any("non-numeric str" in message for message in caplog.messages)
    assert all("soon" not in message for message in caplog.messages)


@freeze_time("2026-10-15T13:00:00Z")
def test_wait_time_from_response_past_timestamp_without_a_floor_defers():
    """`retry_at - now` is negative once the time has passed, without the `max(1, ...)` floor."""
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ timestamp(response.retry_at) - now_utc().timestamp() }}",
        parameters={},
        config={},
    )

    assert strategy.backoff_time(_response({"retry_at": "2026-10-15T12:59:30Z"}), 1) is None
    assert strategy.backoff_time(_response({"retry_at": "2026-10-15T13:00:30Z"}), 1) == 30


@pytest.mark.parametrize(
    "body",
    [
        pytest.param([{"error": {"message": "Retry after 2026-10-15T13:15:00Z"}}], id="list_body"),
        pytest.param({"error": "rate limited"}, id="error_is_a_string"),
        pytest.param({"error": {}}, id="error_without_message"),
        pytest.param({"error": {"message": None}}, id="message_is_null"),
        pytest.param(True, id="scalar_body"),
    ],
)
def test_wait_time_from_response_gmail_expression_defers_on_other_body_shapes(body):
    """The documented example runs on every retryable response, not only Gmail's rate limit."""
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds=GMAIL_WAIT_TIME_EXPRESSION, parameters={}, config={}
    )

    assert strategy.backoff_time(_response(body), 1) is None


def test_wait_time_from_response_returns_none_when_the_expression_fails_on_the_body(caplog):
    """An expression that does not fit the body defers to the next strategy instead of failing.

    `.get` on a list raises `UndefinedError` from Jinja, which `InterpolatedString.eval` re-raises
    when it evaluates the default (the expression itself) a second time.
    """
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.get('retry') }}", parameters={}, config={}
    )

    assert strategy.backoff_time(_response([{"retry": "soon"}]), 1) is None
    assert any("could not be evaluated" in message for message in caplog.messages)
    assert all("soon" not in message for message in caplog.messages)


def test_wait_time_from_response_a_failed_render_does_not_disable_the_strategy(caplog):
    """A `TypeError` while rendering one body must not turn the strategy off for later bodies."""
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response.reset_in - 1 }}", parameters={}, config={}
    )

    assert strategy.backoff_time(_response({"reset_in": "abc"}), 1) is None
    assert any("could not be evaluated" in message for message in caplog.messages)
    assert strategy.backoff_time(_response({"reset_in": 30}), 1) == 29


@pytest.mark.parametrize(
    "expression, body",
    [
        pytest.param("{{ 10 / response.n }}", b'{"n": 0}', id="division_by_zero"),
        pytest.param("{{ response.n }}", b'{"n": 1' + b"0" * 400 + b"}", id="float_overflow"),
    ],
)
def test_wait_time_from_response_returns_none_for_arithmetic_errors(expression, body):
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds=expression, parameters={}, config={}
    )

    assert strategy.backoff_time(_response_body(body), 1) is None


def test_wait_time_from_response_unreadable_body_falls_back_to_headers():
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ headers['Retry-After'] }}", parameters={}, config={}
    )

    deeply_nested_body = b"[" * 100_000
    assert strategy.backoff_time(_response_body(deeply_nested_body, {"Retry-After": "5"}), 1) == 5


def test_wait_time_from_response_undeclared_variable_fails_at_construction():
    with pytest.raises(AirbyteTracedException) as error:
        WaitTimeFromResponseBackoffStrategy(
            wait_time_in_seconds="{{ resposne.retry_in }}", parameters={}, config={}
        )

    assert error.value.failure_type == FailureType.system_error
    assert "resposne" in error.value.internal_message


def test_wait_time_from_response_interpolates_parameters():
    strategy = WaitTimeFromResponseBackoffStrategy(
        wait_time_in_seconds="{{ response[parameters.wait_field] }}",
        parameters={"wait_field": "retry_in"},
        config={},
    )

    assert strategy.backoff_time(_response({"retry_in": 7}), 1) == 7


def test_wait_time_from_response_wait_reaches_the_sleep_in_http_client(requests_mock):
    requests_mock.get(
        "https://api.example.com/data",
        [{"status_code": 429, "json": {"retry_in": 7}}, {"status_code": 200, "json": {}}],
    )
    http_client = _http_client(
        WaitTimeFromResponseBackoffStrategy(
            wait_time_in_seconds="{{ response.retry_in }}", parameters={}, config={}
        )
    )

    sleeps = []
    with patch("time.sleep", side_effect=sleeps.append):
        _, response = http_client.send_request(
            http_method="GET", url="https://api.example.com/data", request_kwargs={}
        )

    assert response.status_code == 200
    # `user_defined_backoff_handler` adds one second to cover fractions of a second; the `backoff`
    # library's own wait is 0.
    assert max(sleeps) == 8


def test_wait_time_from_response_wait_above_cap_stops_the_request_in_http_client(requests_mock):
    requests_mock.get("https://api.example.com/data", status_code=429, json={"retry_in": 3600})
    http_client = _http_client(
        WaitTimeFromResponseBackoffStrategy(
            wait_time_in_seconds="{{ response.retry_in }}",
            parameters={},
            config={},
            max_waiting_time_in_seconds=60,
        )
    )

    with patch("time.sleep") as sleep, pytest.raises(AirbyteTracedException) as error:
        http_client.send_request(
            http_method="GET", url="https://api.example.com/data", request_kwargs={}
        )

    assert error.value.failure_type == FailureType.transient_error
    sleep.assert_not_called()


def _http_client(backoff_strategy):
    return HttpClient(
        name="test",
        logger=logging.getLogger("test"),
        error_handler=DefaultErrorHandler(config={}, parameters={}),
        backoff_strategy=[backoff_strategy],
    )


def _response(json_body, headers=None):
    return _response_body(json.dumps(json_body).encode(), headers=headers)


def _response_body(body, headers=None):
    response = requests.Response()
    response.status_code = 429
    response.headers = headers or {}
    response._content = body
    response.encoding = "utf-8"
    return response
