#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from unittest.mock import MagicMock

import pytest
import requests
from freezegun import freeze_time

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.constant_backoff_strategy import (
    ConstantBackoffStrategy,
)
from airbyte_cdk.utils import AirbyteTracedException

BACKOFF_TIME = 10
PARAMETERS_BACKOFF_TIME = 20
CONFIG_BACKOFF_TIME = 30
GMAIL_BACKOFF_EXPRESSION = r"{{ max(1, timestamp(response.error.message | regex_search('Retry after (\\S+Z)')) - now_utc().timestamp()) if (response.get('error', {}).get('message', '') | regex_search('Retry after (\\S+Z)')) else '' }}"


@pytest.mark.parametrize(
    "test_name, attempt_count, backofftime, jitter_range, expected_backoff_time",
    [
        ("test_constant_backoff_first_attempt", 1, BACKOFF_TIME, None, BACKOFF_TIME),
        ("test_constant_backoff_first_attempt_float", 1, 6.7, None, 6.7),
        ("test_constant_backoff_attempt_round_float", 1.0, 6.7, None, 6.7),
        ("test_constant_backoff_attempt_round_float", 1.5, 6.7, None, 6.7),
        ("test_constant_backoff_first_attempt_round_float", 1, 10.0, None, BACKOFF_TIME),
        ("test_constant_backoff_second_attempt_round_float", 2, 10.0, None, BACKOFF_TIME),
        ("test_constant_backoff_zero_jitter", 2, BACKOFF_TIME, 0, BACKOFF_TIME),
        (
            "test_constant_backoff_from_parameters",
            1,
            "{{ parameters['backoff'] }}",
            None,
            PARAMETERS_BACKOFF_TIME,
        ),
        (
            "test_constant_backoff_from_config",
            1,
            "{{ config['backoff'] }}",
            None,
            CONFIG_BACKOFF_TIME,
        ),
    ],
)
def test_constant_backoff(
    test_name, attempt_count, backofftime, jitter_range, expected_backoff_time
):
    response_mock = MagicMock()
    backoff_strategy = ConstantBackoffStrategy(
        parameters={"backoff": PARAMETERS_BACKOFF_TIME},
        backoff_time_in_seconds=backofftime,
        jitter_range_in_seconds=jitter_range,
        config={"backoff": CONFIG_BACKOFF_TIME, "jitter": 0},
    )
    backoff = backoff_strategy.backoff_time(response_mock, attempt_count=attempt_count)
    assert backoff == expected_backoff_time


@pytest.mark.parametrize(
    "backofftime, jitter_range, expected_lower_bound, expected_upper_bound",
    [
        pytest.param(60, 15, 60, 90, id="base_backoff_floor"),
        pytest.param(10, 30, 10, 70, id="large_jitter"),
        pytest.param(0, 5, 0, 10, id="zero_base"),
    ],
)
def test_constant_backoff_with_jitter_bounds(
    backofftime, jitter_range, expected_lower_bound, expected_upper_bound
):
    response_mock = MagicMock()
    backoff_strategy = ConstantBackoffStrategy(
        parameters={},
        backoff_time_in_seconds=backofftime,
        jitter_range_in_seconds=jitter_range,
        config={},
    )

    backoff_times = [
        backoff_strategy.backoff_time(response_mock, attempt_count=1) for _ in range(2000)
    ]

    assert all(expected_lower_bound <= backoff <= expected_upper_bound for backoff in backoff_times)
    assert len(set(backoff_times)) > 1


def test_constant_backoff_interpolates_response_json():
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds="{{ response.retry_in }}", parameters={}, config={}
    )

    assert backoff_strategy.backoff_time(_response({"retry_in": "12.5"}), 1) == 12.5


def test_constant_backoff_interpolates_headers():
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds="{{ headers['Retry-After'] }}", parameters={}, config={}
    )

    assert backoff_strategy.backoff_time(_response({}, headers={"Retry-After": "15"}), 1) == 15.0


@pytest.mark.parametrize("empty_value", ["", "   "])
def test_constant_backoff_returns_none_for_empty_interpolation(empty_value):
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds="{{ response.get('retry_in', '') }}", parameters={}, config={}
    )

    assert backoff_strategy.backoff_time(_response({"retry_in": empty_value}), 1) is None


def test_constant_backoff_returns_none_when_json_key_is_missing():
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds="{{ response.get('retry_in', '') }}", parameters={}, config={}
    )

    assert backoff_strategy.backoff_time(_response({}), 1) is None


def test_constant_backoff_returns_none_for_non_json_body():
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds="{{ response.get('retry_in', '') }}", parameters={}, config={}
    )

    assert backoff_strategy.backoff_time(_response_body(b"not JSON"), 1) is None


@pytest.mark.parametrize(
    "response_or_exception",
    [None, requests.RequestException()],
    ids=["none", "exception"],
)
def test_constant_backoff_returns_none_for_non_response_input(response_or_exception):
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds="{{ response.get('retry_in', '') }}", parameters={}, config={}
    )

    assert backoff_strategy.backoff_time(response_or_exception, 1) is None


def test_constant_backoff_does_not_apply_jitter_to_empty_interpolation(mocker):
    uniform = mocker.patch(
        "airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.constant_backoff_strategy.random.uniform"
    )
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds="{{ response.get('retry_in', '') }}",
        parameters={},
        config={},
        jitter_range_in_seconds=5,
    )

    assert backoff_strategy.backoff_time(_response({}), 1) is None
    uniform.assert_not_called()


@pytest.mark.parametrize("wait_time", [10, 11], ids=["equal_to_cap", "above_cap"])
def test_constant_backoff_wait_at_or_above_cap_raises_transient_error(wait_time):
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds="{{ response.retry_in }}",
        parameters={},
        config={},
        max_waiting_time_in_seconds=10,
    )

    with pytest.raises(AirbyteTracedException) as exception:
        backoff_strategy.backoff_time(_response({"retry_in": wait_time}), 1)

    assert exception.value.failure_type == FailureType.transient_error
    assert (
        exception.value.message
        == "The rate limit wait time is longer than the connector is allowed to wait."
    )
    assert "greater than or equal to the maximum of 10.0s" in exception.value.internal_message


def test_constant_backoff_wait_below_cap_is_returned():
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds=9,
        parameters={},
        config={},
        max_waiting_time_in_seconds=10,
    )

    assert backoff_strategy.backoff_time(None, 1) == 9.0


def test_constant_backoff_cap_is_interpolated_from_config():
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds="{{ response.retry_in }}",
        parameters={},
        config={"max_waiting_time": 20},
        max_waiting_time_in_seconds="{{ config['max_waiting_time'] }}",
    )

    assert backoff_strategy.backoff_time(_response({"retry_in": 10}), 1) == 10.0


def test_constant_backoff_invalid_cap_fails_at_construction():
    with pytest.raises(AirbyteTracedException) as exception:
        ConstantBackoffStrategy(
            backoff_time_in_seconds=10,
            parameters={},
            config={"max_waiting_time": "not a number"},
            max_waiting_time_in_seconds="{{ config['max_waiting_time'] }}",
        )

    assert exception.value.failure_type == FailureType.system_error


def test_constant_backoff_zero_wait_is_not_refused_by_zero_cap():
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds=0,
        parameters={},
        config={},
        max_waiting_time_in_seconds=0,
    )

    assert backoff_strategy.backoff_time(None, 1) == 0


def test_constant_backoff_cap_is_checked_after_jitter(mocker):
    mocker.patch(
        "airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.constant_backoff_strategy.random.uniform",
        return_value=11,
    )
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds=10,
        parameters={},
        config={},
        jitter_range_in_seconds=1,
        max_waiting_time_in_seconds=11,
    )

    with pytest.raises(AirbyteTracedException) as exception:
        backoff_strategy.backoff_time(None, 1)

    assert exception.value.failure_type == FailureType.transient_error


@freeze_time("2026-10-15T13:00:00Z")
@pytest.mark.parametrize(
    "message, expected_backoff",
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
def test_constant_backoff_gmail_retry_after_expression(message, expected_backoff):
    backoff_strategy = ConstantBackoffStrategy(
        backoff_time_in_seconds=GMAIL_BACKOFF_EXPRESSION, parameters={}, config={}
    )

    actual_backoff = backoff_strategy.backoff_time(_response({"error": {"message": message}}), 1)

    if expected_backoff is None:
        assert actual_backoff is None
    else:
        assert actual_backoff == pytest.approx(expected_backoff)


def _response(json_body, headers=None):
    return _response_body(json.dumps(json_body).encode(), headers=headers)


def _response_body(body, headers=None):
    response = requests.Response()
    response.status_code = 429
    response.headers = headers or {}
    response._content = body
    response.encoding = "utf-8"
    return response
