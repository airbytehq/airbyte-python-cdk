#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

import pytest
from requests import Response

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.wait_time_from_header_backoff_strategy import (
    WaitTimeFromHeaderBackoffStrategy,
)
from airbyte_cdk.utils import AirbyteTracedException

SOME_BACKOFF_TIME = 60
_A_RETRY_HEADER = "retry-header"
_A_MAX_TIME = 100


@pytest.mark.parametrize(
    "test_name, header, header_value, regex, expected_backoff_time",
    [
        ("test_wait_time_from_header", "wait_time", SOME_BACKOFF_TIME, None, SOME_BACKOFF_TIME),
        ("test_wait_time_from_header_string", "wait_time", "60", None, SOME_BACKOFF_TIME),
        (
            "test_wait_time_from_header_parameters",
            "{{ parameters['wait_time'] }}",
            "60",
            None,
            SOME_BACKOFF_TIME,
        ),
        (
            "test_wait_time_from_header_config",
            "{{ config['wait_time'] }}",
            "60",
            None,
            SOME_BACKOFF_TIME,
        ),
        ("test_wait_time_from_header_not_a_number", "wait_time", "61,60", None, None),
        ("test_wait_time_from_header_with_regex", "wait_time", "61,60", r"([-+]?\d+)", 61),  # noqa
        ("test_wait_time_fœrom_header_with_regex_no_match", "wait_time", "...", "[-+]?\d+", None),  # noqa
        ("test_wait_time_from_header", "absent_header", None, None, None),
    ],
)
def test_wait_time_from_header(test_name, header, header_value, regex, expected_backoff_time):
    response_mock = MagicMock(spec=Response)
    response_mock.headers = {"wait_time": header_value}
    backoff_strategy = WaitTimeFromHeaderBackoffStrategy(
        header=header,
        regex=regex,
        parameters={"wait_time": "wait_time"},
        config={"wait_time": "wait_time"},
    )
    backoff = backoff_strategy.backoff_time(response_mock, 1)
    assert backoff == expected_backoff_time


def test_given_retry_after_smaller_than_max_time_then_raise_transient_error():
    response_mock = MagicMock(spec=Response)
    retry_after = _A_MAX_TIME - 1
    response_mock.headers = {_A_RETRY_HEADER: str(retry_after)}
    backoff_strategy = WaitTimeFromHeaderBackoffStrategy(
        header=_A_RETRY_HEADER, max_waiting_time_in_seconds=_A_MAX_TIME, parameters={}, config={}
    )

    assert backoff_strategy.backoff_time(response_mock, 1) == retry_after


def test_given_retry_after_greater_than_max_time_then_raise_transient_error():
    response_mock = MagicMock(spec=Response)
    response_mock.headers = {_A_RETRY_HEADER: str(_A_MAX_TIME + 1)}
    backoff_strategy = WaitTimeFromHeaderBackoffStrategy(
        header=_A_RETRY_HEADER, max_waiting_time_in_seconds=_A_MAX_TIME, parameters={}, config={}
    )

    with pytest.raises(AirbyteTracedException) as exception:
        backoff_strategy.backoff_time(response_mock, 1)
    assert exception.value.failure_type == FailureType.transient_error


def _response(header_value):
    response = MagicMock(spec=Response)
    response.headers = {_A_RETRY_HEADER: str(header_value)}
    return response


def _strategy(max_waiting_time_in_seconds, config=None):
    return WaitTimeFromHeaderBackoffStrategy(
        header=_A_RETRY_HEADER,
        max_waiting_time_in_seconds=max_waiting_time_in_seconds,
        parameters={},
        config=config if config is not None else {},
    )


def test_given_max_waiting_time_is_zero_then_never_wait():
    """`0` is the value a caller uses to say "never wait". It used to be read as falsy, which
    silently disabled the cap; it is now honoured, which is the one behaviour change of the PR
    that introduced interpolation on this field."""
    strategy = _strategy(max_waiting_time_in_seconds=0)

    with pytest.raises(AirbyteTracedException) as exc_info:
        strategy.backoff_time(_response(1), 1)
    assert exc_info.value.failure_type == FailureType.transient_error


@pytest.mark.parametrize(
    "config, expected",
    [
        pytest.param({"max_waiting_time": 10}, 120, id="cap_above_the_header_value_waits"),
        pytest.param({"max_waiting_time": 1}, "raises", id="cap_below_the_header_value_raises"),
        pytest.param({"max_waiting_time": 0}, "raises", id="zero_cap_never_waits"),
    ],
)
def test_max_waiting_time_is_interpolated_from_config(config, expected):
    strategy = _strategy("{{ config['max_waiting_time'] * 60 }}", config=config)

    if expected == "raises":
        with pytest.raises(AirbyteTracedException) as exc_info:
            strategy.backoff_time(_response(120), 1)
        assert exc_info.value.failure_type == FailureType.transient_error
    else:
        assert strategy.backoff_time(_response(120), 1) == expected


@pytest.mark.parametrize(
    "max_waiting_time_in_seconds, config",
    [
        pytest.param("{{ config['max_waiting_time'] * 60 }}", {}, id="config_value_is_missing"),
        pytest.param(
            "{{ config['max_waiting_time'] }}", {"max_waiting_time": "abc"}, id="not_a_number"
        ),
    ],
)
def test_given_max_waiting_time_cannot_be_evaluated_then_raise_system_error(
    max_waiting_time_in_seconds, config
):
    """The cap is only read while handling an error that was already going to be retried, so an
    unresolvable interpolation must not surface as an unhandled jinja or float error. It is a
    system error because the field is declared in the manifest: the user has nothing to fix."""
    strategy = _strategy(max_waiting_time_in_seconds, config=config)

    with pytest.raises(AirbyteTracedException) as exc_info:
        strategy.backoff_time(_response(120), 1)
    assert exc_info.value.failure_type == FailureType.system_error
    assert "max_waiting_time_in_seconds" in exc_info.value.internal_message
