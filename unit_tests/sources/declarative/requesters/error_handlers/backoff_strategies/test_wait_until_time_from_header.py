#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from unittest.mock import MagicMock, patch

import pytest
import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.wait_until_time_from_header_backoff_strategy import (
    WaitUntilTimeFromHeaderBackoffStrategy,
)
from airbyte_cdk.utils import AirbyteTracedException

SOME_BACKOFF_TIME = 60
REGEX = "[-+]?\\d+"


@pytest.mark.parametrize(
    "test_name, header, wait_until, min_wait, regex, expected_backoff_time",
    [
        ("test_wait_until_time_from_header", "wait_until", 1600000060.0, None, None, 60),
        (
            "test_wait_until_time_from_header_parameters",
            "{{parameters['wait_until']}}",
            1600000060.0,
            None,
            None,
            60,
        ),
        (
            "test_wait_until_time_from_header_config",
            "{{config['wait_until']}}",
            1600000060.0,
            None,
            None,
            60,
        ),
        ("test_wait_until_negative_time", "wait_until", 1500000000.0, None, None, None),
        ("test_wait_until_time_less_than_min", "wait_until", 1600000060.0, 120, None, 120),
        ("test_wait_until_no_header", "absent_header", 1600000000.0, None, None, None),
        (
            "test_wait_until_time_from_header_not_numeric",
            "wait_until",
            "1600000000,1600000000",
            None,
            None,
            None,
        ),
        ("test_wait_until_time_from_header_is_numeric", "wait_until", "1600000060", None, None, 60),
        (
            "test_wait_until_time_from_header_with_regex",
            "wait_until",
            "1600000060,60",
            None,
            "[-+]?\d+",
            60,
        ),  # noqa
        (
            "test_wait_until_time_from_header_with_regex_from_parameters",
            "wait_until",
            "1600000060,60",
            None,
            "{{parameters['regex']}}",
            60,
        ),
        # noqa
        (
            "test_wait_until_time_from_header_with_regex_from_config",
            "wait_until",
            "1600000060,60",
            None,
            "{{config['regex']}}",
            60,
        ),  # noqa
        (
            "test_wait_until_time_from_header_with_regex_no_match",
            "wait_time",
            "...",
            None,
            "[-+]?\d+",
            None,
        ),  # noqa
        (
            "test_wait_until_no_header_with_min",
            "absent_header",
            "1600000000.0",
            SOME_BACKOFF_TIME,
            None,
            SOME_BACKOFF_TIME,
        ),
        (
            "test_wait_until_no_header_with_min_from_parameters",
            "absent_header",
            "1600000000.0",
            "{{parameters['min_wait']}}",
            None,
            SOME_BACKOFF_TIME,
        ),
        (
            "test_wait_until_no_header_with_min_from_config",
            "absent_header",
            "1600000000.0",
            "{{config['min_wait']}}",
            None,
            SOME_BACKOFF_TIME,
        ),
    ],
)
@patch("time.time", return_value=1600000000.0)
def test_wait_untiltime_from_header(
    time_mock, test_name, header, wait_until, min_wait, regex, expected_backoff_time
):
    response_mock = MagicMock(spec=requests.Response)
    response_mock.headers = {"wait_until": wait_until}
    backoff_strategy = WaitUntilTimeFromHeaderBackoffStrategy(
        header=header,
        min_wait=min_wait,
        regex=regex,
        parameters={"wait_until": "wait_until", "regex": REGEX, "min_wait": SOME_BACKOFF_TIME},
        config={"wait_until": "wait_until", "regex": REGEX, "min_wait": SOME_BACKOFF_TIME},
    )
    backoff = backoff_strategy.backoff_time(response_mock, 1)
    assert backoff == expected_backoff_time


NOW = 1600000000.0
IN_60_SECONDS = NOW + 60


def _response(wait_until=IN_60_SECONDS):
    response = MagicMock(spec=requests.Response)
    response.headers = {"wait_until": wait_until}
    return response


def _strategy(max_waiting_time_in_seconds, min_wait=None, config=None):
    return WaitUntilTimeFromHeaderBackoffStrategy(
        header="wait_until",
        min_wait=min_wait,
        parameters={},
        config=config if config is not None else {},
        max_waiting_time_in_seconds=max_waiting_time_in_seconds,
    )


@pytest.mark.parametrize(
    "max_waiting_time_in_seconds, expected",
    [
        pytest.param(None, 60, id="no_cap_waits"),
        pytest.param(3600, 60, id="cap_above_the_wait_waits"),
        pytest.param(60, 60, id="cap_equal_to_the_wait_waits"),
        pytest.param(30, "raises", id="cap_below_the_wait_raises"),
        pytest.param(0, "raises", id="zero_cap_never_waits"),
    ],
)
@patch("time.time", return_value=NOW)
def test_max_waiting_time_in_seconds(time_mock, max_waiting_time_in_seconds, expected):
    """The cap is what lets one operation refuse a wait that another operation would accept.

    `0` has to raise rather than switch the cap off: it is the value a caller uses to say "never
    wait", and the equivalent field on WaitTimeFromHeader read it as falsy and ignored it.
    """
    strategy = _strategy(max_waiting_time_in_seconds)

    if expected == "raises":
        with pytest.raises(AirbyteTracedException) as exc_info:
            strategy.backoff_time(_response(), 1)
        assert exc_info.value.failure_type == FailureType.transient_error
    else:
        assert strategy.backoff_time(_response(), 1) == expected


@patch("time.time", return_value=NOW)
def test_cap_is_applied_after_the_min_wait_floor(time_mock):
    """`min_wait` can round a short wait up past the cap. The cap wins -- a caller that says it
    will never wait longer than N seconds means it, floor or no floor."""
    strategy = _strategy(max_waiting_time_in_seconds=30, min_wait=60)

    with pytest.raises(AirbyteTracedException):
        strategy.backoff_time(_response(NOW + 1), 1)


@patch("time.time", return_value=NOW)
def test_cap_applies_to_the_min_wait_fallback_when_the_header_is_absent(time_mock):
    """With no usable header the strategy falls back to `min_wait`; that fallback is a wait like
    any other and has to respect the cap."""
    response = MagicMock(spec=requests.Response)
    response.headers = {}
    strategy = _strategy(max_waiting_time_in_seconds=30, min_wait=60)

    with pytest.raises(AirbyteTracedException):
        strategy.backoff_time(response, 1)


@pytest.mark.parametrize(
    "config, expected",
    [
        pytest.param({"max_waiting_time": 120}, 60, id="sync_budget_allows_the_wait"),
        pytest.param({"max_waiting_time": 0}, "raises", id="check_budget_refuses_the_wait"),
    ],
)
@patch("time.time", return_value=NOW)
def test_cap_is_interpolated_from_config(time_mock, config, expected):
    """The reason the field is interpolatable: one manifest, and an operation that has to answer
    quickly -- a connection check overriding `max_waiting_time` to 0 -- refuses a wait the same
    stream takes happily during a sync."""
    strategy = _strategy("{{ config['max_waiting_time'] * 60 }}", config=config)

    if expected == "raises":
        with pytest.raises(AirbyteTracedException):
            strategy.backoff_time(_response(), 1)
    else:
        assert strategy.backoff_time(_response(), 1) == expected


@pytest.mark.parametrize(
    "max_waiting_time_in_seconds, config",
    [
        pytest.param("{{ config['max_waiting_time'] * 60 }}", {}, id="config_value_is_missing"),
        pytest.param(
            "{{ config['max_waiting_time'] }}", {"max_waiting_time": "abc"}, id="not_a_number"
        ),
    ],
)
@patch("time.time", return_value=NOW)
def test_given_cap_cannot_be_evaluated_then_raise_system_error(
    time_mock, max_waiting_time_in_seconds, config
):
    """The cap is only read while handling an error that was already going to be retried, so an
    unresolvable interpolation must not surface as an unhandled jinja or float error. It is a
    system error because the field is declared in the manifest: the user has nothing to fix."""
    strategy = _strategy(max_waiting_time_in_seconds, config=config)

    with pytest.raises(AirbyteTracedException) as exc_info:
        strategy.backoff_time(_response(), 1)
    assert exc_info.value.failure_type == FailureType.system_error
    assert "max_waiting_time_in_seconds" in exc_info.value.internal_message
