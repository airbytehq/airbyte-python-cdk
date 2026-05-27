#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.constant_backoff_strategy import (
    ConstantBackoffStrategy,
)

BACKOFF_TIME = 10
PARAMETERS_BACKOFF_TIME = 20
CONFIG_BACKOFF_TIME = 30


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
        (
            "test_constant_jitter_from_config",
            1,
            BACKOFF_TIME,
            "{{ config['jitter'] }}",
            BACKOFF_TIME,
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
        pytest.param(60, 15, 45, 75, id="centered_jitter"),
        pytest.param(10, 30, 0, 40, id="lower_bound_clamped_to_zero"),
        pytest.param(0, 5, 0, 5, id="zero_base"),
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


def test_constant_backoff_with_negative_jitter_raises_error():
    response_mock = MagicMock()
    backoff_strategy = ConstantBackoffStrategy(
        parameters={},
        backoff_time_in_seconds=BACKOFF_TIME,
        jitter_range_in_seconds=-5,
        config={},
    )

    with pytest.raises(ValueError, match="jitter_range_in_seconds"):
        backoff_strategy.backoff_time(response_mock, attempt_count=1)
