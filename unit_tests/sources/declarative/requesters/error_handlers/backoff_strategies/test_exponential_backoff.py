#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies.exponential_backoff_strategy import (
    ExponentialBackoffStrategy,
)

parameters = {"backoff": 5}
config = {"backoff": 5}


@pytest.mark.parametrize(
    "test_name, attempt_count, factor, jitter_range, expected_backoff_time",
    [
        ("test_exponential_backoff_first_attempt", 1, 5, None, 10),
        ("test_exponential_backoff_second_attempt", 2, 5, None, 20),
        ("test_exponential_backoff_zero_jitter", 2, 5, 0, 20),
        ("test_exponential_backoff_from_parameters", 2, "{{parameters['backoff']}}", None, 20),
        ("test_exponential_backoff_from_config", 2, "{{config['backoff']}}", None, 20),
    ],
)
def test_exponential_backoff(test_name, attempt_count, factor, jitter_range, expected_backoff_time):
    response_mock = MagicMock()
    backoff_strategy = ExponentialBackoffStrategy(
        factor=factor,
        jitter_range_in_seconds=jitter_range,
        parameters=parameters,
        config={**config, "jitter": 0},
    )
    backoff = backoff_strategy.backoff_time(response_mock, attempt_count=attempt_count)
    assert backoff == expected_backoff_time


def test_exponential_backoff_default():
    response_mock = MagicMock()
    backoff_strategy = ExponentialBackoffStrategy(parameters=parameters, config=config)
    backoff = backoff_strategy.backoff_time(response_mock, attempt_count=3)
    assert backoff == 40


@pytest.mark.parametrize(
    "attempt_count, factor, jitter_range, expected_lower_bound, expected_upper_bound",
    [
        pytest.param(2, 5, 5, 20, 30, id="base_backoff_floor"),
        pytest.param(1, 2, 10, 4, 24, id="large_jitter"),
    ],
)
def test_exponential_backoff_with_jitter_bounds(
    attempt_count, factor, jitter_range, expected_lower_bound, expected_upper_bound
):
    response_mock = MagicMock()
    backoff_strategy = ExponentialBackoffStrategy(
        factor=factor,
        jitter_range_in_seconds=jitter_range,
        parameters=parameters,
        config=config,
    )

    backoff_times = [
        backoff_strategy.backoff_time(response_mock, attempt_count=attempt_count)
        for _ in range(2000)
    ]

    assert all(expected_lower_bound <= backoff <= expected_upper_bound for backoff in backoff_times)
    assert len(set(backoff_times)) > 1
