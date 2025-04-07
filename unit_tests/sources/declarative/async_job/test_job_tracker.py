# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import List
from unittest import TestCase

import pytest

from airbyte_cdk.sources.declarative.async_job.job_tracker import (
    ConcurrentJobLimitReached,
    JobTracker,
)

_LIMIT = 3


class JobTrackerTest(TestCase):
    def setUp(self) -> None:
        self._tracker = JobTracker(_LIMIT)

    def test_given_limit_reached_when_remove_job_then_can_get_intent_again(self) -> None:
        intents = self._reach_limit()
        with pytest.raises(ConcurrentJobLimitReached):
            self._tracker.try_to_get_intent()

        self._tracker.remove_job(intents[0])
        assert self._tracker.try_to_get_intent()

    def test_given_job_does_not_exist_when_remove_job_then_do_not_raise(self) -> None:
        self._tracker.remove_job("non existing job id")

    def test_given_limit_reached_when_add_job_then_limit_is_still_reached(self) -> None:
        intents = [self._tracker.try_to_get_intent() for i in range(_LIMIT)]
        with pytest.raises(ConcurrentJobLimitReached):
            self._tracker.try_to_get_intent()

        self._tracker.add_job(intents[0], "a created job")
        with pytest.raises(ConcurrentJobLimitReached):
            self._tracker.try_to_get_intent()

    def _reach_limit(self) -> List[str]:
        return [self._tracker.try_to_get_intent() for i in range(_LIMIT)]


@pytest.mark.parametrize("limit", [-1, 0])
def test_given_limit_is_less_than_1_when_init_then_set_to_1(limit: int):
    tracker = JobTracker(limit)
    assert tracker._limit == 1


@pytest.mark.parametrize(
    ("limit", "config", "expected_limit"),
    [
        ("2", {}, 2),
        (
            "{{ config['max_concurrent_async_job_count'] }}",
            {"max_concurrent_async_job_count": 2},
            2,
        ),
    ],
)
def test_given_limit_as_string_when_init_then_interpolate_correctly(limit, config, expected_limit):
    tracker = JobTracker(limit, config)
    assert tracker._limit == expected_limit


def test_given_interpolated_limit_and_empty_config_when_init_then_set_to_1():
    tracker = JobTracker(
        "{{ config['max_concurrent_async_job_count'] }}",
        {"max_concurrent_async_job_count": "hello"},
    )
    assert tracker._limit == 1
