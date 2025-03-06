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
        self._tracker = JobTracker(
            limit="{{config['max_concurrent_jobs']}}", config={"max_concurrent_jobs": _LIMIT}
        )

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


def test_given_limit_is_interpolated_string_when_init_then_limit_is_int():
    tracker = JobTracker(
        limit="{{config['max_concurrent_jobs']}}", config={"max_concurrent_jobs": _LIMIT}
    )
    assert tracker._limit == _LIMIT


def test_given_limit_is_less_than_1_when_init_then_raise_value_error():
    with pytest.raises(ValueError):
        JobTracker(limit="-1", config={})
