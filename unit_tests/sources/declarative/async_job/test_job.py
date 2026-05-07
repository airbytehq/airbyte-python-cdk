# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

from airbyte_cdk.sources.declarative.async_job.job import AsyncJob
from airbyte_cdk.sources.declarative.async_job.status import AsyncJobStatus
from airbyte_cdk.sources.declarative.types import StreamSlice

_AN_API_JOB_ID = "an api job id"
_ANY_STREAM_SLICE = StreamSlice(partition={}, cursor_slice={})
_A_VERY_BIG_TIMEOUT = timedelta(days=999999999)
_IMMEDIATELY_TIMED_OUT = timedelta(microseconds=1)


def test_given_timer_is_not_out_when_status_then_return_actual_status() -> None:
    job = AsyncJob(_AN_API_JOB_ID, _ANY_STREAM_SLICE, _A_VERY_BIG_TIMEOUT)
    assert job.status() == AsyncJobStatus.RUNNING


def test_given_timer_is_out_when_status_then_return_timed_out() -> None:
    job = AsyncJob(_AN_API_JOB_ID, _ANY_STREAM_SLICE, _IMMEDIATELY_TIMED_OUT)
    time.sleep(0.001)
    assert job.status() == AsyncJobStatus.TIMED_OUT


@pytest.mark.parametrize(
    "retry_after,expected_deferred,expected_ready",
    [
        pytest.param(None, False, True, id="no_retry_after_set"),
        pytest.param(
            datetime.now(tz=timezone.utc) + timedelta(hours=1),
            True,
            False,
            id="retry_after_in_future",
        ),
        pytest.param(
            datetime.now(tz=timezone.utc) - timedelta(seconds=1),
            True,
            True,
            id="retry_after_in_past",
        ),
    ],
)
def test_retry_after(
    retry_after: Optional[datetime], expected_deferred: bool, expected_ready: bool
) -> None:
    job = AsyncJob(_AN_API_JOB_ID, _ANY_STREAM_SLICE, _A_VERY_BIG_TIMEOUT)
    if retry_after is not None:
        job.set_retry_after(retry_after)
    assert job.retry_deferred() == expected_deferred
    assert job.ready_to_retry() == expected_ready
