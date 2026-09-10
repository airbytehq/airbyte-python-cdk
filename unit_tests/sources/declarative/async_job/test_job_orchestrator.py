# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import logging
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Mapping, Optional, Set, Tuple
from unittest import TestCase, mock
from unittest.mock import MagicMock, Mock, call

import pytest

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.async_job.job import AsyncJob, AsyncJobStatus
from airbyte_cdk.sources.declarative.async_job.job_orchestrator import (
    AsyncJobOrchestrator,
    AsyncPartition,
)
from airbyte_cdk.sources.declarative.async_job.job_tracker import JobTracker
from airbyte_cdk.sources.declarative.async_job.repository import AsyncJobRepository
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.types import StreamSlice
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_cdk.utils.traced_exception import RateLimitBudgetExhaustedException

_ANY_STREAM_SLICE = Mock()
_A_STREAM_SLICE = Mock()
_ANOTHER_STREAM_SLICE = Mock()
_ANY_RECORD = {"a record field": "a record value"}
_NO_JOB_LIMIT = sys.maxsize
_BUFFER = 10000  # this buffer allows us to be unconcerned with the number of times the update status is called


def _create_job(status: AsyncJobStatus = AsyncJobStatus.FAILED) -> AsyncJob:
    job = Mock(spec=AsyncJob)
    job.status.return_value = status
    return job


class AsyncPartitionTest(TestCase):
    def test_given_one_failed_job_when_status_then_return_failed(self) -> None:
        partition = AsyncPartition(
            [_create_job(status) for status in AsyncJobStatus], _ANY_STREAM_SLICE
        )
        assert partition.status == AsyncJobStatus.FAILED

    def test_given_all_status_except_failed_when_status_then_return_timed_out(self) -> None:
        statuses = [status for status in AsyncJobStatus if status != AsyncJobStatus.FAILED]
        partition = AsyncPartition([_create_job(status) for status in statuses], _ANY_STREAM_SLICE)
        assert partition.status == AsyncJobStatus.TIMED_OUT

    def test_given_running_and_completed_jobs_when_status_then_return_running(self) -> None:
        partition = AsyncPartition(
            [_create_job(AsyncJobStatus.RUNNING), _create_job(AsyncJobStatus.COMPLETED)],
            _ANY_STREAM_SLICE,
        )
        assert partition.status == AsyncJobStatus.RUNNING

    def test_given_only_completed_jobs_when_status_then_return_completed(self) -> None:
        partition = AsyncPartition(
            [_create_job(AsyncJobStatus.COMPLETED) for _ in range(10)], _ANY_STREAM_SLICE
        )
        assert partition.status == AsyncJobStatus.COMPLETED

    def test_given_only_skipped_jobs_when_status_then_return_skipped(self) -> None:
        partition = AsyncPartition(
            [_create_job(AsyncJobStatus.SKIPPED) for _ in range(3)], _ANY_STREAM_SLICE
        )
        assert partition.status == AsyncJobStatus.SKIPPED

    def test_given_completed_and_skipped_jobs_when_status_then_return_completed(self) -> None:
        partition = AsyncPartition(
            [_create_job(AsyncJobStatus.COMPLETED), _create_job(AsyncJobStatus.SKIPPED)],
            _ANY_STREAM_SLICE,
        )
        assert partition.status == AsyncJobStatus.COMPLETED

    def test_given_skipped_and_running_jobs_when_status_then_return_running(self) -> None:
        partition = AsyncPartition(
            [_create_job(AsyncJobStatus.SKIPPED), _create_job(AsyncJobStatus.RUNNING)],
            _ANY_STREAM_SLICE,
        )
        assert partition.status == AsyncJobStatus.RUNNING

    def test_given_skipped_and_failed_jobs_when_status_then_return_failed(self) -> None:
        partition = AsyncPartition(
            [_create_job(AsyncJobStatus.SKIPPED), _create_job(AsyncJobStatus.FAILED)],
            _ANY_STREAM_SLICE,
        )
        assert partition.status == AsyncJobStatus.FAILED


def _status_update_per_jobs(
    status_update_per_jobs: Mapping[AsyncJob, List[AsyncJobStatus]],
) -> Callable[[set[AsyncJob]], None]:
    status_index_by_job = {job: 0 for job in status_update_per_jobs.keys()}

    def _update_status(jobs: Set[AsyncJob]) -> None:
        for job in jobs:
            status_index = status_index_by_job[job]
            job.update_status(status_update_per_jobs[job][status_index])
            status_index_by_job[job] += 1

    return _update_status


sleep_mock_target = "airbyte_cdk.sources.declarative.async_job.job_orchestrator.time.sleep"
_MAX_NUMBER_OF_ATTEMPTS = 3


class AsyncJobOrchestratorTest(TestCase):
    def setUp(self) -> None:
        self._job_repository = Mock(spec=AsyncJobRepository)
        self._message_repository = Mock(spec=MessageRepository)
        self._logger = Mock(spec=logging.Logger)

        self._job_for_a_slice = self._an_async_job("an api job id", _A_STREAM_SLICE)
        self._job_for_another_slice = self._an_async_job(
            "another api job id", _ANOTHER_STREAM_SLICE
        )

    @mock.patch(sleep_mock_target)
    def test_when_create_and_get_completed_partitions_then_create_job_and_update_status_until_completed(
        self, mock_sleep: MagicMock
    ) -> None:
        self._job_repository.start.return_value = self._job_for_a_slice
        status_updates = [AsyncJobStatus.RUNNING, AsyncJobStatus.RUNNING, AsyncJobStatus.COMPLETED]
        self._job_repository.update_jobs_status.side_effect = _status_update_per_jobs(
            {self._job_for_a_slice: status_updates}
        )
        orchestrator = self._orchestrator([_A_STREAM_SLICE])

        partitions = list(orchestrator.create_and_get_completed_partitions())

        assert len(partitions) == 1
        assert partitions[0].status == AsyncJobStatus.COMPLETED
        assert self._job_for_a_slice.update_status.mock_calls == [
            call(status) for status in status_updates
        ]

    @mock.patch(sleep_mock_target)
    def test_given_one_job_still_running_when_create_and_get_completed_partitions_then_only_update_running_job_status(
        self, mock_sleep: MagicMock
    ) -> None:
        self._job_repository.start.side_effect = [
            self._job_for_a_slice,
            self._job_for_another_slice,
        ]
        self._job_repository.update_jobs_status.side_effect = _status_update_per_jobs(
            {
                self._job_for_a_slice: [AsyncJobStatus.COMPLETED],
                self._job_for_another_slice: [AsyncJobStatus.RUNNING, AsyncJobStatus.COMPLETED],
            }
        )
        orchestrator = self._orchestrator([_A_STREAM_SLICE, _ANOTHER_STREAM_SLICE])

        list(orchestrator.create_and_get_completed_partitions())

        assert self._job_repository.update_jobs_status.mock_calls == [
            call({self._job_for_a_slice, self._job_for_another_slice}),
            call({self._job_for_another_slice}),
        ]

    @mock.patch(sleep_mock_target)
    def test_given_timeout_when_create_and_get_completed_partitions_then_free_budget_and_raise_exception(
        self, mock_sleep: MagicMock
    ) -> None:
        job_tracker = JobTracker(1)
        self._job_repository.start.return_value = self._job_for_a_slice
        self._job_repository.update_jobs_status.side_effect = _status_update_per_jobs(
            {self._job_for_a_slice: [AsyncJobStatus.TIMED_OUT]}
        )
        orchestrator = self._orchestrator([_A_STREAM_SLICE], job_tracker)

        with pytest.raises(AirbyteTracedException):
            list(orchestrator.create_and_get_completed_partitions())

        assert job_tracker.try_to_get_intent()
        assert (
            self._job_repository.start.call_args_list
            == [call(_A_STREAM_SLICE)] * _MAX_NUMBER_OF_ATTEMPTS
        )

    @mock.patch(sleep_mock_target)
    def test_given_failure_when_create_and_get_completed_partitions_then_raise_exception(
        self, mock_sleep: MagicMock
    ) -> None:
        self._job_repository.start.return_value = self._job_for_a_slice
        self._job_repository.update_jobs_status.side_effect = _status_update_per_jobs(
            {self._job_for_a_slice: [AsyncJobStatus.FAILED]}
        )
        orchestrator = self._orchestrator([_A_STREAM_SLICE])

        with pytest.raises(AirbyteTracedException):
            list(orchestrator.create_and_get_completed_partitions())
        assert (
            self._job_repository.start.call_args_list
            == [call(_A_STREAM_SLICE)] * _MAX_NUMBER_OF_ATTEMPTS
        )

    def test_when_fetch_records_then_yield_records_from_each_job(self) -> None:
        self._job_repository.fetch_records.return_value = [_ANY_RECORD]
        orchestrator = self._orchestrator([_A_STREAM_SLICE])
        first_job = _create_job()
        second_job = _create_job()

        records = list(orchestrator.fetch_records([first_job, second_job]))

        assert len(records) == 2
        assert self._job_repository.fetch_records.mock_calls == [call(first_job), call(second_job)]
        assert self._job_repository.delete.mock_calls == [call(first_job), call(second_job)]

    def _orchestrator(
        self, slices: List[StreamSlice], job_tracker: Optional[JobTracker] = None
    ) -> AsyncJobOrchestrator:
        job_tracker = job_tracker if job_tracker else JobTracker(_NO_JOB_LIMIT)
        return AsyncJobOrchestrator(
            self._job_repository, slices, job_tracker, self._message_repository
        )

    def test_given_more_jobs_than_limit_when_create_and_get_completed_partitions_then_still_return_all_slices_and_free_job_budget(
        self,
    ) -> None:
        job_tracker = JobTracker(1)
        self._job_repository.start.side_effect = [
            self._job_for_a_slice,
            self._job_for_another_slice,
        ]
        self._job_repository.update_jobs_status.side_effect = _status_update_per_jobs(
            {
                self._job_for_a_slice: [AsyncJobStatus.COMPLETED],
                self._job_for_another_slice: [AsyncJobStatus.COMPLETED],
            }
        )
        orchestrator = self._orchestrator(
            [self._job_for_a_slice.job_parameters(), self._job_for_another_slice.job_parameters()],
            job_tracker,
        )

        partitions = list(orchestrator.create_and_get_completed_partitions())

        assert len(partitions) == 2
        assert job_tracker.try_to_get_intent()

    @mock.patch(sleep_mock_target)
    def test_given_exception_to_break_when_start_job_and_raise_this_exception_and_abort_jobs(
        self, mock_sleep: MagicMock
    ) -> None:
        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [_A_STREAM_SLICE, _ANOTHER_STREAM_SLICE],
            JobTracker(_NO_JOB_LIMIT),
            self._message_repository,
            exceptions_to_break_on=[ValueError],
        )
        self._job_repository.start.side_effect = [
            self._job_for_a_slice,
            ValueError("Something went wrong"),
        ]

        with pytest.raises(ValueError):
            # assert that orchestrator exits on expected error
            list(orchestrator.create_and_get_completed_partitions())
        assert len(orchestrator._job_tracker._jobs) == 0
        self._job_repository.abort.assert_called_once_with(self._job_for_a_slice)

    def test_given_traced_config_error_when_start_job_and_raise_this_exception_and_abort_jobs(
        self,
    ) -> None:
        """
        Since this is a config error, we assume the other jobs will fail for the same reasons.
        """
        job_tracker = JobTracker(1)
        self._job_repository.start.side_effect = AirbyteTracedException(
            "Can't create job", failure_type=FailureType.config_error
        )

        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [_A_STREAM_SLICE],
            job_tracker,
            self._message_repository,
            [ValueError],
        )

        with pytest.raises(AirbyteTracedException):
            list(orchestrator.create_and_get_completed_partitions())

        assert job_tracker.try_to_get_intent()

    def test_given_rate_limit_budget_exhausted_when_start_job_then_break_immediately(
        self,
    ) -> None:
        """Rate limit budget exhaustion during job creation must be treated as a
        breaking exception so the orchestrator does not retry and cascade into
        hundreds of API calls."""
        job_tracker = JobTracker(1)
        self._job_repository.start.side_effect = RateLimitBudgetExhaustedException(
            internal_message="Rate limit retry budget exhausted.",
            message="Rate limit retry budget exhausted.",
            failure_type=FailureType.transient_error,
        )

        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [_A_STREAM_SLICE],
            job_tracker,
            self._message_repository,
        )

        with pytest.raises(RateLimitBudgetExhaustedException):
            list(orchestrator.create_and_get_completed_partitions())

        # Only one attempt — no orchestrator retry
        assert self._job_repository.start.call_count == 1
        # Budget was freed
        assert job_tracker.try_to_get_intent()

    @mock.patch(sleep_mock_target)
    def test_given_rate_limit_budget_exhausted_with_running_jobs_then_abort_and_break(
        self, mock_sleep: MagicMock
    ) -> None:
        """When rate limit budget exhaustion occurs while other jobs are running,
        all running jobs should be aborted and the exception should propagate."""
        self._job_repository.start.side_effect = [
            self._job_for_a_slice,
            RateLimitBudgetExhaustedException(
                internal_message="Rate limit retry budget exhausted.",
                message="Rate limit retry budget exhausted.",
                failure_type=FailureType.transient_error,
            ),
        ]

        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [_A_STREAM_SLICE, _ANOTHER_STREAM_SLICE],
            JobTracker(_NO_JOB_LIMIT),
            self._message_repository,
        )

        with pytest.raises(RateLimitBudgetExhaustedException):
            list(orchestrator.create_and_get_completed_partitions())

        assert len(orchestrator._job_tracker._jobs) == 0
        self._job_repository.abort.assert_called_once_with(self._job_for_a_slice)

    @mock.patch(sleep_mock_target)
    def test_given_exception_on_single_job_when_create_and_get_completed_partitions_then_return(
        self, mock_sleep: MagicMock
    ) -> None:
        """
        We added this test because the initial logic of breaking the main loop we implemented (when `self._has_started_a_job and self._running_partitions`) was not enough in the case where there was only one slice and it would fail to start.
        """
        orchestrator = self._orchestrator([_A_STREAM_SLICE])
        self._job_repository.start.side_effect = ValueError

        with pytest.raises(AirbyteTracedException):
            # assert that orchestrator exits on expected error
            list(orchestrator.create_and_get_completed_partitions())

    @mock.patch(sleep_mock_target)
    def test_given_exception_when_start_job_and_skip_this_exception(
        self, mock_sleep: MagicMock
    ) -> None:
        self._job_repository.start.side_effect = [
            AirbyteTracedException("Something went wrong. Expected error #1"),
            self._job_for_another_slice,
            AirbyteTracedException("Something went wrong. Expected error #2"),
            AirbyteTracedException("Something went wrong. Expected error #3"),
        ]
        self._job_repository.update_jobs_status.side_effect = _status_update_per_jobs(
            {
                self._job_for_a_slice: [AsyncJobStatus.COMPLETED],
                self._job_for_another_slice: [AsyncJobStatus.RUNNING, AsyncJobStatus.COMPLETED],
            }
        )
        orchestrator = self._orchestrator([_A_STREAM_SLICE, _ANOTHER_STREAM_SLICE])

        partitions, exception = self._accumulate_create_and_get_completed_partitions(orchestrator)

        assert len(partitions) == 1  # only _job_for_another_slice has succeeded
        assert self._message_repository.emit_message.call_count == 3  # one for each traced message
        assert exception.failure_type == FailureType.system_error  # type: ignore  # exception should be of type AirbyteTracedException

    @mock.patch(sleep_mock_target)
    def test_given_jobs_failed_more_than_max_attempts_when_create_and_get_completed_partitions_then_free_job_budget(
        self, mock_sleep: MagicMock
    ) -> None:
        job_tracker = JobTracker(1)
        jobs = [self._an_async_job(str(i), _A_STREAM_SLICE) for i in range(_MAX_NUMBER_OF_ATTEMPTS)]
        self._job_repository.start.side_effect = jobs
        self._job_repository.update_jobs_status.side_effect = _status_update_per_jobs(
            {job: [AsyncJobStatus.FAILED] for job in jobs}
        )

        orchestrator = self._orchestrator([_A_STREAM_SLICE], job_tracker)

        with pytest.raises(AirbyteTracedException):
            list(orchestrator.create_and_get_completed_partitions())

        assert job_tracker.try_to_get_intent()

    def given_budget_already_taken_before_start_when_create_and_get_completed_partitions_then_wait_for_budget_to_be_freed(
        self,
    ) -> None:
        job_tracker = JobTracker(1)
        intent_to_free = job_tracker.try_to_get_intent()

        def wait_and_free_intent(_job_tracker: JobTracker, _intent_to_free: str) -> None:
            print("Waiting before freeing budget...")
            time.sleep(1)
            print("Waiting done, freeing budget!")
            _job_tracker.remove_job(_intent_to_free)

        self._job_repository.start.return_value = self._job_for_a_slice
        self._job_repository.update_jobs_status.side_effect = _status_update_per_jobs(
            {self._job_for_a_slice: [AsyncJobStatus.COMPLETED] * _BUFFER}
        )
        orchestrator = self._orchestrator([_A_STREAM_SLICE], job_tracker)

        threading.Thread(target=wait_and_free_intent, args=[job_tracker, intent_to_free]).start()
        partitions = list(orchestrator.create_and_get_completed_partitions())

        assert len(partitions) == 1

    def test_given_start_job_raise_when_create_and_get_completed_partitions_then_free_budget(
        self,
    ) -> None:
        job_tracker = JobTracker(1)
        self._job_repository.start.side_effect = ValueError("Can't create job")

        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [_A_STREAM_SLICE],
            job_tracker,
            self._message_repository,
            [ValueError],
        )

        with pytest.raises(Exception):
            list(orchestrator.create_and_get_completed_partitions())

        assert job_tracker.try_to_get_intent()

    @mock.patch(sleep_mock_target)
    def test_given_skipped_when_create_and_get_completed_partitions_then_skip_without_fetching_records(
        self, mock_sleep: MagicMock
    ) -> None:
        job_tracker = JobTracker(1)
        self._job_repository.start.return_value = self._job_for_a_slice
        self._job_repository.update_jobs_status.side_effect = _status_update_per_jobs(
            {self._job_for_a_slice: [AsyncJobStatus.SKIPPED]}
        )
        orchestrator = self._orchestrator([_A_STREAM_SLICE], job_tracker)

        partitions = list(orchestrator.create_and_get_completed_partitions())

        assert len(partitions) == 0  # skipped partitions are not yielded
        assert job_tracker.try_to_get_intent()  # budget was freed
        self._job_repository.fetch_records.assert_not_called()

    @mock.patch(sleep_mock_target)
    def test_given_skipped_does_not_retry(self, mock_sleep: MagicMock) -> None:
        self._job_repository.start.return_value = self._job_for_a_slice
        self._job_repository.update_jobs_status.side_effect = _status_update_per_jobs(
            {self._job_for_a_slice: [AsyncJobStatus.SKIPPED]}
        )
        orchestrator = self._orchestrator([_A_STREAM_SLICE])

        partitions = list(orchestrator.create_and_get_completed_partitions())

        assert len(partitions) == 0
        # start is called only once — SKIPPED does not trigger a retry
        assert self._job_repository.start.call_count == 1

    @mock.patch(sleep_mock_target)
    def test_given_failed_retry_wait_time_when_job_fails_then_defers_retry(
        self, mock_sleep: MagicMock
    ) -> None:
        """When failed_retry_wait_time_in_seconds is set and a job fails, the retry should
        be deferred: first call sets retry_after timestamp and skips, subsequent calls skip
        until cooldown elapses, then the job is replaced."""
        job_tracker = JobTracker(_NO_JOB_LIMIT)
        job = self._an_async_job("deferred-job", _A_STREAM_SLICE)
        job_tracker._jobs.add("deferred-job")
        partition = AsyncPartition([job], _A_STREAM_SLICE)
        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [],
            job_tracker,
            self._message_repository,
            failed_retry_wait_time_in_seconds=1800,
        )

        job.update_status(AsyncJobStatus.FAILED)

        # First call: should set retry_after and NOT replace
        orchestrator._replace_failed_jobs(partition)
        assert job.retry_deferred()
        assert not job.ready_to_retry()
        self._job_repository.start.assert_not_called()

        # Second call while cooldown hasn't elapsed: should still skip
        orchestrator._replace_failed_jobs(partition)
        self._job_repository.start.assert_not_called()

        # Simulate cooldown elapsed by setting retry_after to the past
        job.set_retry_after(datetime.now(tz=timezone.utc) - timedelta(seconds=1))
        replacement_job = self._an_async_job("replacement-job", _A_STREAM_SLICE)
        self._job_repository.start.return_value = replacement_job

        # Third call after cooldown: should replace the job
        orchestrator._replace_failed_jobs(partition)
        self._job_repository.start.assert_called_once()

    @mock.patch(sleep_mock_target)
    def test_given_no_failed_retry_wait_time_when_job_fails_then_replaces_immediately(
        self, mock_sleep: MagicMock
    ) -> None:
        """Without failed_retry_wait_time_in_seconds, FAILED jobs are replaced immediately
        (existing behavior)."""
        job_tracker = JobTracker(_NO_JOB_LIMIT)
        job = self._an_async_job("immediate-job", _A_STREAM_SLICE)
        job_tracker._jobs.add("immediate-job")
        partition = AsyncPartition([job], _A_STREAM_SLICE)
        replacement_job = self._an_async_job("replacement-job", _A_STREAM_SLICE)
        self._job_repository.start.return_value = replacement_job
        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [],
            job_tracker,
            self._message_repository,
        )

        job.update_status(AsyncJobStatus.FAILED)
        orchestrator._replace_failed_jobs(partition)

        self._job_repository.start.assert_called_once()

    @mock.patch(sleep_mock_target)
    def test_given_failed_retry_wait_time_when_timed_out_job_then_replaces_immediately(
        self, mock_sleep: MagicMock
    ) -> None:
        """TIMED_OUT jobs should be replaced immediately even when
        failed_retry_wait_time_in_seconds is set (only FAILED gets deferred)."""
        job_tracker = JobTracker(_NO_JOB_LIMIT)
        job = self._an_async_job("timed-out-job", _A_STREAM_SLICE)
        job_tracker._jobs.add("timed-out-job")
        partition = AsyncPartition([job], _A_STREAM_SLICE)
        replacement_job = self._an_async_job("replacement-job", _A_STREAM_SLICE)
        self._job_repository.start.return_value = replacement_job
        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [],
            job_tracker,
            self._message_repository,
            failed_retry_wait_time_in_seconds=1800,
        )

        job.update_status(AsyncJobStatus.TIMED_OUT)
        orchestrator._replace_failed_jobs(partition)

        self._job_repository.start.assert_called_once()

    @mock.patch(sleep_mock_target)
    def test_given_creation_failure_job_when_cooldown_configured_then_replaces_immediately(
        self, mock_sleep: MagicMock
    ) -> None:
        """Creation-failure jobs (created when API rejects with 429) should be retried
        immediately even when failed_retry_wait_time_in_seconds is set. Only real FAILED
        jobs (report created but got FATAL) should be deferred."""
        job_tracker = JobTracker(_NO_JOB_LIMIT)
        creation_failure_job = AsyncJob(
            "creation-failure-job", _A_STREAM_SLICE, is_creation_failure=True
        )
        creation_failure_job.update_status(AsyncJobStatus.FAILED)
        job_tracker._jobs.add("creation-failure-job")
        partition = AsyncPartition([creation_failure_job], _A_STREAM_SLICE)
        replacement_job = self._an_async_job("replacement-job", _A_STREAM_SLICE)
        self._job_repository.start.return_value = replacement_job
        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [],
            job_tracker,
            self._message_repository,
            failed_retry_wait_time_in_seconds=1800,
        )

        orchestrator._replace_failed_jobs(partition)
        self._job_repository.start.assert_called_once()
        assert not creation_failure_job.retry_deferred()

    @mock.patch(sleep_mock_target)
    def test_given_real_failed_then_cooldown_elapses_then_start_returns_creation_failure_then_no_rearm(
        self, mock_sleep: MagicMock
    ) -> None:
        """Regression test for the original bug: real FAILED -> arm cooldown -> cooldown
        elapses -> _start_job returns a creation-failure job (API still rejects) -> the
        replacement must NOT re-arm cooldown; it should be replaced immediately on the
        next tick."""
        job_tracker = JobTracker(_NO_JOB_LIMIT)
        real_job = self._an_async_job("real-job", _A_STREAM_SLICE)
        job_tracker._jobs.add("real-job")
        partition = AsyncPartition([real_job], _A_STREAM_SLICE)
        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [],
            job_tracker,
            self._message_repository,
            failed_retry_wait_time_in_seconds=1800,
        )

        real_job.update_status(AsyncJobStatus.FAILED)

        # Tick 1: arms cooldown on the real FAILED job
        orchestrator._replace_failed_jobs(partition)
        assert real_job.retry_deferred()
        self._job_repository.start.assert_not_called()

        # Simulate cooldown elapsed
        real_job.set_retry_after(datetime.now(tz=timezone.utc) - timedelta(seconds=1))

        # _start_job returns a creation-failure job (API still rejects with 429)
        creation_failure_replacement = AsyncJob(
            "creation-failure-replacement", _A_STREAM_SLICE, is_creation_failure=True
        )
        creation_failure_replacement.update_status(AsyncJobStatus.FAILED)
        self._job_repository.start.return_value = creation_failure_replacement
        job_tracker._jobs.add("creation-failure-replacement")

        # Tick 2: cooldown elapsed, replaces with creation-failure job
        orchestrator._replace_failed_jobs(partition)
        self._job_repository.start.assert_called_once()

        # The replacement is a creation-failure -> should NOT have cooldown armed
        replaced_job = list(partition.jobs)[0]
        assert replaced_job.is_creation_failure()
        assert not replaced_job.retry_deferred()

        # Tick 3: creation-failure job should be replaced immediately (no deferral)
        second_replacement = self._an_async_job("second-replacement", _A_STREAM_SLICE)
        self._job_repository.start.return_value = second_replacement
        job_tracker._jobs.add("second-replacement")
        orchestrator._replace_failed_jobs(partition)
        assert self._job_repository.start.call_count == 2

    def test_create_failed_job_tags_as_creation_failure(self) -> None:
        """Verify _create_failed_job produces a job marked as is_creation_failure."""
        orchestrator = AsyncJobOrchestrator(
            self._job_repository,
            [],
            JobTracker(_NO_JOB_LIMIT),
            self._message_repository,
        )
        job = orchestrator._create_failed_job(_A_STREAM_SLICE)
        assert job.is_creation_failure()
        assert job.status() == AsyncJobStatus.FAILED

    def _mock_repository(self) -> None:
        self._job_repository = Mock(spec=AsyncJobRepository)

    def _an_async_job(self, job_id: str, stream_slice: StreamSlice) -> AsyncJob:
        return mock.Mock(wraps=AsyncJob(job_id, stream_slice))

    def _accumulate_create_and_get_completed_partitions(
        self, orchestrator: AsyncJobOrchestrator
    ) -> Tuple[List[AsyncPartition], Optional[Exception]]:
        result = []
        try:
            for i in orchestrator.create_and_get_completed_partitions():
                result.append(i)
        except Exception as exception:
            return result, exception

        return result, None
