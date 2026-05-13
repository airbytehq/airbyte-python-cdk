# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


from datetime import datetime, timedelta, timezone
from typing import Optional

from airbyte_cdk.sources.declarative.async_job.timer import Timer
from airbyte_cdk.sources.types import StreamSlice

from .status import AsyncJobStatus


class AsyncJob:
    """
    Description of an API job.

    Note that the timer will only stop once `update_status` is called so the job might be completed on the API side but until we query for
    it and call `ApiJob.update_status`, `ApiJob.status` will not reflect the actual API side status.
    """

    def __init__(
        self,
        api_job_id: str,
        job_parameters: StreamSlice,
        timeout: Optional[timedelta] = None,
        is_creation_failure: bool = False,
    ) -> None:
        self._api_job_id = api_job_id
        self._job_parameters = job_parameters
        self._status = AsyncJobStatus.RUNNING
        self._retry_after: Optional[datetime] = None
        self._is_creation_failure = is_creation_failure

        timeout = timeout if timeout else timedelta(minutes=60)
        self._timer = Timer(timeout)
        self._timer.start()

    def api_job_id(self) -> str:
        return self._api_job_id

    def status(self) -> AsyncJobStatus:
        if self._timer.has_timed_out():
            # TODO: we should account the fact that,
            # certain APIs could send the `Timeout` status,
            # thus we should not return `Timeout` in that case,
            # but act based on the scenario.

            # the default behavior is to return `Timeout` status and retry.
            return AsyncJobStatus.TIMED_OUT
        return self._status

    def job_parameters(self) -> StreamSlice:
        return self._job_parameters

    def update_status(self, status: AsyncJobStatus) -> None:
        if self._status != AsyncJobStatus.RUNNING and status == AsyncJobStatus.RUNNING:
            self._timer.start()
        elif status.is_terminal():
            self._timer.stop()

        self._status = status

    def is_creation_failure(self) -> bool:
        """Return True if this job was never actually created on the API side."""
        return self._is_creation_failure

    def set_retry_after(self, retry_after: datetime) -> None:
        """Set the earliest time this job can be retried."""
        self._retry_after = retry_after

    def retry_deferred(self) -> bool:
        """Return True if a deferred retry has been scheduled."""
        return self._retry_after is not None

    def ready_to_retry(self) -> bool:
        """Return True if the job has no deferred retry or the wait period has elapsed."""
        if self._retry_after is None:
            return True
        return datetime.now(tz=timezone.utc) >= self._retry_after

    def __repr__(self) -> str:
        return f"AsyncJob(api_job_id={self.api_job_id()}, job_parameters={self.job_parameters()}, status={self.status()})"
