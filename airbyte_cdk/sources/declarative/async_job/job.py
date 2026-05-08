# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


from datetime import timedelta
from typing import Optional

from airbyte_cdk.sources.declarative.async_job.timer import Timer
from airbyte_cdk.sources.types import StreamSlice
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from .status import AsyncJobStatus


class AsyncJob:
    """
    Description of an API job.

    Note that the timer will only stop once `update_status` is called so the job might be completed on the API side but until we query for
    it and call `ApiJob.update_status`, `ApiJob.status` will not reflect the actual API side status.
    """

    def __init__(
        self, api_job_id: str, job_parameters: StreamSlice, timeout: Optional[timedelta] = None
    ) -> None:
        self._api_job_id = api_job_id
        self._job_parameters = job_parameters
        self._status = AsyncJobStatus.RUNNING
        self._failure_exception: Optional[AirbyteTracedException] = None

        timeout = timeout if timeout else timedelta(minutes=60)
        self._timer = Timer(timeout)
        self._timer.start()

    def failure_exception(self) -> Optional[AirbyteTracedException]:
        """
        Return the exception that caused this job to fail, if any.

        This is set by the orchestrator (or repository) when a job transitions
        to a terminal failure state so that downstream error aggregation can
        preserve the original `failure_type` instead of collapsing every async
        job failure to `system_error`.
        """
        return self._failure_exception

    def set_failure_exception(self, exception: Optional[Exception]) -> None:
        """
        Attach the originating exception for a failed job.

        Wraps non-`AirbyteTracedException` values via `AirbyteTracedException.from_exception`
        so the stored value always carries a `failure_type`.
        """
        if exception is None:
            self._failure_exception = None
            return
        self._failure_exception = (
            exception
            if isinstance(exception, AirbyteTracedException)
            else AirbyteTracedException.from_exception(exception)
        )

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

    def __repr__(self) -> str:
        return f"AsyncJob(api_job_id={self.api_job_id()}, job_parameters={self.job_parameters()}, status={self.status()})"
