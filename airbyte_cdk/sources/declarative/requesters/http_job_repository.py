# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
import logging
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, Iterable, Mapping, Optional

import requests
from requests import Response

from airbyte_cdk import AirbyteMessage
from airbyte_cdk.logger import lazy_log
from airbyte_cdk.models import FailureType, Type
from airbyte_cdk.sources.declarative.async_job.job import AsyncJob
from airbyte_cdk.sources.declarative.async_job.repository import AsyncJobRepository
from airbyte_cdk.sources.declarative.async_job.status import AsyncJobStatus
from airbyte_cdk.sources.declarative.extractors.dpath_extractor import (
    DpathExtractor,
    RecordExtractor,
)
from airbyte_cdk.sources.declarative.extractors.response_to_file_extractor import (
    ResponseToFileExtractor,
)
from airbyte_cdk.sources.declarative.requesters.requester import Requester
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import SimpleRetriever
from airbyte_cdk.sources.types import Record, StreamSlice
from airbyte_cdk.utils import AirbyteTracedException

LOGGER = logging.getLogger("airbyte")


@dataclass
class AsyncHttpJobRepository(AsyncJobRepository):
    """
    See Readme file for more details about flow.
    """

    creation_requester: Requester
    polling_requester: Requester
    download_retriever: SimpleRetriever
    abort_requester: Optional[Requester]
    delete_requester: Optional[Requester]
    status_extractor: DpathExtractor
    status_mapping: Mapping[str, AsyncJobStatus]
    urls_extractor: DpathExtractor

    job_timeout: Optional[timedelta] = None
    record_extractor: RecordExtractor = field(
        init=False, repr=False, default_factory=lambda: ResponseToFileExtractor({})
    )
    url_requester: Optional[Requester] = (
        None  # use it in case polling_requester provides some <id> and extra request is needed to obtain list of urls to download from
    )

    def __post_init__(self) -> None:
        self._create_job_response_by_id: Dict[str, Response] = {}
        self._polling_job_response_by_id: Dict[str, Response] = {}

    def _get_validated_polling_response(self, stream_slice: StreamSlice) -> requests.Response:
        """
        Validates and retrieves the pooling response for a given stream slice.

        Args:
            stream_slice (StreamSlice): The stream slice to send the pooling request for.

        Returns:
            requests.Response: The validated pooling response.

        Raises:
            AirbyteTracedException: If the polling request returns an empty response.
        """

        polling_response: Optional[requests.Response] = self.polling_requester.send_request(
            stream_slice=stream_slice
        )
        if polling_response is None:
            raise AirbyteTracedException(
                internal_message="Polling Requester received an empty Response.",
                failure_type=FailureType.system_error,
            )
        return polling_response

    def _get_validated_job_status(self, response: requests.Response) -> AsyncJobStatus:
        """
        Validates the job status extracted from the API response.

        Args:
            response (requests.Response): The API response.

        Returns:
            AsyncJobStatus: The validated job status.

        Raises:
            ValueError: If the API status is unknown.
        """

        api_status = next(iter(self.status_extractor.extract_records(response)), None)
        job_status = self.status_mapping.get(str(api_status), None)
        if job_status is None:
            raise ValueError(
                f"API status `{api_status}` is unknown. Contact the connector developer to make sure this status is supported."
            )

        return job_status

    def _start_job_and_validate_response(self, stream_slice: StreamSlice) -> requests.Response:
        """
        Starts a job and validates the response.

        Args:
            stream_slice (StreamSlice): The stream slice to be used for the job.

        Returns:
            requests.Response: The response from the job creation requester.

        Raises:
            AirbyteTracedException: If no response is received from the creation requester.
        """

        response: Optional[requests.Response] = self.creation_requester.send_request(
            stream_slice=stream_slice
        )
        if not response:
            raise AirbyteTracedException(
                internal_message="Always expect a response or an exception from creation_requester",
                failure_type=FailureType.system_error,
            )

        return response

    def start(self, stream_slice: StreamSlice) -> AsyncJob:
        """
        Starts a job for the given stream slice.

        Args:
            stream_slice (StreamSlice): The stream slice to start the job for.

        Returns:
            AsyncJob: The asynchronous job object representing the started job.
        """

        response: requests.Response = self._start_job_and_validate_response(stream_slice)
        job_id: str = str(uuid.uuid4())
        self._create_job_response_by_id[job_id] = response

        return AsyncJob(api_job_id=job_id, job_parameters=stream_slice, timeout=self.job_timeout)

    def update_jobs_status(self, jobs: Iterable[AsyncJob]) -> None:
        """
        Updates the status of multiple jobs.

        Because we don't have interpolation on random fields, we have this hack which consist on using the stream_slice to allow for
        interpolation. We are looking at enabling interpolation on more field which would require a change to those three layers:
        HttpRequester, RequestOptionProvider, RequestInputProvider.

        Args:
            jobs (Iterable[AsyncJob]): An iterable of AsyncJob objects representing the jobs to update.

        Returns:
            None
        """
        for job in jobs:
            stream_slice = self._get_create_job_stream_slice(job)
            polling_response: requests.Response = self._get_validated_polling_response(stream_slice)
            job_status: AsyncJobStatus = self._get_validated_job_status(polling_response)

            if job_status != job.status():
                lazy_log(
                    LOGGER,
                    logging.DEBUG,
                    lambda: f"Status of job {job.api_job_id()} changed from {job.status()} to {job_status}",
                )
            else:
                lazy_log(
                    LOGGER,
                    logging.DEBUG,
                    lambda: f"Status of job {job.api_job_id()} is still {job.status()}",
                )

            job.update_status(job_status)
            if job_status == AsyncJobStatus.COMPLETED:
                self._polling_job_response_by_id[job.api_job_id()] = polling_response

    def fetch_records(self, job: AsyncJob) -> Iterable[Mapping[str, Any]]:
        """
        Fetches records from the given job.

        Args:
            job (AsyncJob): The job to fetch records from.

        Yields:
            Iterable[Mapping[str, Any]]: A generator that yields records as dictionaries.

        """

        for url in self._get_download_url(job):
            job_slice = job.job_parameters()
            stream_slice = StreamSlice(
                partition=job_slice.partition,
                cursor_slice=job_slice.cursor_slice,
                extra_fields={**job_slice.extra_fields, "url": url},
            )
            for message in self.download_retriever.read_records({}, stream_slice):
                if isinstance(message, Record):
                    yield message.data
                elif isinstance(message, AirbyteMessage):
                    if message.type == Type.RECORD:
                        yield message.record.data  # type: ignore  # message.record won't be None here as the message is a record
                elif isinstance(message, (dict, Mapping)):
                    yield message
                else:
                    raise TypeError(f"Unknown type `{type(message)}` for message")

        yield from []

    def abort(self, job: AsyncJob) -> None:
        if not self.abort_requester:
            return

        self.abort_requester.send_request(stream_slice=self._get_create_job_stream_slice(job))

    def delete(self, job: AsyncJob) -> None:
        if not self.delete_requester:
            return

        self.delete_requester.send_request(stream_slice=self._get_create_job_stream_slice(job))
        self._clean_up_job(job.api_job_id())

    def _clean_up_job(self, job_id: str) -> None:
        del self._create_job_response_by_id[job_id]
        del self._polling_job_response_by_id[job_id]

    def _get_create_job_stream_slice(self, job: AsyncJob) -> StreamSlice:
        stream_slice = StreamSlice(
            partition={"create_job_response": self._create_job_response_by_id[job.api_job_id()]},
            cursor_slice={},
        )
        return stream_slice

    def _get_download_url(self, job: AsyncJob) -> Iterable[str]:
        if not self.url_requester:
            url_response = self._polling_job_response_by_id[job.api_job_id()]
        else:
            stream_slice: StreamSlice = StreamSlice(
                partition={
                    "polling_job_response": self._polling_job_response_by_id[job.api_job_id()]
                },
                cursor_slice={},
            )
            url_response = self.url_requester.send_request(stream_slice=stream_slice)  # type: ignore # we expect url_requester to always be presented, otherwise raise an exception as we cannot proceed with the report
            if not url_response:
                raise AirbyteTracedException(
                    internal_message="Always expect a response or an exception from url_requester",
                    failure_type=FailureType.system_error,
                )

        yield from self.urls_extractor.extract_records(url_response)  # type: ignore # we expect urls_extractor to always return list of strings
