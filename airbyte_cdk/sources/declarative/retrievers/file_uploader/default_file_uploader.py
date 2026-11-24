#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
import logging
import uuid
from dataclasses import InitVar, dataclass, field
from pathlib import Path
from time import sleep
from typing import Any, Iterable, Mapping, Optional, Sequence, Union

from requests import Response
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from urllib3.exceptions import IncompleteRead, ProtocolError

from airbyte_cdk.models import AirbyteRecordMessageFileReference, FailureType
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import (
    InterpolatedString,
)
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import (
    SafeResponse,
)
from airbyte_cdk.sources.declarative.requesters import HttpRequester, Requester
from airbyte_cdk.sources.declarative.types import Record, StreamSlice
from airbyte_cdk.sources.streams.http.http_client import HttpRetryBudget
from airbyte_cdk.sources.types import Config
from airbyte_cdk.sources.utils.files_directory import get_files_directory
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from .file_type_policy import FileTypePolicy
from .file_uploader import FileUploader
from .file_writer import FileWriter

logger = logging.getLogger("airbyte")

DEFAULT_MAX_FILE_SIZE_BYTES = 1_500_000_000
BUILDER_MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024
DOWNLOAD_CHUNK_SIZE_BYTES = 64 * 1024
MAX_DOWNLOAD_RETRIES = 2


class _AttachmentReadError(Exception):
    pass


def _safe_relative_path(value: Any, field_name: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty relative path")

    relative_path = Path(value)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError(f"{field_name} must remain within the files directory")

    normalized_path = Path(*relative_path.parts)
    if not normalized_path.parts:
        raise ValueError(f"{field_name} must be a non-empty relative path")

    return normalized_path


@dataclass
class DefaultFileUploader(FileUploader):
    """
    File uploader class
    Handles the upload logic: fetching the download target, making the request via its requester, determining the file path, and calling self.file_writer.write()
    Different types of file_writer:BaseFileWriter can be injected to handle different file writing strategies.
    """

    requester: Requester
    download_target_extractor: RecordExtractor
    config: Config
    file_writer: FileWriter
    parameters: InitVar[Mapping[str, Any]]

    filename_extractor: Optional[Union[InterpolatedString, str]] = None
    max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES
    allowed_extensions: Optional[Sequence[str]] = None
    allowed_content_types: Optional[Sequence[str]] = None
    _file_type_policy: Optional[FileTypePolicy] = field(init=False, default=None)

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if self.allowed_extensions is not None or self.allowed_content_types is not None:
            self._file_type_policy = FileTypePolicy(
                self.allowed_extensions, self.allowed_content_types
            )
        if self.filename_extractor:
            self.filename_extractor = InterpolatedString.create(
                self.filename_extractor,
                parameters=parameters,
            )

    def upload(self, record: Record) -> None:
        mocked_response = SafeResponse()
        mocked_response.content = json.dumps(record.data).encode()
        download_targets = list(self.download_target_extractor.extract_records(mocked_response))
        if not download_targets:
            raise ValueError("No download targets found")

        download_target = download_targets[0]  # we just expect one download target
        if not isinstance(download_target, str):
            raise ValueError(
                f"download_target is expected to be a str but was {type(download_target)}: {download_target}"
            )

        files_directory = Path(get_files_directory())

        file_name = (
            self.filename_extractor.eval(self.config, record=record)
            if self.filename_extractor
            else str(uuid.uuid4())
        )
        stream_path = _safe_relative_path(record.stream_name, "stream name")
        filename_path = _safe_relative_path(file_name, "filename")
        file_relative_path = stream_path / filename_path
        expected_type = (
            self._file_type_policy.expected_type_from_filename(str(filename_path))
            if self._file_type_policy
            else None
        )
        full_path = files_directory / file_relative_path
        stream_slice = StreamSlice(
            partition={}, cursor_slice={}, extra_fields={"download_target": download_target}
        )
        file_size_bytes = self._download(full_path, stream_slice, expected_type)

        logger.info("File uploaded successfully")
        logger.info(f"File url: {str(full_path)}")
        logger.info(f"File size: {file_size_bytes / 1024} KB")
        logger.info(f"File relative path: {str(file_relative_path)}")

        record.file_reference = AirbyteRecordMessageFileReference(
            staging_file_url=str(full_path),
            source_file_relative_path=str(file_relative_path),
            file_size_bytes=file_size_bytes,
        )

    def _download(
        self, full_path: Path, stream_slice: StreamSlice, expected_type: Optional[str]
    ) -> int:
        retries = self._download_retries()
        retry_budget = HttpRetryBudget()
        attempt = 1
        while True:
            try:
                return self._download_once(full_path, stream_slice, expected_type, retry_budget)
            except _AttachmentReadError as error:
                delay = 2 ** (attempt - 1)
                if attempt > retries or not retry_budget.can_retry(delay):
                    raise AirbyteTracedException(
                        message="The attachment download fails because the source connection does not deliver the complete file.",
                        internal_message=f"Attachment body read retry budget exhausted. Download attempts: {attempt}.",
                        failure_type=FailureType.transient_error,
                        exception=error.__cause__,
                    ) from error
                logger.info("Attachment download interrupted; restarting in %s second(s).", delay)
                sleep(delay)
                attempt += 1

    def _download_retries(self) -> int:
        if isinstance(self.requester, HttpRequester):
            if self.requester.disable_retries:
                return 0
            if self.requester.error_handler is not None:
                configured_retries = self.requester.error_handler.max_retries
                if configured_retries is not None:
                    return max(0, min(configured_retries, MAX_DOWNLOAD_RETRIES))
        return MAX_DOWNLOAD_RETRIES

    def _download_once(
        self,
        full_path: Path,
        stream_slice: StreamSlice,
        expected_type: Optional[str],
        retry_budget: HttpRetryBudget,
    ) -> int:
        if isinstance(self.requester, HttpRequester):
            response = self.requester.send_request(
                stream_slice=stream_slice,
                stream_response=True,
                bypass_cache=True,
                retry_budget=retry_budget,
            )
        else:
            response = self.requester.send_request(stream_slice=stream_slice)
        if response is None:
            raise ValueError("No response was returned for the file download")
        with response:
            chunks = self._bounded_chunks(response)
            if self._file_type_policy:
                declared_type = self._file_type_policy.validate_content_type(
                    response.headers.get("Content-Type", "")
                )
                chunks = self._file_type_policy.validate_stream(
                    chunks, expected_type, declared_type
                )
            return self.file_writer.write(full_path, chunks)

    def _bounded_chunks(self, response: Response) -> Iterable[bytes]:
        content_length = response.headers.get("Content-Length", "")
        if content_length.isascii() and content_length.isdecimal():
            declared_size = content_length.lstrip("0")
            limit = str(self.max_file_size_bytes)
            if (len(declared_size), declared_size) > (len(limit), limit):
                raise self._file_size_error()

        size = 0
        for chunk in self._body_chunks(response):
            size += len(chunk)
            if size > self.max_file_size_bytes:
                raise self._file_size_error()
            if chunk:
                yield chunk

    @staticmethod
    def _body_chunks(response: Response) -> Iterable[bytes]:
        try:
            yield from response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE_BYTES)
        except (
            ChunkedEncodingError,
            ConnectionError,
            ReadTimeout,
            ProtocolError,
            IncompleteRead,
        ) as error:
            raise _AttachmentReadError from error

    def _file_size_error(self) -> AirbyteTracedException:
        return AirbyteTracedException(
            message=(
                f"The attachment exceeds the {self.max_file_size_bytes}-byte download limit. "
                "Use a smaller attachment or exclude it from the stream."
            ),
            failure_type=FailureType.config_error,
        )
