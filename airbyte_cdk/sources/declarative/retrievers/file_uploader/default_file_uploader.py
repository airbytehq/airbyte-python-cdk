#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
import logging
import uuid
from dataclasses import InitVar, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Union

from requests import Response

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
from airbyte_cdk.sources.types import Config
from airbyte_cdk.sources.utils.files_directory import get_files_directory
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from .file_uploader import FileUploader
from .file_writer import FileWriter

logger = logging.getLogger("airbyte")

DEFAULT_MAX_FILE_SIZE_BYTES = 1_500_000_000
BUILDER_MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024
DOWNLOAD_CHUNK_SIZE_BYTES = 64 * 1024


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

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
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

        full_path = files_directory / file_relative_path
        stream_slice = StreamSlice(
            partition={}, cursor_slice={}, extra_fields={"download_target": download_target}
        )
        if isinstance(self.requester, HttpRequester):
            response = self.requester.send_request(
                stream_slice=stream_slice, stream_response=True, bypass_cache=True
            )
        else:
            response = self.requester.send_request(stream_slice=stream_slice)
        if response is None:
            raise ValueError("No response was returned for the file download")
        with response:
            file_size_bytes = self.file_writer.write(full_path, self._bounded_chunks(response))

        logger.info("File uploaded successfully")
        logger.info(f"File url: {str(full_path)}")
        logger.info(f"File size: {file_size_bytes / 1024} KB")
        logger.info(f"File relative path: {str(file_relative_path)}")

        record.file_reference = AirbyteRecordMessageFileReference(
            staging_file_url=str(full_path),
            source_file_relative_path=str(file_relative_path),
            file_size_bytes=file_size_bytes,
        )

    def _bounded_chunks(self, response: Response) -> Iterable[bytes]:
        content_length = response.headers.get("Content-Length", "")
        if content_length.isascii() and content_length.isdecimal():
            declared_size = content_length.lstrip("0")
            limit = str(self.max_file_size_bytes)
            if (len(declared_size), declared_size) > (len(limit), limit):
                raise self._file_size_error()

        size = 0
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE_BYTES):
            size += len(chunk)
            if size > self.max_file_size_bytes:
                raise self._file_size_error()
            if chunk:
                yield chunk

    def _file_size_error(self) -> AirbyteTracedException:
        return AirbyteTracedException(
            message=(
                f"The attachment exceeds the {self.max_file_size_bytes}-byte download limit. "
                "Use a smaller attachment or exclude it from the stream."
            ),
            failure_type=FailureType.config_error,
        )
