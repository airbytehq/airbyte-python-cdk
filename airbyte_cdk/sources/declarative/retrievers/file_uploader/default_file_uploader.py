#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
import logging
import uuid
from dataclasses import InitVar, dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Union

from airbyte_cdk.models import AirbyteRecordMessageFileReference
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import (
    InterpolatedString,
)
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import (
    SafeResponse,
)
from airbyte_cdk.sources.declarative.requesters import Requester
from airbyte_cdk.sources.declarative.types import Record, StreamSlice
from airbyte_cdk.sources.types import Config
from airbyte_cdk.sources.utils.files_directory import get_files_directory

from .file_uploader import FileUploader
from .file_writer import FileWriter

logger = logging.getLogger("airbyte")


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

        response = self.requester.send_request(
            stream_slice=StreamSlice(
                partition={}, cursor_slice={}, extra_fields={"download_target": download_target}
            ),
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

        file_size_bytes = self.file_writer.write(full_path, content=response.content)

        logger.info("File uploaded successfully")
        logger.info(f"File url: {str(full_path)}")
        logger.info(f"File size: {file_size_bytes / 1024} KB")
        logger.info(f"File relative path: {str(file_relative_path)}")

        record.file_reference = AirbyteRecordMessageFileReference(
            staging_file_url=str(full_path),
            source_file_relative_path=str(file_relative_path),
            file_size_bytes=file_size_bytes,
        )
