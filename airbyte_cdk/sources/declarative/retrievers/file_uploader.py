#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
import logging
import uuid
from dataclasses import InitVar, dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Union

from abc import ABC, abstractmethod
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

logger = logging.getLogger("airbyte")

@dataclass
class BaseFileUploader(ABC):
    """
    Base class for file uploader
    """

    @abstractmethod
    def upload(self, record: Record) -> None:
        """
        Uploads the file to the specified location
        """
        ...

class BaseFileWriter(ABC):
    """
    Base File writer class
    """

    @abstractmethod
    def write(self, file_path: Path, content: bytes) -> int:
        """
        Writes the file to the specified location
        """
        ...

class FileWriter(BaseFileWriter):

    def write(self, file_path: Path, content: bytes) -> int:
        """
        Writes the file to the specified location
        """
        with open(str(file_path), "wb") as f:
            f.write(content)

        return file_path.stat().st_size

class NoopFileWriter(BaseFileWriter):

    def write(self, file_path: Path, content: bytes) -> int:
        """
        Noop file writer
        """
        return 0

@dataclass
class FileUploader(BaseFileUploader):
    requester: Requester
    download_target_extractor: RecordExtractor
    config: Config
    file_writer: BaseFileWriter
    parameters: InitVar[Mapping[str, Any]]

    filename_extractor: Optional[Union[InterpolatedString, str]] = None
    content_extractor: Optional[RecordExtractor] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if self.filename_extractor:
            self.filename_extractor = InterpolatedString.create(
                self.filename_extractor,
                parameters=parameters,
            )

    def upload(self, record: Record) -> None:
        mocked_response = SafeResponse()
        mocked_response.content = json.dumps(record.data).encode()
        download_target = list(self.download_target_extractor.extract_records(mocked_response))[0]
        if not isinstance(download_target, str):
            raise ValueError(
                f"download_target is expected to be a str but was {type(download_target)}: {download_target}"
            )

        response = self.requester.send_request(
            stream_slice=StreamSlice(
                partition={}, cursor_slice={}, extra_fields={"download_target": download_target}
            ),
        )

        if self.content_extractor:
            raise NotImplementedError("TODO")
        else:
            files_directory = Path(get_files_directory())

            file_name = (
                self.filename_extractor.eval(self.config, record=record)
                if self.filename_extractor
                else str(uuid.uuid4())
            )
            file_name = file_name.lstrip("/")
            file_relative_path = Path(record.stream_name) / Path(file_name)

            full_path = files_directory / file_relative_path
            full_path.parent.mkdir(parents=True, exist_ok=True)

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


@dataclass
class ConnectorBuilderFileUploader(BaseFileUploader):
    file_uploader: FileUploader

    def upload(self, record: Record) -> None:
        self.file_uploader.upload(record=record)
        for file_reference_attribute in [file_reference_attribute for file_reference_attribute in record.file_reference.__dict__ if not file_reference_attribute.startswith('_')]:
            record.data[file_reference_attribute] = getattr(record.file_reference, file_reference_attribute)
