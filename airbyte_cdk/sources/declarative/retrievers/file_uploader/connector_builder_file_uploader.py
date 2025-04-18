#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass

from airbyte_cdk.sources.declarative.types import Record

from .base_file_uploader import BaseFileUploader
from .file_uploader import FileUploader


@dataclass
class ConnectorBuilderFileUploader(BaseFileUploader):
    """
    Connector builder file uploader
    Acts as a decorator or wrapper around a FileUploader instance, copying the attributes from record.file_reference into the record.data.
    """

    file_uploader: FileUploader

    def upload(self, record: Record) -> None:
        self.file_uploader.upload(record=record)
        for file_reference_attribute in [
            file_reference_attribute
            for file_reference_attribute in record.file_reference.__dict__
            if not file_reference_attribute.startswith("_")
        ]:
            record.data[file_reference_attribute] = getattr( # type: ignore
                record.file_reference, file_reference_attribute
            )
