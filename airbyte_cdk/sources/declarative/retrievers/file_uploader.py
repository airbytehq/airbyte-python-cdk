import json
from pathlib import Path
from typing import Optional

from airbyte_cdk.models import AirbyteRecordMessageFileReference
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import (
    SafeResponse,
)
from airbyte_cdk.sources.declarative.requesters import Requester
from airbyte_cdk.sources.declarative.types import Record, StreamSlice
from airbyte_cdk.sources.utils.files_directory import get_files_directory


class FileUploader:
    def __init__(
        self,
        requester: Requester,
        download_target_extractor: RecordExtractor,
        content_extractor: Optional[RecordExtractor] = None,
    ) -> None:
        self._requester = requester
        self._download_target_extractor = download_target_extractor
        self._content_extractor = content_extractor

    def upload(self, record: Record) -> None:
        # TODO validate record shape - is the transformation applied at this point?
        mocked_response = SafeResponse()
        mocked_response.content = json.dumps(record.data).encode("utf-8")
        download_target = list(self._download_target_extractor.extract_records(mocked_response))[0]
        if not isinstance(download_target, str):
            raise ValueError(
                f"download_target is expected to be a str but was {type(download_target)}: {download_target}"
            )

        response = self._requester.send_request(
            stream_slice=StreamSlice(
                partition={}, cursor_slice={}, extra_fields={"download_target": download_target}
            ),
        )

        if self._content_extractor:
            raise NotImplementedError("TODO")
        else:
            files_directory = Path(get_files_directory())
            # TODO:: we could either interpolate record data if some relative_path is provided or
            #  use partition_field value in the slice {"partition_field": some_value_id} to create a path
            file_relative_path = Path(record.stream_name) / record.data["file_name"]

            full_path = files_directory / file_relative_path
            full_path.parent.mkdir(parents=True, exist_ok=True)

            with open(str(full_path), "wb") as f:
                f.write(response.content)
            file_size_bytes = full_path.stat().st_size

            record.file_reference = AirbyteRecordMessageFileReference(
                file_url=download_target,
                file_relative_path=str(file_relative_path),
                file_size_bytes=file_size_bytes,
            )
