import json
from pathlib import Path
from typing import Optional

from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import (
    SafeResponse,
)
from airbyte_cdk.sources.declarative.requesters import Requester
from airbyte_cdk.sources.declarative.types import Record, StreamSlice


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
        mocked_response.content = json.dumps(record.data)
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
            with open(str(Path(__file__).parent / record.data["file_name"]), "ab") as f:
                f.write(response.content)
