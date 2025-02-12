#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import traceback
from typing import Any, Dict, Iterable

from airbyte_cdk.models import AirbyteLogMessage, AirbyteMessage, Level
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.file_based.types import StreamSlice
from airbyte_cdk.sources.streams.core import JsonSchema
from airbyte_cdk.sources.utils.record_helper import stream_data_to_airbyte_message
from airbyte_cdk.sources.file_based.stream import DefaultFileBasedStream


class PermissionsFileBasedStream(DefaultFileBasedStream):
    """
    The permissions stream, stream_reader on source handles logic for schemas and ACLs permissions.
    """

    def _filter_schema_invalid_properties(
        self, configured_catalog_json_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        return self.stream_reader.file_permissions_schema

    def read_records_from_slice(self, stream_slice: StreamSlice) -> Iterable[AirbyteMessage]:
        """
        Yield permissions records from all remote files
        """
        for file in stream_slice["files"]:
            file_datetime_string = file.last_modified.strftime(self.DATE_TIME_FORMAT)
            try:
                permissions_record = self.stream_reader.get_file_acl_permissions(
                    file, logger=self.logger
                )
                permissions_record = self.transform_record(
                    permissions_record, file, file_datetime_string
                )
                yield stream_data_to_airbyte_message(
                    self.name, permissions_record, is_file_transfer_message=False
                )
            except Exception as e:
                self.logger.error(f"Failed to retrieve permissions for file {file.uri}: {str(e)}")
                yield AirbyteMessage(
                    type=MessageType.LOG,
                    log=AirbyteLogMessage(
                        level=Level.ERROR,
                        message=f"Error retrieving files permissions: stream={self.name} file={file.uri}",
                        stack_trace=traceback.format_exc(),
                    ),
                )

    def _get_raw_json_schema(self) -> JsonSchema:
        return self.stream_reader.file_permissions_schema
