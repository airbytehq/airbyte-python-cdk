#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import unittest
from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import Mock

from airbyte_cdk.sources.file_based.availability_strategy import (
    AbstractFileBasedAvailabilityStrategy,
)
from airbyte_cdk.sources.file_based.discovery_policy import AbstractDiscoveryPolicy
from airbyte_cdk.sources.file_based.exceptions import (
    FileBasedErrorsCollector,
)
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_validation_policies import AbstractSchemaValidationPolicy
from airbyte_cdk.sources.file_based.stream.cursor import AbstractFileBasedCursor
from airbyte_cdk.sources.file_based.stream.permissions_file_based_stream import (
    PermissionsFileBasedStream,
)


class MockFormat:
    pass


class PermissionsFileBasedStreamTest(unittest.TestCase):
    _NOW = datetime(2022, 10, 22, tzinfo=timezone.utc)
    _A_RECORD = {
        "id": "some_id",
        "file_path": "Company_Files/Accounting/Financial_Statements/2023/February/Expenses_Feb.xlsx",
        "allowed_identity_remote_ids": ["integration-test@somedomain.com"],
        "publicly_accessible": False,
    }

    _A_PERMISSIONS_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "file_path": {"type": "string"},
            "allowed_identity_remote_ids": {"type": "array", "items": {"type": "string"}},
            "publicly_accessible": {"type": "boolean"},
        },
    }

    def setUp(self) -> None:
        self._stream_config = Mock()
        self._stream_config.format = MockFormat()
        self._stream_config.name = "a stream name"
        self._catalog_schema = Mock()
        self._stream_reader = Mock(spec=AbstractFileBasedStreamReader)
        self._availability_strategy = Mock(spec=AbstractFileBasedAvailabilityStrategy)
        self._discovery_policy = Mock(spec=AbstractDiscoveryPolicy)
        self._parser = Mock(spec=FileTypeParser)
        self._validation_policy = Mock(spec=AbstractSchemaValidationPolicy)
        self._validation_policy.name = "validation policy name"
        self._cursor = Mock(spec=AbstractFileBasedCursor)

        self._stream_reader.file_permissions_schema = self._A_PERMISSIONS_SCHEMA

        self._stream = PermissionsFileBasedStream(
            config=self._stream_config,
            catalog_schema=self._catalog_schema,
            stream_reader=self._stream_reader,
            availability_strategy=self._availability_strategy,
            discovery_policy=self._discovery_policy,
            parsers={MockFormat: self._parser},
            validation_policy=self._validation_policy,
            cursor=self._cursor,
            errors_collector=FileBasedErrorsCollector(),
        )

    def test_when_read_records_from_slice_then_return_records(self) -> None:
        self._stream_reader.get_file_acl_permissions.return_value = self._A_RECORD
        messages = list(
            self._stream.read_records_from_slice(
                {"files": [RemoteFile(uri="uri", last_modified=self._NOW)]}
            )
        )
        assert list(map(lambda message: message.record.data, messages)) == [self._A_RECORD]

    def test_when_transform_record_then_return_updated_record(self) -> None:
        file = RemoteFile(uri="uri", last_modified=self._NOW)
        last_updated = self._NOW.isoformat()
        transformed_record = self._stream.transform_record(self._A_RECORD, file, last_updated)
        assert transformed_record[self._stream.ab_last_mod_col] == last_updated
        assert transformed_record[self._stream.ab_file_name_col] == file.uri

    def test_when_getting_schema(self):
        returned_schema = self._stream.get_json_schema()
        expected_schema = deepcopy(self._A_PERMISSIONS_SCHEMA)
        expected_schema["properties"][PermissionsFileBasedStream.ab_last_mod_col] = {
            "type": "string"
        }
        expected_schema["properties"][PermissionsFileBasedStream.ab_file_name_col] = {
            "type": "string"
        }
        assert returned_schema == expected_schema

    def test_when_read_records_from_slice_and_raise_exception(self) -> None:
        self._stream_reader.get_file_acl_permissions.side_effect = Exception(
            "ACL permissions retrieval failed"
        )

        messages = list(
            self._stream.read_records_from_slice(
                {"files": [RemoteFile(uri="uri", last_modified=self._NOW)]}
            )
        )
        assert (
            messages[0].log.message
            == "Error retrieving files permissions: stream=a stream name file=uri"
        )

    def test_when_read_records_from_slice_with_empty_permissions_then_return_empty(self) -> None:
        self._stream_reader.get_file_acl_permissions.return_value = {}
        messages = list(
            self._stream.read_records_from_slice(
                {"files": [RemoteFile(uri="uri", last_modified=self._NOW)]}
            )
        )
        assert (
            messages[0].log.message == "Unable to fetch permissions. stream=a stream name file=uri"
        )
