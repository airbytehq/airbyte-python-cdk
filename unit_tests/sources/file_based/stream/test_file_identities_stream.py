#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from airbyte_protocol_dataclasses.models import SyncMode

from airbyte_cdk.sources.file_based.discovery_policy import AbstractDiscoveryPolicy
from airbyte_cdk.sources.file_based.exceptions import (
    FileBasedErrorsCollector,
)
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader
from airbyte_cdk.sources.file_based.stream import FileIdentitiesStream


class MockFormat:
    pass


class IdentitiesFileBasedStreamTest(unittest.TestCase):
    _NOW = datetime(2022, 10, 22, tzinfo=timezone.utc)
    _A_RECORD = {
        "id": "userid1",
        "remote_id": "user1@domain.com",
        "name": "user one",
        "email_address": "user1@domain.com",
        "member_email_addresses": ["user1@domain.com", "user1@domain.com.test-google-a.com"],
        "type": "user",
        "modified_at": "2025-02-12T23:06:45.304942+00:00",
    }

    _GROUP_RECORD = {
        "id": "groupid1",
        "remote_id": "team_work@domain.com",
        "name": "team work",
        "email_address": "team_work@domain.com",
        "member_email_addresses": ["user1@domain.com", "user2@domain.com"],
        "type": "group",
        "modified_at": "2025-02-12T23:06:45.604572+00:00",
    }

    _IDENTITIES_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "remote_id": {"type": "string"},
            "parent_id": {"type": ["null", "string"]},
            "name": {"type": ["null", "string"]},
            "description": {"type": ["null", "string"]},
            "email_address": {"type": ["null", "string"]},
            "member_email_addresses": {"type": ["null", "array"]},
            "type": {"type": "string"},
            "modified_at": {"type": "string"},
        },
    }

    def setUp(self) -> None:
        self._catalog_schema = Mock()
        self._stream_reader = Mock(spec=AbstractFileBasedStreamReader)
        self._discovery_policy = Mock(spec=AbstractDiscoveryPolicy)

        self._stream_reader.identities_schema = self._IDENTITIES_SCHEMA

        self._stream = FileIdentitiesStream(
            catalog_schema=self._catalog_schema,
            stream_reader=self._stream_reader,
            discovery_policy=self._discovery_policy,
            errors_collector=FileBasedErrorsCollector(),
        )

    def test_when_read_records_then_return_records(self) -> None:
        self._stream_reader.load_identity_groups.return_value = [self._A_RECORD, self._GROUP_RECORD]
        messages = list(self._stream.read_records(SyncMode.full_refresh))
        assert list(map(lambda message: message.record.data, messages)) == [
            self._A_RECORD,
            self._GROUP_RECORD,
        ]

    def test_when_getting_schema(self):
        returned_schema = self._stream.get_json_schema()
        assert returned_schema == self._IDENTITIES_SCHEMA

    def test_when_read_records_and_raise_exception(self) -> None:
        self._stream_reader.load_identity_groups.side_effect = Exception(
            "Identities retrieval failed"
        )

        messages = list(self._stream.read_records(SyncMode.full_refresh))
        assert (
            messages[0].log.message
            == "Error trying to read identities: Identities retrieval failed stream=identities"
        )
