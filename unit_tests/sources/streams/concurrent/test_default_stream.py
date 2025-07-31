#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import unittest
from unittest.mock import Mock

import pytest

from airbyte_cdk.models import AirbyteStream, SyncMode
from airbyte_cdk.sources.message import InMemoryMessageRepository
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor, FinalStateCursor
from airbyte_cdk.sources.streams.concurrent.default_stream import DefaultStream
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.partition_generator import PartitionGenerator
from airbyte_cdk.sources.types import Record
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


class ThreadBasedConcurrentStreamTest(unittest.TestCase):
    def setUp(self):
        self._partition_generator = Mock(spec=PartitionGenerator)
        self._partition = Mock(spec=Partition)
        self._name = "name"
        self._json_schema = {}
        self._primary_key = []
        self._cursor_field = None
        self._logger = Mock()
        self._cursor = Mock(spec=Cursor)
        self._message_repository = InMemoryMessageRepository()
        self._stream = DefaultStream(
            self._partition_generator,
            self._name,
            self._json_schema,
            self._primary_key,
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
        )

    def test_get_json_schema(self):
        json_schema = self._stream.get_json_schema()
        assert json_schema == self._json_schema

    def test_check_for_error_raises_an_exception_if_any_of_the_futures_are_not_done(self):
        futures = [Mock() for _ in range(3)]
        for f in futures:
            f.exception.return_value = None
        futures[0].done.return_value = False

        with self.assertRaises(Exception):
            self._stream._check_for_errors(futures)

    def test_check_for_error_raises_an_exception_if_any_of_the_futures_raised_an_exception(self):
        futures = [Mock() for _ in range(3)]
        for f in futures:
            f.exception.return_value = None
        futures[0].exception.return_value = Exception("error")

        with self.assertRaises(Exception):
            self._stream._check_for_errors(futures)

    def test_as_airbyte_stream(self):
        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=self._json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            source_defined_cursor=None,
            default_cursor_field=None,
            source_defined_primary_key=None,
            namespace=None,
            is_resumable=False,
            is_file_based=False,
        )
        actual_airbyte_stream = self._stream.as_airbyte_stream()

        assert actual_airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_primary_key(self):
        json_schema = {
            "type": "object",
            "properties": {
                "id_a": {"type": ["null", "string"]},
                "id_b": {"type": ["null", "string"]},
            },
        }
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            json_schema,
            ["composite_key_1", "composite_key_2"],
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
        )

        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            source_defined_cursor=None,
            default_cursor_field=None,
            source_defined_primary_key=[["composite_key_1"], ["composite_key_2"]],
            namespace=None,
            is_resumable=False,
            is_file_based=False,
        )

        airbyte_stream = stream.as_airbyte_stream()
        assert airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_composite_primary_key(self):
        json_schema = {
            "type": "object",
            "properties": {
                "id_a": {"type": ["null", "string"]},
                "id_b": {"type": ["null", "string"]},
            },
        }
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            json_schema,
            ["id_a", "id_b"],
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
        )

        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            source_defined_cursor=None,
            default_cursor_field=None,
            source_defined_primary_key=[["id_a"], ["id_b"]],
            namespace=None,
            is_resumable=False,
            is_file_based=False,
        )

        airbyte_stream = stream.as_airbyte_stream()
        assert airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_a_cursor(self):
        json_schema = {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "string"]},
                "date": {"type": ["null", "string"]},
            },
        }
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            json_schema,
            self._primary_key,
            "date",
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
        )

        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=json_schema,
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
            source_defined_cursor=True,
            default_cursor_field=["date"],
            source_defined_primary_key=None,
            namespace=None,
            is_resumable=True,
            is_file_based=False,
        )

        airbyte_stream = stream.as_airbyte_stream()
        assert airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_namespace(self):
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            self._json_schema,
            self._primary_key,
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
            namespace="test",
        )
        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=self._json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            source_defined_cursor=None,
            default_cursor_field=None,
            source_defined_primary_key=None,
            namespace="test",
            is_resumable=False,
            is_file_based=False,
        )
        actual_airbyte_stream = stream.as_airbyte_stream()

        assert actual_airbyte_stream == expected_airbyte_stream

    def test_as_airbyte_stream_with_file_transfer_support(self):
        stream = DefaultStream(
            self._partition_generator,
            self._name,
            self._json_schema,
            self._primary_key,
            self._cursor_field,
            self._logger,
            FinalStateCursor(
                stream_name=self._name,
                stream_namespace=None,
                message_repository=self._message_repository,
            ),
            namespace="test",
            supports_file_transfer=True,
        )
        expected_airbyte_stream = AirbyteStream(
            name=self._name,
            json_schema=self._json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            source_defined_cursor=None,
            default_cursor_field=None,
            source_defined_primary_key=None,
            namespace="test",
            is_resumable=False,
            is_file_based=True,
        )
        actual_airbyte_stream = stream.as_airbyte_stream()

        assert actual_airbyte_stream == expected_airbyte_stream

    def test_given_no_partitions_when_get_availability_then_unavailable(self) -> None:
        self._partition_generator.generate.return_value = []

        availability = self._stream.check_availability()

        assert availability.is_available == False
        assert "no stream slices were found" in availability.reason

    def test_given_AirbyteTracedException_when_generating_partitions_when_get_availability_then_unavailable(
        self,
    ) -> None:
        error_message = "error while generating partitions"
        self._partition_generator.generate.side_effect = AirbyteTracedException(
            message=error_message
        )

        availability = self._stream.check_availability()

        assert availability.is_available == False
        assert error_message in availability.reason

    def test_given_unknown_error_when_generating_partitions_when_get_availability_then_raise(
        self,
    ) -> None:
        """
        I'm not sure why we handle AirbyteTracedException but not other exceptions but this is to keep feature compatibility with HttpAvailabilityStrategy
        """
        self._partition_generator.generate.side_effect = ValueError()
        with pytest.raises(ValueError):
            self._stream.check_availability()

    def test_given_no_records_when_get_availability_then_available(self) -> None:
        self._partition_generator.generate.return_value = [self._partition]
        self._partition.read.return_value = []

        availability = self._stream.check_availability()

        assert availability.is_available == True

    def test_given_records_when_get_availability_then_available(self) -> None:
        self._partition_generator.generate.return_value = [self._partition]
        self._partition.read.return_value = [Mock(spec=Record)]

        availability = self._stream.check_availability()

        assert availability.is_available == True

    def test_given_AirbyteTracedException_when_reading_records_when_get_availability_then_unavailable(
        self,
    ) -> None:
        self._partition_generator.generate.return_value = [self._partition]
        error_message = "error while reading records"
        self._partition.read.side_effect = AirbyteTracedException(message=error_message)

        availability = self._stream.check_availability()

        assert availability.is_available == False

    def test_given_unknown_error_when_reading_record_when_get_availability_then_raise(self) -> None:
        """
        I'm not sure why we handle AirbyteTracedException but not other exceptions but this is to keep feature compatibility with HttpAvailabilityStrategy
        """
        self._partition_generator.generate.side_effect = ValueError()
        self._partition.read.return_value = []
        with pytest.raises(ValueError):
            self._stream.check_availability()
