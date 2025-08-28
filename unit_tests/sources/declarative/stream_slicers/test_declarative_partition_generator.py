# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import List
from unittest import TestCase
from unittest.mock import Mock

from airbyte_cdk.models import AirbyteLogMessage, AirbyteMessage, Level, Type
from airbyte_cdk.sources.declarative.retrievers import Retriever
from airbyte_cdk.sources.declarative.schema import InlineSchemaLoader
from airbyte_cdk.sources.declarative.stream_slicers.declarative_partition_generator import (
    DeclarativePartitionFactory,
)
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.types import Record, StreamSlice

_STREAM_NAME = "a_stream_name"
_SCHEMA_LOADER = InlineSchemaLoader({"type": "object", "properties": {}}, {})
_A_STREAM_SLICE = StreamSlice(
    partition={"partition_key": "partition_value"}, cursor_slice={"cursor_key": "cursor_value"}
)
_ANOTHER_STREAM_SLICE = StreamSlice(
    partition={"partition_key": "another_partition_value"},
    cursor_slice={"cursor_key": "cursor_value"},
)
_AIRBYTE_LOG_MESSAGE = AirbyteMessage(
    type=Type.LOG, log=AirbyteLogMessage(level=Level.DEBUG, message="a log message")
)
_A_RECORD = {"record_field": "record_value"}


class StreamSlicerPartitionGeneratorTest(TestCase):
    def test_given_multiple_slices_partition_generator_uses_the_same_retriever(self) -> None:
        retriever = self._mock_retriever([])
        message_repository = Mock(spec=MessageRepository)
        partition_factory = DeclarativePartitionFactory(
            _STREAM_NAME,
            _SCHEMA_LOADER,
            retriever,
            message_repository,
        )

        list(partition_factory.create(_A_STREAM_SLICE).read())
        list(partition_factory.create(_ANOTHER_STREAM_SLICE).read())

        assert retriever.read_records.call_count == 2

    def test_given_a_mapping_when_read_then_yield_record(self) -> None:
        retriever = self._mock_retriever([_A_RECORD])
        message_repository = Mock(spec=MessageRepository)
        partition_factory = DeclarativePartitionFactory(
            _STREAM_NAME,
            _SCHEMA_LOADER,
            retriever,
            message_repository,
        )

        partition = partition_factory.create(_A_STREAM_SLICE)

        records = list(partition.read())

        assert len(records) == 1
        assert records[0].associated_slice == _A_STREAM_SLICE
        assert records[0].data == _A_RECORD

    def test_given_not_a_record_when_read_then_send_to_message_repository(self) -> None:
        retriever = self._mock_retriever([_AIRBYTE_LOG_MESSAGE])
        message_repository = Mock(spec=MessageRepository)
        partition_factory = DeclarativePartitionFactory(
            _STREAM_NAME,
            _SCHEMA_LOADER,
            retriever,
            message_repository,
        )

        list(partition_factory.create(_A_STREAM_SLICE).read())

        message_repository.emit_message.assert_called_once_with(_AIRBYTE_LOG_MESSAGE)

    def test_max_records_reached_stops_reading(self) -> None:
        expected_records = [
            Record(data={"id": 1, "name": "Max"}, stream_name="stream_name"),
            Record(data={"id": 1, "name": "Oscar"}, stream_name="stream_name"),
            Record(data={"id": 1, "name": "Charles"}, stream_name="stream_name"),
            Record(data={"id": 1, "name": "Alex"}, stream_name="stream_name"),
            Record(data={"id": 1, "name": "Yuki"}, stream_name="stream_name"),
        ]

        mock_records = expected_records + [
            Record(data={"id": 1, "name": "Lewis"}, stream_name="stream_name"),
            Record(data={"id": 1, "name": "Lando"}, stream_name="stream_name"),
        ]

        retriever = self._mock_retriever(mock_records)
        message_repository = Mock(spec=MessageRepository)
        partition_factory = DeclarativePartitionFactory(
            _STREAM_NAME,
            _SCHEMA_LOADER,
            retriever,
            message_repository,
            max_records_limit=5,
        )

        partition = partition_factory.create(_A_STREAM_SLICE)

        actual_records = list(partition.read())

        assert len(actual_records) == 5
        assert actual_records == expected_records

    def test_max_records_reached_on_previous_partition(self) -> None:
        expected_records = [
            Record(data={"id": 1, "name": "Max"}, stream_name="stream_name"),
            Record(data={"id": 1, "name": "Oscar"}, stream_name="stream_name"),
            Record(data={"id": 1, "name": "Charles"}, stream_name="stream_name"),
        ]

        mock_records = expected_records + [
            Record(data={"id": 1, "name": "Alex"}, stream_name="stream_name"),
            Record(data={"id": 1, "name": "Yuki"}, stream_name="stream_name"),
        ]

        retriever = self._mock_retriever(mock_records)
        message_repository = Mock(spec=MessageRepository)
        partition_factory = DeclarativePartitionFactory(
            _STREAM_NAME,
            _SCHEMA_LOADER,
            retriever,
            message_repository,
            max_records_limit=3,
        )

        partition = partition_factory.create(_A_STREAM_SLICE)

        first_partition_records = list(partition.read())

        assert len(first_partition_records) == 3
        assert first_partition_records == expected_records

        second_partition_records = list(partition.read())
        assert len(second_partition_records) == 0

        # The DeclarativePartition exits out of the read before attempting to read_records() if
        # the max_records_limit has already been reached. So we only expect to see read_records()
        # called for the first partition read and not the second
        retriever.read_records.assert_called_once()

    @staticmethod
    def _mock_retriever(read_return_value: List[StreamData]) -> Mock:
        retriever = Mock(spec=Retriever)
        retriever.read_records.return_value = iter(read_return_value)
        return retriever
