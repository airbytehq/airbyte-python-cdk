# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import List
from unittest import TestCase
from unittest.mock import Mock

from airbyte_cdk.models import AirbyteLogMessage, AirbyteMessage, Level, Type
from airbyte_cdk.sources.declarative.retrievers import Retriever
from airbyte_cdk.sources.declarative.stream_slicers.declarative_partition_generator import (
    DeclarativePartitionFactory,
)
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.types import StreamSlice

_STREAM_NAME = "a_stream_name"
_JSON_SCHEMA = {"type": "object", "properties": {}}
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
            _JSON_SCHEMA,
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
            _JSON_SCHEMA,
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
            _JSON_SCHEMA,
            retriever,
            message_repository,
        )

        list(partition_factory.create(_A_STREAM_SLICE).read())

        message_repository.emit_message.assert_called_once_with(_AIRBYTE_LOG_MESSAGE)

    @staticmethod
    def _mock_retriever(read_return_value: List[StreamData]) -> Mock:
        retriever = Mock(spec=Retriever)
        retriever.read_records.return_value = iter(read_return_value)
        return retriever
