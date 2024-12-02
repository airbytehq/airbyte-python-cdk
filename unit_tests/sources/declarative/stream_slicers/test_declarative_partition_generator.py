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
    def setUp(self) -> None:
        self._retriever_factory = Mock()
        self._message_repository = Mock(spec=MessageRepository)
        self._partition_factory = DeclarativePartitionFactory(
            _STREAM_NAME,
            _JSON_SCHEMA,
            self._retriever_factory,
            self._message_repository,
        )

    def test_given_multiple_slices_when_read_then_read_from_different_retrievers(self) -> None:
        first_retriever = self._mock_retriever([])
        second_retriever = self._mock_retriever([])
        self._retriever_factory.side_effect = [first_retriever, second_retriever]

        list(self._partition_factory.create(_A_STREAM_SLICE).read())
        list(self._partition_factory.create(_ANOTHER_STREAM_SLICE).read())

        first_retriever.read_records.assert_called_once()
        second_retriever.read_records.assert_called_once()

    def test_given_a_mapping_when_read_then_yield_record(self) -> None:
        retriever = self._mock_retriever([_A_RECORD])
        self._retriever_factory.return_value = retriever
        partition = self._partition_factory.create(_A_STREAM_SLICE)

        records = list(partition.read())

        assert len(records) == 1
        assert records[0].associated_slice == _A_STREAM_SLICE
        assert records[0].data == _A_RECORD

    def test_given_not_a_record_when_read_then_send_to_message_repository(self) -> None:
        retriever = self._mock_retriever([_AIRBYTE_LOG_MESSAGE])
        self._retriever_factory.return_value = retriever

        list(self._partition_factory.create(_A_STREAM_SLICE).read())

        self._message_repository.emit_message.assert_called_once_with(_AIRBYTE_LOG_MESSAGE)

    def _mock_retriever(self, read_return_value: List[StreamData]) -> Mock:
        retriever = Mock(spec=Retriever)
        retriever.read_records.return_value = iter(read_return_value)
        return retriever
