# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import unittest
from queue import Queue
from typing import Callable, Iterable, List
from unittest.mock import Mock

import pytest

from airbyte_cdk.sources.concurrent_source.stream_thread_exception import StreamThreadException
from airbyte_cdk.sources.message.repository import InMemoryMessageRepository
from airbyte_cdk.sources.streams.concurrent.cursor import FinalStateCursor
from airbyte_cdk.sources.streams.concurrent.partition_reader import PartitionReader
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.types import (
    PartitionCompleteSentinel,
    QueueItem,
)
from airbyte_cdk.sources.types import Record

_RECORDS = [
    Record({"id": 1, "name": "Jack"}, "stream"),
    Record({"id": 2, "name": "John"}, "stream"),
]


class PartitionReaderTest(unittest.TestCase):
    def setUp(self) -> None:
        self._queue: Queue[QueueItem] = Queue()
        self._partition_reader = PartitionReader(self._queue, None)

    def test_given_no_records_when_process_partition_then_only_emit_sentinel(self):
        cursor = FinalStateCursor(
            stream_name="test",
            stream_namespace=None,
            message_repository=InMemoryMessageRepository(),
        )
        self._partition_reader.process_partition(self._a_partition([]), cursor)

        while queue_item := self._queue.get():
            if not isinstance(queue_item, PartitionCompleteSentinel):
                pytest.fail("Only one PartitionCompleteSentinel is expected")
            break

    def test_given_read_partition_successful_when_process_partition_then_queue_records_and_sentinel(
        self,
    ):
        partition = self._a_partition(_RECORDS)
        cursor = Mock()
        self._partition_reader.process_partition(partition, cursor)

        queue_content = self._consume_queue()

        assert queue_content == _RECORDS + [PartitionCompleteSentinel(partition)]

        cursor.observe.assert_called()
        cursor.close_partition.assert_called_once()

    def test_given_exception_from_read_when_process_partition_then_queue_records_and_exception_and_sentinel(
        self,
    ):
        partition = Mock()
        cursor = Mock()
        exception = ValueError()
        partition.read.side_effect = self._read_with_exception(_RECORDS, exception)
        self._partition_reader.process_partition(partition, cursor)

        queue_content = self._consume_queue()

        assert queue_content == _RECORDS + [
            StreamThreadException(exception, partition.stream_name()),
            PartitionCompleteSentinel(partition),
        ]

    def test_given_exception_from_close_slice_when_process_partition_then_queue_records_and_exception_and_sentinel(
        self,
    ):
        partition = self._a_partition(_RECORDS)
        cursor = Mock()
        exception = ValueError()
        cursor.close_partition.side_effect = self._close_partition_with_exception(exception)
        self._partition_reader.process_partition(partition, cursor)

        queue_content = self._consume_queue()

        # 4 total messages in queue. 2 records, 1 thread exception, 1 partition sentinel value
        assert len(queue_content) == 4
        assert queue_content[:2] == _RECORDS
        assert isinstance(queue_content[2], StreamThreadException)
        assert queue_content[3] == PartitionCompleteSentinel(partition)

    def _a_partition(self, records: List[Record]) -> Partition:
        partition = Mock(spec=Partition)
        partition.read.return_value = iter(records)
        return partition

    @staticmethod
    def _read_with_exception(
        records: List[Record], exception: Exception
    ) -> Callable[[], Iterable[Record]]:
        def mocked_function() -> Iterable[Record]:
            yield from records
            raise exception

        return mocked_function

    @staticmethod
    def _close_partition_with_exception(exception: Exception) -> Callable[[Partition], None]:
        def mocked_function(partition: Partition) -> None:
            raise exception

        return mocked_function

    def _consume_queue(self):
        queue_content = []
        while queue_item := self._queue.get():
            queue_content.append(queue_item)
            if isinstance(queue_item, PartitionCompleteSentinel):
                break
        return queue_content
