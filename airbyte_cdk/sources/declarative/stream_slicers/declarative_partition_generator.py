# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import Iterable, Optional, Mapping, Any

from airbyte_cdk.sources.declarative.retrievers import Retriever
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.partition_generator import PartitionGenerator
from airbyte_cdk.sources.streams.concurrent.partitions.record import Record
from airbyte_cdk.sources.streams.concurrent.partitions.stream_slicer import StreamSlicer
from airbyte_cdk.sources.types import StreamSlice
from airbyte_cdk.utils.slice_hasher import SliceHasher


class DeclarativePartitionFactory:
    def __init__(self, stream_name: str, json_schema: Mapping[str, Any], retriever: Retriever, message_repository: MessageRepository) -> None:
        self._stream_name = stream_name
        self._json_schema = json_schema
        self._retriever = retriever  # FIXME: it should be a retriever_factory here to ensure that paginators and other classes don't share interal/class state
        self._message_repository = message_repository

    def create(self, stream_slice: StreamSlice) -> Partition:
        return DeclarativePartition(
            self._stream_name,
            self._json_schema,
            self._retriever,
            self._message_repository,
            stream_slice,
        )


class DeclarativePartition(Partition):
    def __init__(self, stream_name: str, json_schema: Mapping[str, Any], retriever: Retriever, message_repository: MessageRepository, stream_slice: StreamSlice):
        self._stream_name = stream_name
        self._json_schema = json_schema
        self._retriever = retriever
        self._message_repository = message_repository
        self._stream_slice = stream_slice
        self._hash = SliceHasher.hash(self._stream_name, self._stream_slice)

    def read(self) -> Iterable[Record]:
        for stream_data in self._retriever.read_records(self._json_schema, self._stream_slice):
            if isinstance(stream_data, Mapping):
                # TODO validate if this is necessary: self._stream.transformer.transform(data_to_return, self._stream.get_json_schema())
                yield Record(stream_data, self)
            else:
                self._message_repository.emit_message(stream_data)

    def to_slice(self) -> Optional[Mapping[str, Any]]:
        return self._stream_slice

    def stream_name(self) -> str:
        return self._stream_name

    def __hash__(self) -> int:
        return self._hash


class StreamSlicerPartitionGenerator(PartitionGenerator):
    def __init__(self, partition_factory: DeclarativePartitionFactory, stream_slicer: StreamSlicer) -> None:
        self._partition_factory = partition_factory
        self._stream_slicer = stream_slicer

    def generate(self) -> Iterable[Partition]:
        for stream_slice in self._stream_slicer.stream_slices():
            yield self._partition_factory.create(stream_slice)
