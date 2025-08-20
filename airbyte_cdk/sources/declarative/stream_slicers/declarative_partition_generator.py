# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import Any, Iterable, Mapping, Optional

from airbyte_cdk.sources.declarative.retrievers import Retriever
from airbyte_cdk.sources.declarative.schema import SchemaLoader
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.partition_generator import PartitionGenerator
from airbyte_cdk.sources.streams.concurrent.partitions.stream_slicer import StreamSlicer
from airbyte_cdk.sources.types import Record, StreamSlice
from airbyte_cdk.utils.slice_hasher import SliceHasher


class SchemaLoaderCachingDecorator(SchemaLoader):
    def __init__(self, schema_loader: SchemaLoader):
        self._decorated = schema_loader
        self._loaded_schema: Optional[Mapping[str, Any]] = None

    def get_json_schema(self) -> Mapping[str, Any]:
        if self._loaded_schema is None:
            self._loaded_schema = self._decorated.get_json_schema()

        return self._loaded_schema  # type: ignore  # at that point, we assume the schema will be populated


class SchemaLoaderCachingDecorator(SchemaLoader):
    def __init__(self, schema_loader: SchemaLoader):
        self._decorated = schema_loader
        self._loaded_schema: Optional[Mapping[str, Any]] = None

    def get_json_schema(self) -> Mapping[str, Any]:
        if self._loaded_schema is None:
            self._loaded_schema = self._decorated.get_json_schema()

        return self._loaded_schema  # type: ignore  # at that point, we assume the schema will be populated


class DeclarativePartitionFactory:
    def __init__(
        self,
        stream_name: str,
        schema_loader: SchemaLoader,
        retriever: Retriever,
        message_repository: MessageRepository,
    ) -> None:
        """
        The DeclarativePartitionFactory takes a retriever_factory and not a retriever directly. The reason is that our components are not
        thread safe and classes like `DefaultPaginator` may not work because multiple threads can access and modify a shared field across each other.
        In order to avoid these problems, we will create one retriever per thread which should make the processing thread-safe.
        """
        self._stream_name = stream_name
        self._schema_loader = SchemaLoaderCachingDecorator(schema_loader)
        self._retriever = retriever
        self._message_repository = message_repository

    def create(self, stream_slice: StreamSlice) -> Partition:
        return DeclarativePartition(
            stream_name=self._stream_name,
            schema_loader=self._schema_loader,
            retriever=self._retriever,
            message_repository=self._message_repository,
            stream_slice=stream_slice,
        )


class DeclarativePartition(Partition):
    def __init__(
        self,
        stream_name: str,
        schema_loader: SchemaLoader,
        retriever: Retriever,
        message_repository: MessageRepository,
        stream_slice: StreamSlice,
    ):
        self._stream_name = stream_name
        self._schema_loader = schema_loader
        self._retriever = retriever
        self._message_repository = message_repository
        self._stream_slice = stream_slice
        self._hash = SliceHasher.hash(self._stream_name, self._stream_slice)

    def read(self) -> Iterable[Record]:
        for stream_data in self._retriever.read_records(
            self._schema_loader.get_json_schema(), self._stream_slice
        ):
            if isinstance(stream_data, Mapping):
                record = (
                    stream_data
                    if isinstance(stream_data, Record)
                    else Record(
                        data=stream_data,
                        stream_name=self.stream_name(),
                        associated_slice=self._stream_slice,
                    )
                )
                yield record
            else:
                self._message_repository.emit_message(stream_data)

    def to_slice(self) -> Optional[Mapping[str, Any]]:
        return self._stream_slice

    def stream_name(self) -> str:
        return self._stream_name

    def __hash__(self) -> int:
        return self._hash


class StreamSlicerPartitionGenerator(PartitionGenerator):
    def __init__(
        self, partition_factory: DeclarativePartitionFactory, stream_slicer: StreamSlicer
    ) -> None:
        self._partition_factory = partition_factory
        self._stream_slicer = stream_slicer

    def generate(self) -> Iterable[Partition]:
        for stream_slice in self._stream_slicer.stream_slices():
            yield self._partition_factory.create(stream_slice)
