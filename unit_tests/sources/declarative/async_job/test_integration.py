# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


import logging
from queue import Queue
from typing import Any, Iterable, Iterator, List, Mapping, Optional, Set, Tuple
from unittest import TestCase, mock

from airbyte_protocol_dataclasses.models import (
    AirbyteCatalog,
    AirbyteMessage,
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
)

from airbyte_cdk.models import ConnectorSpecification
from airbyte_cdk.sources.concurrent_source.concurrent_source import ConcurrentSource
from airbyte_cdk.sources.declarative.async_job.job import AsyncJob
from airbyte_cdk.sources.declarative.async_job.job_orchestrator import AsyncJobOrchestrator
from airbyte_cdk.sources.declarative.async_job.job_tracker import JobTracker
from airbyte_cdk.sources.declarative.async_job.repository import AsyncJobRepository
from airbyte_cdk.sources.declarative.async_job.status import AsyncJobStatus
from airbyte_cdk.sources.declarative.extractors.dpath_extractor import DpathExtractor
from airbyte_cdk.sources.declarative.extractors.record_selector import RecordSelector
from airbyte_cdk.sources.declarative.partition_routers import SinglePartitionRouter
from airbyte_cdk.sources.declarative.partition_routers.async_job_partition_router import (
    AsyncJobPartitionRouter,
)
from airbyte_cdk.sources.declarative.retrievers.async_retriever import AsyncRetriever
from airbyte_cdk.sources.declarative.schema import InlineSchemaLoader
from airbyte_cdk.sources.declarative.stream_slicers import StreamSlicer
from airbyte_cdk.sources.declarative.stream_slicers.declarative_partition_generator import (
    DeclarativePartitionFactory,
    StreamSlicerPartitionGenerator,
)
from airbyte_cdk.sources.message import NoopMessageRepository
from airbyte_cdk.sources.source import Source
from airbyte_cdk.sources.streams.concurrent.abstract_stream import AbstractStream
from airbyte_cdk.sources.streams.concurrent.cursor import FinalStateCursor
from airbyte_cdk.sources.streams.concurrent.default_stream import DefaultStream
from airbyte_cdk.sources.streams.concurrent.partitions.types import QueueItem
from airbyte_cdk.sources.types import StreamSlice
from airbyte_cdk.sources.utils.slice_logger import DebugSliceLogger
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from airbyte_cdk.test.catalog_builder import CatalogBuilder, ConfiguredAirbyteStreamBuilder
from airbyte_cdk.test.entrypoint_wrapper import read

_A_STREAM_NAME = "a_stream_name"
_NO_LIMIT = 10000


class MockAsyncJobRepository(AsyncJobRepository):
    def start(self, stream_slice: StreamSlice) -> AsyncJob:
        return AsyncJob("a_job_id", stream_slice)

    def update_jobs_status(self, jobs: Set[AsyncJob]) -> None:
        for job in jobs:
            job.update_status(AsyncJobStatus.COMPLETED)

    def fetch_records(self, job: AsyncJob) -> Iterable[Mapping[str, Any]]:
        yield from [{"record_field": 10}]

    def abort(self, job: AsyncJob) -> None:
        pass

    def delete(self, job: AsyncJob) -> None:
        pass


class MockSource(Source):
    def __init__(self, stream_slicer: Optional[StreamSlicer] = None) -> None:
        self._stream_slicer = SinglePartitionRouter({}) if stream_slicer is None else stream_slicer
        queue: Queue[QueueItem] = Queue(maxsize=10_000)
        self._message_repository = NoopMessageRepository()
        self._config = {}

        self._concurrent_source = ConcurrentSource.create(
            num_workers=1,
            initial_number_of_partitions_to_generate=1,
            logger=logging.getLogger("airbyte"),
            slice_logger=DebugSliceLogger(),
            queue=queue,
            message_repository=self._message_repository,
        )

    def check(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Optional[Any]]:
        return True, None

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(connectionSpecification={})

    def streams(self, config: Mapping[str, Any]) -> List[AbstractStream]:
        # Build the partition router with the mock repository
        partition_router = AsyncJobPartitionRouter(
            stream_slicer=self._stream_slicer,
            job_orchestrator_factory=lambda stream_slices: AsyncJobOrchestrator(
                MockAsyncJobRepository(),
                stream_slices,
                JobTracker(_NO_LIMIT),
                self._message_repository,
            ),
            config={},
            parameters={},
        )

        # Create the extractor that extracts records from responses
        extractor = DpathExtractor(
            field_path=[],
            config={},
            parameters={},
        )

        # Create the record selector with the extractor
        record_selector = RecordSelector(
            extractor=extractor,
            config={},
            parameters={},
            schema_normalization=TypeTransformer(TransformConfig.NoTransform),
            name=_A_STREAM_NAME,
            record_filter=None,
            transformations=[],
        )

        # Build the retriever with the partition router
        retriever = AsyncRetriever(
            config={},
            parameters={},
            record_selector=record_selector,
            stream_slicer=partition_router,
        )

        # Create schema loader
        schema_loader = InlineSchemaLoader({}, {})

        # Create partition factory that will create partitions from stream slices
        partition_factory = DeclarativePartitionFactory(
            stream_name=_A_STREAM_NAME,
            schema_loader=schema_loader,
            retriever=retriever,
            message_repository=self._message_repository,
            max_records_limit=None,
        )

        # Create partition generator that wraps the partition router
        partition_generator = StreamSlicerPartitionGenerator(
            partition_factory=partition_factory,
            stream_slicer=partition_router,
            slice_limit=None,
            max_records_limit=None,
        )

        # Create cursor (using FinalStateCursor for full refresh)
        cursor = FinalStateCursor(
            stream_name=_A_STREAM_NAME,
            stream_namespace=None,
            message_repository=self._message_repository,
        )

        # Directly instantiate DefaultStream with all components
        stream = DefaultStream(
            partition_generator=partition_generator,
            name=_A_STREAM_NAME,
            json_schema={},
            primary_key=["id"],
            cursor_field=None,
            logger=logging.getLogger("airbyte"),
            cursor=cursor,
        )

        return [stream]

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[List[AirbyteStateMessage]] = None,
    ) -> Iterator[AirbyteMessage]:
        stream_name_to_instance = {s.name: s for s in self.streams(config=self._config)}
        selected_concurrent_streams = [
            stream_name_to_instance[configured_stream.stream.name]
            for configured_stream in catalog.streams
            if configured_stream.stream.name in stream_name_to_instance
        ]

        # selected_concurrent_streams = self._select_streams(
        #     streams=,  # type: ignore  # We are migrating away from the DeclarativeStream implementation and streams() only returns the concurrent-compatible AbstractStream. To preserve compatibility, we retain the existing method interface
        #     configured_catalog=catalog,
        # )

        # It would appear that passing in an empty set of streams causes an infinite loop in ConcurrentReadProcessor.
        # This is also evident in concurrent_source_adapter.py so I'll leave this out of scope to fix for now
        if len(selected_concurrent_streams) > 0:
            yield from self._concurrent_source.read(selected_concurrent_streams)

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        return AirbyteCatalog(
            streams=[stream.as_airbyte_stream() for stream in self.streams(config=self._config)]
        )


class JobDeclarativeStreamTest(TestCase):
    _CONFIG: Mapping[str, Any] = {}

    def setUp(self) -> None:
        self._stream_slicer = mock.Mock(wraps=SinglePartitionRouter({}))
        self._source = MockSource(self._stream_slicer)
        self._source.streams({})

    def test_when_read_then_return_records_from_repository(self) -> None:
        output = read(
            self._source,
            self._CONFIG,
            CatalogBuilder()
            .with_stream(ConfiguredAirbyteStreamBuilder().with_name(_A_STREAM_NAME))
            .build(),
        )

        assert len(output.records) == 1

    def test_when_read_then_call_stream_slices_only_once(self) -> None:
        """
        As generating stream slices is very expensive, we want to ensure that during a read, it is only called once.
        """
        output = read(
            self._source,
            self._CONFIG,
            CatalogBuilder()
            .with_stream(ConfiguredAirbyteStreamBuilder().with_name(_A_STREAM_NAME))
            .build(),
        )

        assert not output.errors
        assert self._stream_slicer.stream_slices.call_count == 1
