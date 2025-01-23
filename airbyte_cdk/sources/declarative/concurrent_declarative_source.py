#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Generic, Iterator, List, Mapping, Optional, Tuple

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteMessage,
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
)
from airbyte_cdk.sources.concurrent_source.concurrent_source import ConcurrentSource
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.declarative.concurrency_level import ConcurrencyLevel
from airbyte_cdk.sources.declarative.declarative_stream import DeclarativeStream
from airbyte_cdk.sources.declarative.extractors import RecordSelector
from airbyte_cdk.sources.declarative.extractors.record_filter import (
    ClientSideIncrementalRecordFilterDecorator,
)
from airbyte_cdk.sources.declarative.incremental.datetime_based_cursor import DatetimeBasedCursor
from airbyte_cdk.sources.declarative.incremental.per_partition_with_global import (
    PerPartitionWithGlobalCursor,
)
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    ConcurrencyLevel as ConcurrencyLevelModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    DatetimeBasedCursor as DatetimeBasedCursorModel,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)
from airbyte_cdk.sources.declarative.partition_routers import AsyncJobPartitionRouter
from airbyte_cdk.sources.declarative.requesters import HttpRequester
from airbyte_cdk.sources.declarative.retrievers import AsyncRetriever, Retriever, SimpleRetriever
from airbyte_cdk.sources.declarative.stream_slicers.declarative_partition_generator import (
    DeclarativePartitionFactory,
    StreamSlicerPartitionGenerator,
)
from airbyte_cdk.sources.declarative.transformations.add_fields import AddFields
from airbyte_cdk.sources.declarative.types import ConnectionDefinition
from airbyte_cdk.sources.source import TState
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.concurrent.abstract_stream import AbstractStream
from airbyte_cdk.sources.streams.concurrent.availability_strategy import (
    AlwaysAvailableAvailabilityStrategy,
)
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor, FinalStateCursor
from airbyte_cdk.sources.streams.concurrent.default_stream import DefaultStream
from airbyte_cdk.sources.streams.concurrent.helpers import get_primary_key_from_stream


class ConcurrentDeclarativeSource(ManifestDeclarativeSource, Generic[TState]):
    # By default, we defer to a value of 2. A value lower than than could cause a PartitionEnqueuer to be stuck in a state of deadlock
    # because it has hit the limit of futures but not partition reader is consuming them.
    _LOWEST_SAFE_CONCURRENCY_LEVEL = 2

    def __init__(
        self,
        catalog: Optional[ConfiguredAirbyteCatalog],
        config: Optional[Mapping[str, Any]],
        state: TState,
        source_config: ConnectionDefinition,
        debug: bool = False,
        emit_connector_builder_messages: bool = False,
        component_factory: Optional[ModelToComponentFactory] = None,
        **kwargs: Any,
    ) -> None:
        # todo: We could remove state from initialization. Now that streams are grouped during the read(), a source
        #  no longer needs to store the original incoming state. But maybe there's an edge case?
        self._connector_state_manager = ConnectorStateManager(state=state)  # type: ignore  # state is always in the form of List[AirbyteStateMessage]. The ConnectorStateManager should use generics, but this can be done later

        # To reduce the complexity of the concurrent framework, we are not enabling RFR with synthetic
        # cursors. We do this by no longer automatically instantiating RFR cursors when converting
        # the declarative models into runtime components. Concurrent sources will continue to checkpoint
        # incremental streams running in full refresh.
        component_factory = component_factory or ModelToComponentFactory(
            emit_connector_builder_messages=emit_connector_builder_messages,
            disable_resumable_full_refresh=True,
            connector_state_manager=self._connector_state_manager,
        )

        super().__init__(
            source_config=source_config,
            config=config,
            debug=debug,
            emit_connector_builder_messages=emit_connector_builder_messages,
            component_factory=component_factory,
        )

        concurrency_level_from_manifest = self._source_config.get("concurrency_level")
        if concurrency_level_from_manifest:
            concurrency_level_component = self._constructor.create_component(
                model_type=ConcurrencyLevelModel,
                component_definition=concurrency_level_from_manifest,
                config=config or {},
            )
            if not isinstance(concurrency_level_component, ConcurrencyLevel):
                raise ValueError(
                    f"Expected to generate a ConcurrencyLevel component, but received {concurrency_level_component.__class__}"
                )

            concurrency_level = concurrency_level_component.get_concurrency_level()
            initial_number_of_partitions_to_generate = max(
                concurrency_level // 2, 1
            )  # Partition_generation iterates using range based on this value. If this is floored to zero we end up in a dead lock during start up
        else:
            concurrency_level = self._LOWEST_SAFE_CONCURRENCY_LEVEL
            initial_number_of_partitions_to_generate = self._LOWEST_SAFE_CONCURRENCY_LEVEL // 2

        self._concurrent_source = ConcurrentSource.create(
            num_workers=concurrency_level,
            initial_number_of_partitions_to_generate=initial_number_of_partitions_to_generate,
            logger=self.logger,
            slice_logger=self._slice_logger,
            message_repository=self.message_repository,
        )

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[List[AirbyteStateMessage]] = None,
    ) -> Iterator[AirbyteMessage]:
        concurrent_streams, _ = self._group_streams(config=config)

        # ConcurrentReadProcessor pops streams that are finished being read so before syncing, the names of
        # the concurrent streams must be saved so that they can be removed from the catalog before starting
        # synchronous streams
        if len(concurrent_streams) > 0:
            concurrent_stream_names = set(
                [concurrent_stream.name for concurrent_stream in concurrent_streams]
            )

            selected_concurrent_streams = self._select_streams(
                streams=concurrent_streams, configured_catalog=catalog
            )
            # It would appear that passing in an empty set of streams causes an infinite loop in ConcurrentReadProcessor.
            # This is also evident in concurrent_source_adapter.py so I'll leave this out of scope to fix for now
            if selected_concurrent_streams:
                yield from self._concurrent_source.read(selected_concurrent_streams)

            # Sync all streams that are not concurrent compatible. We filter out concurrent streams because the
            # existing AbstractSource.read() implementation iterates over the catalog when syncing streams. Many
            # of which were already synced using the Concurrent CDK
            filtered_catalog = self._remove_concurrent_streams_from_catalog(
                catalog=catalog, concurrent_stream_names=concurrent_stream_names
            )
        else:
            filtered_catalog = catalog

        yield from super().read(logger, config, filtered_catalog, state)

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        concurrent_streams, synchronous_streams = self._group_streams(config=config)
        return AirbyteCatalog(
            streams=[
                stream.as_airbyte_stream() for stream in concurrent_streams + synchronous_streams
            ]
        )

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        The `streams` method is used as part of the AbstractSource in the following cases:
        * ConcurrentDeclarativeSource.check -> ManifestDeclarativeSource.check -> AbstractSource.check -> DeclarativeSource.check_connection -> CheckStream.check_connection -> streams
        * ConcurrentDeclarativeSource.read -> AbstractSource.read -> streams (note that we filter for a specific catalog which excludes concurrent streams so not all streams actually read from all the streams returned by `streams`)
        Note that `super.streams(config)` is also called when splitting the streams between concurrent or not in `_group_streams`.

        In both case, we will assume that calling the DeclarativeStream is perfectly fine as the result for these is the same regardless of if it is a DeclarativeStream or a DefaultStream (concurrent). This should simply be removed once we have moved away from the mentioned code paths above.
        """
        return super().streams(config)

    def _group_streams(
        self, config: Mapping[str, Any]
    ) -> Tuple[List[AbstractStream], List[Stream]]:
        concurrent_streams: List[AbstractStream] = []
        synchronous_streams: List[Stream] = []

        # Combine streams and dynamic_streams. Note: both cannot be empty at the same time,
        # and this is validated during the initialization of the source.
        streams = self._stream_configs(self._source_config) + self._dynamic_stream_configs(
            self._source_config, config
        )

        name_to_stream_mapping = {stream["name"]: stream for stream in streams}

        for declarative_stream in self.streams(config=config):
            # Some low-code sources use a combination of DeclarativeStream and regular Python streams. We can't inspect
            # these legacy Python streams the way we do low-code streams to determine if they are concurrent compatible,
            # so we need to treat them as synchronous
            if isinstance(declarative_stream, DeclarativeStream) and (
                name_to_stream_mapping[declarative_stream.name]["retriever"]["type"]
                == "SimpleRetriever"
                or name_to_stream_mapping[declarative_stream.name]["retriever"]["type"]
                == "AsyncRetriever"
            ):
                incremental_sync_component_definition = name_to_stream_mapping[
                    declarative_stream.name
                ].get("incremental_sync")

                partition_router_component_definition = (
                    name_to_stream_mapping[declarative_stream.name]
                    .get("retriever", {})
                    .get("partition_router")
                )
                is_without_partition_router_or_cursor = not bool(
                    incremental_sync_component_definition
                ) and not bool(partition_router_component_definition)

                is_substream_without_incremental = (
                    partition_router_component_definition
                    and not incremental_sync_component_definition
                )

                if self._is_datetime_incremental_without_partition_routing(
                    declarative_stream, incremental_sync_component_definition
                ):
                    stream_state = self._connector_state_manager.get_stream_state(
                        stream_name=declarative_stream.name, namespace=declarative_stream.namespace
                    )

                    retriever = self._get_retriever(declarative_stream, stream_state)

                    if isinstance(declarative_stream.retriever, AsyncRetriever) and isinstance(
                        declarative_stream.retriever.stream_slicer, AsyncJobPartitionRouter
                    ):
                        cursor = declarative_stream.retriever.stream_slicer.stream_slicer

                        if not isinstance(cursor, ConcurrentCursor):
                            # This should never happen since we instantiate ConcurrentCursor in
                            # model_to_component_factory.py
                            raise ValueError(
                                f"Expected AsyncJobPartitionRouter stream_slicer to be of type ConcurrentCursor, but received{cursor.__class__}"
                            )

                        partition_generator = StreamSlicerPartitionGenerator(
                            partition_factory=DeclarativePartitionFactory(
                                declarative_stream.name,
                                declarative_stream.get_json_schema(),
                                retriever,
                                self.message_repository,
                            ),
                            stream_slicer=declarative_stream.retriever.stream_slicer,
                        )
                    else:
                        cursor = (
                            self._constructor.create_concurrent_cursor_from_datetime_based_cursor(
                                model_type=DatetimeBasedCursorModel,
                                component_definition=incremental_sync_component_definition,  # type: ignore  # Not None because of the if condition above
                                stream_name=declarative_stream.name,
                                stream_namespace=declarative_stream.namespace,
                                config=config or {},
                            )
                        )
                        partition_generator = StreamSlicerPartitionGenerator(
                            partition_factory=DeclarativePartitionFactory(
                                declarative_stream.name,
                                declarative_stream.get_json_schema(),
                                retriever,
                                self.message_repository,
                            ),
                            stream_slicer=cursor,
                        )

                    concurrent_streams.append(
                        DefaultStream(
                            partition_generator=partition_generator,
                            name=declarative_stream.name,
                            json_schema=declarative_stream.get_json_schema(),
                            availability_strategy=AlwaysAvailableAvailabilityStrategy(),
                            primary_key=get_primary_key_from_stream(declarative_stream.primary_key),
                            cursor_field=cursor.cursor_field.cursor_field_key
                            if hasattr(cursor, "cursor_field")
                            and hasattr(
                                cursor.cursor_field, "cursor_field_key"
                            )  # FIXME this will need to be updated once we do the per partition
                            else None,
                            logger=self.logger,
                            cursor=cursor,
                        )
                    )
                elif (
                    is_substream_without_incremental or is_without_partition_router_or_cursor
                ) and hasattr(declarative_stream.retriever, "stream_slicer"):
                    partition_generator = StreamSlicerPartitionGenerator(
                        DeclarativePartitionFactory(
                            declarative_stream.name,
                            declarative_stream.get_json_schema(),
                            declarative_stream.retriever,
                            self.message_repository,
                        ),
                        declarative_stream.retriever.stream_slicer,
                    )

                    final_state_cursor = FinalStateCursor(
                        stream_name=declarative_stream.name,
                        stream_namespace=declarative_stream.namespace,
                        message_repository=self.message_repository,
                    )

                    concurrent_streams.append(
                        DefaultStream(
                            partition_generator=partition_generator,
                            name=declarative_stream.name,
                            json_schema=declarative_stream.get_json_schema(),
                            availability_strategy=AlwaysAvailableAvailabilityStrategy(),
                            primary_key=get_primary_key_from_stream(declarative_stream.primary_key),
                            cursor_field=None,
                            logger=self.logger,
                            cursor=final_state_cursor,
                        )
                    )
                elif (
                    incremental_sync_component_definition
                    and incremental_sync_component_definition.get("type", "")
                    == DatetimeBasedCursorModel.__name__
                    and self._stream_supports_concurrent_partition_processing(
                        declarative_stream=declarative_stream
                    )
                    and hasattr(declarative_stream.retriever, "stream_slicer")
                    and isinstance(
                        declarative_stream.retriever.stream_slicer, PerPartitionWithGlobalCursor
                    )
                ):
                    stream_state = self._connector_state_manager.get_stream_state(
                        stream_name=declarative_stream.name, namespace=declarative_stream.namespace
                    )
                    partition_router = declarative_stream.retriever.stream_slicer._partition_router

                    perpartition_cursor = (
                        self._constructor.create_concurrent_cursor_from_perpartition_cursor(
                            state_manager=self._connector_state_manager,
                            model_type=DatetimeBasedCursorModel,
                            component_definition=incremental_sync_component_definition,
                            stream_name=declarative_stream.name,
                            stream_namespace=declarative_stream.namespace,
                            config=config or {},
                            stream_state=stream_state,
                            partition_router=partition_router,
                        )
                    )

                    retriever = self._get_retriever(declarative_stream, stream_state)

                    partition_generator = StreamSlicerPartitionGenerator(
                        DeclarativePartitionFactory(
                            declarative_stream.name,
                            declarative_stream.get_json_schema(),
                            retriever,
                            self.message_repository,
                        ),
                        perpartition_cursor,
                    )

                    concurrent_streams.append(
                        DefaultStream(
                            partition_generator=partition_generator,
                            name=declarative_stream.name,
                            json_schema=declarative_stream.get_json_schema(),
                            availability_strategy=AlwaysAvailableAvailabilityStrategy(),
                            primary_key=get_primary_key_from_stream(declarative_stream.primary_key),
                            cursor_field=perpartition_cursor.cursor_field.cursor_field_key,
                            logger=self.logger,
                            cursor=perpartition_cursor,
                        )
                    )
                else:
                    synchronous_streams.append(declarative_stream)
            else:
                synchronous_streams.append(declarative_stream)

        return concurrent_streams, synchronous_streams

    def _is_datetime_incremental_without_partition_routing(
        self,
        declarative_stream: DeclarativeStream,
        incremental_sync_component_definition: Mapping[str, Any] | None,
    ) -> bool:
        return (
            incremental_sync_component_definition is not None
            and bool(incremental_sync_component_definition)
            and incremental_sync_component_definition.get("type", "")
            == DatetimeBasedCursorModel.__name__
            and self._stream_supports_concurrent_partition_processing(
                declarative_stream=declarative_stream
            )
            and hasattr(declarative_stream.retriever, "stream_slicer")
            and (
                isinstance(declarative_stream.retriever.stream_slicer, DatetimeBasedCursor)
                or isinstance(declarative_stream.retriever.stream_slicer, AsyncJobPartitionRouter)
            )
        )

    def _stream_supports_concurrent_partition_processing(
        self, declarative_stream: DeclarativeStream
    ) -> bool:
        """
        Many connectors make use of stream_state during interpolation on a per-partition basis under the assumption that
        state is updated sequentially. Because the concurrent CDK engine processes different partitions in parallel,
        stream_state is no longer a thread-safe interpolation context. It would be a race condition because a cursor's
        stream_state can be updated in any order depending on which stream partition's finish first.

        We should start to move away from depending on the value of stream_state for low-code components that operate
        per-partition, but we need to gate this otherwise some connectors will be blocked from publishing. See the
        cdk-migrations.md for the full list of connectors.
        """

        if isinstance(declarative_stream.retriever, SimpleRetriever) and isinstance(
            declarative_stream.retriever.requester, HttpRequester
        ):
            http_requester = declarative_stream.retriever.requester
            if "stream_state" in http_requester._path.string:
                self.logger.warning(
                    f"Low-code stream '{declarative_stream.name}' uses interpolation of stream_state in the HttpRequester which is not thread-safe. Defaulting to synchronous processing"
                )
                return False

            request_options_provider = http_requester._request_options_provider
            if request_options_provider.request_options_contain_stream_state():
                self.logger.warning(
                    f"Low-code stream '{declarative_stream.name}' uses interpolation of stream_state in the HttpRequester which is not thread-safe. Defaulting to synchronous processing"
                )
                return False

            record_selector = declarative_stream.retriever.record_selector
            if isinstance(record_selector, RecordSelector):
                if (
                    record_selector.record_filter
                    and not isinstance(
                        record_selector.record_filter, ClientSideIncrementalRecordFilterDecorator
                    )
                    and "stream_state" in record_selector.record_filter.condition
                ):
                    self.logger.warning(
                        f"Low-code stream '{declarative_stream.name}' uses interpolation of stream_state in the RecordFilter which is not thread-safe. Defaulting to synchronous processing"
                    )
                    return False

                for add_fields in [
                    transformation
                    for transformation in record_selector.transformations
                    if isinstance(transformation, AddFields)
                ]:
                    for field in add_fields.fields:
                        if isinstance(field.value, str) and "stream_state" in field.value:
                            self.logger.warning(
                                f"Low-code stream '{declarative_stream.name}' uses interpolation of stream_state in the AddFields which is not thread-safe. Defaulting to synchronous processing"
                            )
                            return False
                        if (
                            isinstance(field.value, InterpolatedString)
                            and "stream_state" in field.value.string
                        ):
                            self.logger.warning(
                                f"Low-code stream '{declarative_stream.name}' uses interpolation of stream_state in the AddFields which is not thread-safe. Defaulting to synchronous processing"
                            )
                            return False
        return True

    @staticmethod
    def _get_retriever(
        declarative_stream: DeclarativeStream, stream_state: Mapping[str, Any]
    ) -> Retriever:
        retriever = declarative_stream.retriever

        # This is an optimization so that we don't invoke any cursor or state management flows within the
        # low-code framework because state management is handled through the ConcurrentCursor.
        if declarative_stream and isinstance(retriever, SimpleRetriever):
            # Also a temporary hack. In the legacy Stream implementation, as part of the read,
            # set_initial_state() is called to instantiate incoming state on the cursor. Although we no
            # longer rely on the legacy low-code cursor for concurrent checkpointing, low-code components
            # like StopConditionPaginationStrategyDecorator and ClientSideIncrementalRecordFilterDecorator
            # still rely on a DatetimeBasedCursor that is properly initialized with state.
            if retriever.cursor:
                retriever.cursor.set_initial_state(stream_state=stream_state)
            # We zero it out here, but since this is a cursor reference, the state is still properly
            # instantiated for the other components that reference it
            retriever.cursor = None

        return retriever

    @staticmethod
    def _select_streams(
        streams: List[AbstractStream], configured_catalog: ConfiguredAirbyteCatalog
    ) -> List[AbstractStream]:
        stream_name_to_instance: Mapping[str, AbstractStream] = {s.name: s for s in streams}
        abstract_streams: List[AbstractStream] = []
        for configured_stream in configured_catalog.streams:
            stream_instance = stream_name_to_instance.get(configured_stream.stream.name)
            if stream_instance:
                abstract_streams.append(stream_instance)

        return abstract_streams

    @staticmethod
    def _remove_concurrent_streams_from_catalog(
        catalog: ConfiguredAirbyteCatalog,
        concurrent_stream_names: set[str],
    ) -> ConfiguredAirbyteCatalog:
        return ConfiguredAirbyteCatalog(
            streams=[
                stream
                for stream in catalog.streams
                if stream.stream.name not in concurrent_stream_names
            ]
        )
