import copy

from airbyte_cdk.sources.streams.concurrent.cursor import Cursor

#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from collections import OrderedDict
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional, Union

from airbyte_cdk.sources.declarative.incremental.declarative_cursor import DeclarativeCursor
from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.streams.checkpoint.per_partition_key_serializer import (
    PerPartitionKeySerializer,
)
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor, CursorField, CursorValueType, GapType
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.types import Record, StreamSlice, StreamState
import functools
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Protocol, Tuple

from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams import NO_CURSOR_STATE_KEY
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.record import Record
from airbyte_cdk.sources.streams.concurrent.partitions.stream_slicer import StreamSlicer
from airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter import (
    AbstractStreamStateConverter,
)
from airbyte_cdk.sources.types import StreamSlice

logger = logging.getLogger("airbyte")


class ConcurrentCursorFactory:
    def __init__(self, create_function: Callable[[Mapping[str, Any]], DeclarativeCursor]):
        self._create_function = create_function

    def create(self, stream_state: Mapping[str, Any]) -> DeclarativeCursor:
        return self._create_function(stream_state=stream_state)[0]


class ConcurrentPerPartitionCursor(Cursor):
    """
    Manages state per partition when a stream has many partitions, to prevent data loss or duplication.

    **Partition Limitation and Limit Reached Logic**

    - **DEFAULT_MAX_PARTITIONS_NUMBER**: The maximum number of partitions to keep in memory (default is 10,000).
    - **_cursor_per_partition**: An ordered dictionary that stores cursors for each partition.
    - **_over_limit**: A counter that increments each time an oldest partition is removed when the limit is exceeded.

    The class ensures that the number of partitions tracked does not exceed the `DEFAULT_MAX_PARTITIONS_NUMBER` to prevent excessive memory usage.

    - When the number of partitions exceeds the limit, the oldest partitions are removed from `_cursor_per_partition`, and `_over_limit` is incremented accordingly.
    - The `limit_reached` method returns `True` when `_over_limit` exceeds `DEFAULT_MAX_PARTITIONS_NUMBER`, indicating that the global cursor should be used instead of per-partition cursors.

    This approach avoids unnecessary switching to a global cursor due to temporary spikes in partition counts, ensuring that switching is only done when a sustained high number of partitions is observed.
    """

    DEFAULT_MAX_PARTITIONS_NUMBER = 10000
    _NO_STATE: Mapping[str, Any] = {}
    _NO_CURSOR_STATE: Mapping[str, Any] = {}
    _KEY = 0
    _VALUE = 1
    _state_to_migrate_from: Mapping[str, Any] = {}

    def __init__(
        self,
        cursor_factory: ConcurrentCursorFactory,
        partition_router: PartitionRouter,
        stream_name: str,
        stream_namespace: Optional[str],
        stream_state: Any,
        message_repository: MessageRepository,
        connector_state_manager: ConnectorStateManager,
        connector_state_converter: AbstractStreamStateConverter,
        cursor_field: CursorField,
        slice_boundary_fields: Optional[Tuple[str, str]],
        start: Optional[CursorValueType],
        end_provider: Callable[[], CursorValueType],
        lookback_window: Optional[GapType] = None,
        slice_range: Optional[GapType] = None,
        cursor_granularity: Optional[GapType] = None,
    ) -> None:
        self._stream_name = stream_name
        self._stream_namespace = stream_namespace
        self._message_repository = message_repository
        self._connector_state_converter = connector_state_converter
        self._connector_state_manager = connector_state_manager
        self._cursor_field = cursor_field
        # To see some example where the slice boundaries might not be defined, check https://github.com/airbytehq/airbyte/blob/1ce84d6396e446e1ac2377362446e3fb94509461/airbyte-integrations/connectors/source-stripe/source_stripe/streams.py#L363-L379
        self._slice_boundary_fields = slice_boundary_fields
        self._start = start
        self._end_provider = end_provider
        self._conccurent_state = stream_state
        # self.start, self._concurrent_state = self._get_concurrent_state(stream_state)
        self._lookback_window = lookback_window
        self._slice_range = slice_range
        self._most_recent_cursor_value_per_partition: MutableMapping[Partition, Any] = {}
        self._has_closed_at_least_one_slice = False
        self._cursor_granularity = cursor_granularity

        self._cursor_factory = cursor_factory
        self._partition_router = partition_router
        # The dict is ordered to ensure that once the maximum number of partitions is reached,
        # the oldest partitions can be efficiently removed, maintaining the most recent partitions.
        self._cursor_per_partition: OrderedDict[str, Cursor] = OrderedDict()
        self._over_limit = 0
        self._state = {}
        self._partition_serializer = PerPartitionKeySerializer()

        self._set_initial_state(stream_state)

    @property
    def state(self) -> MutableMapping[str, Any]:
        states = []
        for partition_tuple, cursor in self._cursor_per_partition.items():
            cursor_state = cursor._connector_state_converter.convert_to_state_message(
                cursor._cursor_field, cursor.state
            )
            # print(cursor_state, cursor.state)
            if cursor_state:
                states.append(
                    {
                        "partition": self._to_dict(partition_tuple),
                        "cursor": copy.deepcopy(cursor_state),
                    }
                )
        state: dict[str, Any] = {"states": states}
        # print(state)
        return state

    @property
    def cursor_field(self) -> CursorField:
        return self._cursor_field

    def close_partition(self, partition: Partition) -> None:
        self._cursor_per_partition[self._to_partition_key(partition._stream_slice.partition)].close_partition_without_emit(partition=partition)

    def ensure_at_least_one_state_emitted(self) -> None:
        """
        The platform expect to have at least one state message on successful syncs. Hence, whatever happens, we expect this method to be
        called.
        """
        self._emit_state_message()

    def _emit_state_message(self) -> None:
        self._connector_state_manager.update_state_for_stream(
            self._stream_name,
            self._stream_namespace,
            self.state,
        )
        state_message = self._connector_state_manager.create_state_message(
            self._stream_name, self._stream_namespace
        )
        self._message_repository.emit_message(state_message)


    def stream_slices(self) -> Iterable[StreamSlice]:
        slices = self._partition_router.stream_slices()
        for partition in slices:
            yield from self.generate_slices_from_partition(partition)

    def generate_slices_from_partition(self, partition: StreamSlice) -> Iterable[StreamSlice]:
        # Ensure the maximum number of partitions is not exceeded
        self._ensure_partition_limit()

        cursor = self._cursor_per_partition.get(self._to_partition_key(partition.partition))
        if not cursor:
            partition_state = (
                self._state_to_migrate_from
                if self._state_to_migrate_from
                else self._NO_CURSOR_STATE
            )
            cursor = self._create_cursor(partition_state)
            self._cursor_per_partition[self._to_partition_key(partition.partition)] = cursor

        for cursor_slice in cursor.stream_slices():
            yield StreamSlice(
                partition=partition, cursor_slice=cursor_slice, extra_fields=partition.extra_fields
            )

    def _ensure_partition_limit(self) -> None:
        """
        Ensure the maximum number of partitions is not exceeded. If so, the oldest added partition will be dropped.
        """
        while len(self._cursor_per_partition) > self.DEFAULT_MAX_PARTITIONS_NUMBER - 1:
            self._over_limit += 1
            oldest_partition = self._cursor_per_partition.popitem(last=False)[
                0
            ]  # Remove the oldest partition
            logger.warning(
                f"The maximum number of partitions has been reached. Dropping the oldest partition: {oldest_partition}. Over limit: {self._over_limit}."
            )

    def limit_reached(self) -> bool:
        return self._over_limit > self.DEFAULT_MAX_PARTITIONS_NUMBER

    def _set_initial_state(self, stream_state: StreamState) -> None:
        """
        Set the initial state for the cursors.

        This method initializes the state for each partition cursor using the provided stream state.
        If a partition state is provided in the stream state, it will update the corresponding partition cursor with this state.

        Additionally, it sets the parent state for partition routers that are based on parent streams. If a partition router
        does not have parent streams, this step will be skipped due to the default PartitionRouter implementation.

        Args:
            stream_state (StreamState): The state of the streams to be set. The format of the stream state should be:
                {
                    "states": [
                        {
                            "partition": {
                                "partition_key": "value"
                            },
                            "cursor": {
                                "last_updated": "2023-05-27T00:00:00Z"
                            }
                        }
                    ],
                    "parent_state": {
                        "parent_stream_name": {
                            "last_updated": "2023-05-27T00:00:00Z"
                        }
                    }
                }
        """
        if not stream_state:
            return

        if "states" not in stream_state:
            # We assume that `stream_state` is in a global format that can be applied to all partitions.
            # Example: {"global_state_format_key": "global_state_format_value"}
            self._state_to_migrate_from = stream_state

        else:
            for state in stream_state["states"]:
                self._cursor_per_partition[self._to_partition_key(state["partition"])] = (
                    self._create_cursor(state["cursor"])
                )

            # set default state for missing partitions if it is per partition with fallback to global
            if "state" in stream_state:
                self._state_to_migrate_from = stream_state["state"]

        # Set parent state for partition routers based on parent streams
        self._partition_router.set_initial_state(stream_state)

    def observe(self, record: Record) -> None:
        print(f"ESTATE: {self._to_partition_key(record.partition._stream_slice.partition)}: {record.data[self.cursor_field.cursor_field_key]}")
        self._cursor_per_partition[self._to_partition_key(record.partition._stream_slice.partition)].observe(record)

    def _to_partition_key(self, partition: Mapping[str, Any]) -> str:
        return self._partition_serializer.to_partition_key(partition)

    def _to_dict(self, partition_key: str) -> Mapping[str, Any]:
        return self._partition_serializer.to_partition(partition_key)

    def _create_cursor(self, cursor_state: Any) -> DeclarativeCursor:
        cursor = self._cursor_factory.create(stream_state=cursor_state)
        return cursor

    def should_be_synced(self, record: Record) -> bool:
        return self._get_cursor(record).should_be_synced(
            self._convert_record_to_cursor_record(record)
        )

    def is_greater_than_or_equal(self, first: Record, second: Record) -> bool:
        if not first.associated_slice or not second.associated_slice:
            raise ValueError(
                f"Both records should have an associated slice but got {first.associated_slice} and {second.associated_slice}"
            )
        if first.associated_slice.partition != second.associated_slice.partition:
            raise ValueError(
                f"To compare records, partition should be the same but got {first.associated_slice.partition} and {second.associated_slice.partition}"
            )

        return self._get_cursor(first).is_greater_than_or_equal(
            self._convert_record_to_cursor_record(first),
            self._convert_record_to_cursor_record(second),
        )

    @staticmethod
    def _convert_record_to_cursor_record(record: Record) -> Record:
        return Record(
            record.data,
            StreamSlice(partition={}, cursor_slice=record.associated_slice.cursor_slice)
            if record.associated_slice
            else None,
        )

    def _get_cursor(self, record: Record) -> DeclarativeCursor:
        if not record.associated_slice:
            raise ValueError(
                "Invalid state as stream slices that are emitted should refer to an existing cursor"
            )
        partition_key = self._to_partition_key(record.associated_slice.partition)
        if partition_key not in self._cursor_per_partition:
            raise ValueError(
                "Invalid state as stream slices that are emitted should refer to an existing cursor"
            )
        cursor = self._cursor_per_partition[partition_key]
        return cursor