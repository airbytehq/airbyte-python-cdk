#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from collections import OrderedDict
from typing import Any, Callable, Iterable, Mapping, Optional, Union

from airbyte_cdk.sources.declarative.incremental.declarative_cursor import DeclarativeCursor
from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.streams.checkpoint.per_partition_key_serializer import (
    PerPartitionKeySerializer,
)
from airbyte_cdk.sources.types import Record, StreamSlice, StreamState

logger = logging.getLogger("airbyte")


class CursorFactory:
    def __init__(self, create_function: Callable[[], DeclarativeCursor]):
        self._create_function = create_function

    def create(self) -> DeclarativeCursor:
        return self._create_function()


class PerPartitionCursor(DeclarativeCursor):
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

    def __init__(self, cursor_factory: CursorFactory, partition_router: PartitionRouter):
        self._cursor_factory = cursor_factory
        self._partition_router = partition_router
        # The dict is ordered to ensure that once the maximum number of partitions is reached,
        # the oldest partitions can be efficiently removed, maintaining the most recent partitions.
        self._cursor_per_partition: OrderedDict[str, DeclarativeCursor] = OrderedDict()
        self._over_limit = 0
        self._partition_serializer = PerPartitionKeySerializer()

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

    def set_initial_state(self, stream_state: StreamState) -> None:
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

    def observe(self, stream_slice: StreamSlice, record: Record) -> None:
        self._cursor_per_partition[self._to_partition_key(stream_slice.partition)].observe(
            StreamSlice(partition={}, cursor_slice=stream_slice.cursor_slice), record
        )

    def close_slice(self, stream_slice: StreamSlice, *args: Any) -> None:
        try:
            self._cursor_per_partition[self._to_partition_key(stream_slice.partition)].close_slice(
                StreamSlice(partition={}, cursor_slice=stream_slice.cursor_slice), *args
            )
        except KeyError as exception:
            raise ValueError(
                f"Partition {str(exception)} could not be found in current state based on the record. This is unexpected because "
                f"we should only update state for partitions that were emitted during `stream_slices`"
            )

    def get_stream_state(self) -> StreamState:
        states = []
        for partition_tuple, cursor in self._cursor_per_partition.items():
            cursor_state = cursor.get_stream_state()
            if cursor_state:
                states.append(
                    {
                        "partition": self._to_dict(partition_tuple),
                        "cursor": cursor_state,
                    }
                )
        state: dict[str, Any] = {"states": states}

        parent_state = self._partition_router.get_stream_state()
        if parent_state:
            state["parent_state"] = parent_state
        return state

    def _get_state_for_partition(self, partition: Mapping[str, Any]) -> Optional[StreamState]:
        cursor = self._cursor_per_partition.get(self._to_partition_key(partition))
        if cursor:
            return cursor.get_stream_state()

        return None

    @staticmethod
    def _is_new_state(stream_state: Mapping[str, Any]) -> bool:
        return not bool(stream_state)

    def _to_partition_key(self, partition: Mapping[str, Any]) -> str:
        return self._partition_serializer.to_partition_key(partition)

    def _to_dict(self, partition_key: str) -> Mapping[str, Any]:
        return self._partition_serializer.to_partition(partition_key)

    def select_state(self, stream_slice: Optional[StreamSlice] = None) -> Optional[StreamState]:
        if not stream_slice:
            raise ValueError("A partition needs to be provided in order to extract a state")

        if not stream_slice:
            return None

        return self._get_state_for_partition(stream_slice.partition)

    def _create_cursor(self, cursor_state: Any) -> DeclarativeCursor:
        cursor = self._cursor_factory.create()
        cursor.set_initial_state(cursor_state)
        return cursor

    def get_request_params(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        if stream_slice:
            if self._to_partition_key(stream_slice.partition) not in self._cursor_per_partition:
                self._create_cursor_for_partition(self._to_partition_key(stream_slice.partition))
            return self._partition_router.get_request_params(  # type: ignore # this always returns a mapping
                stream_state=stream_state,
                stream_slice=StreamSlice(partition=stream_slice.partition, cursor_slice={}),
                next_page_token=next_page_token,
            ) | self._cursor_per_partition[
                self._to_partition_key(stream_slice.partition)
            ].get_request_params(
                stream_state=stream_state,
                stream_slice=StreamSlice(partition={}, cursor_slice=stream_slice.cursor_slice),
                next_page_token=next_page_token,
            )
        else:
            raise ValueError("A partition needs to be provided in order to get request params")

    def get_request_headers(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        if stream_slice:
            if self._to_partition_key(stream_slice.partition) not in self._cursor_per_partition:
                self._create_cursor_for_partition(self._to_partition_key(stream_slice.partition))
            return self._partition_router.get_request_headers(  # type: ignore # this always returns a mapping
                stream_state=stream_state,
                stream_slice=StreamSlice(partition=stream_slice.partition, cursor_slice={}),
                next_page_token=next_page_token,
            ) | self._cursor_per_partition[
                self._to_partition_key(stream_slice.partition)
            ].get_request_headers(
                stream_state=stream_state,
                stream_slice=StreamSlice(partition={}, cursor_slice=stream_slice.cursor_slice),
                next_page_token=next_page_token,
            )
        else:
            raise ValueError("A partition needs to be provided in order to get request headers")

    def get_request_body_data(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Union[Mapping[str, Any], str]:
        if stream_slice:
            if self._to_partition_key(stream_slice.partition) not in self._cursor_per_partition:
                self._create_cursor_for_partition(self._to_partition_key(stream_slice.partition))
            return self._partition_router.get_request_body_data(  # type: ignore # this always returns a mapping
                stream_state=stream_state,
                stream_slice=StreamSlice(partition=stream_slice.partition, cursor_slice={}),
                next_page_token=next_page_token,
            ) | self._cursor_per_partition[
                self._to_partition_key(stream_slice.partition)
            ].get_request_body_data(
                stream_state=stream_state,
                stream_slice=StreamSlice(partition={}, cursor_slice=stream_slice.cursor_slice),
                next_page_token=next_page_token,
            )
        else:
            raise ValueError("A partition needs to be provided in order to get request body data")

    def get_request_body_json(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        if stream_slice:
            if self._to_partition_key(stream_slice.partition) not in self._cursor_per_partition:
                self._create_cursor_for_partition(self._to_partition_key(stream_slice.partition))
            return self._partition_router.get_request_body_json(  # type: ignore # this always returns a mapping
                stream_state=stream_state,
                stream_slice=StreamSlice(partition=stream_slice.partition, cursor_slice={}),
                next_page_token=next_page_token,
            ) | self._cursor_per_partition[
                self._to_partition_key(stream_slice.partition)
            ].get_request_body_json(
                stream_state=stream_state,
                stream_slice=StreamSlice(partition={}, cursor_slice=stream_slice.cursor_slice),
                next_page_token=next_page_token,
            )
        else:
            raise ValueError("A partition needs to be provided in order to get request body json")

    def should_be_synced(self, record: Record) -> bool:
        return self._get_cursor(record).should_be_synced(
            self._convert_record_to_cursor_record(record)
        )

    @staticmethod
    def _convert_record_to_cursor_record(record: Record) -> Record:
        return Record(
            data=record.data,
            stream_name=record.stream_name,
            associated_slice=StreamSlice(
                partition={}, cursor_slice=record.associated_slice.cursor_slice
            )
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
            self._create_cursor_for_partition(partition_key)
        cursor = self._cursor_per_partition[partition_key]
        return cursor

    def _create_cursor_for_partition(self, partition_key: str) -> None:
        """
        Dynamically creates and initializes a cursor for the specified partition.

        This method is required for `ConcurrentPerPartitionCursor`. For concurrent cursors,
        stream_slices is executed only for the concurrent cursor, so cursors per partition
        are not created for the declarative cursor. This method ensures that a cursor is available
        to create requests for the specified partition. The cursor is initialized
        with the per-partition state if present in the initial state, or with the global state
        adjusted by the lookback window, or with the state to migrate from.

        Note:
            This is a temporary workaround and should be removed once the declarative cursor
            is decoupled from the concurrent cursor implementation.

        Args:
            partition_key (str): The unique identifier for the partition for which the cursor
            needs to be created.
        """
        partition_state = (
            self._state_to_migrate_from if self._state_to_migrate_from else self._NO_CURSOR_STATE
        )
        cursor = self._create_cursor(partition_state)

        self._cursor_per_partition[partition_key] = cursor
