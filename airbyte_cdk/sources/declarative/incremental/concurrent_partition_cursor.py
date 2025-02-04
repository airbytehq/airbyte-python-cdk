#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
import logging
import threading
from collections import OrderedDict
from copy import deepcopy
from datetime import timedelta
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.declarative.incremental.global_substream_cursor import (
    Timer,
    iterate_with_last_flag_and_state,
)
from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.checkpoint.per_partition_key_serializer import (
    PerPartitionKeySerializer,
)
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor, Cursor, CursorField
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter import (
    AbstractStreamStateConverter,
)
from airbyte_cdk.sources.types import Record, StreamSlice, StreamState

logger = logging.getLogger("airbyte")


class ConcurrentCursorFactory:
    def __init__(self, create_function: Callable[..., ConcurrentCursor]):
        self._create_function = create_function

    def create(
        self, stream_state: Mapping[str, Any], runtime_lookback_window: Optional[timedelta]
    ) -> ConcurrentCursor:
        return self._create_function(
            stream_state=stream_state, runtime_lookback_window=runtime_lookback_window
        )


class ConcurrentPerPartitionCursor(Cursor):
    """
    Manages state per partition when a stream has many partitions, preventing data loss or duplication.

    Attributes:
        DEFAULT_MAX_PARTITIONS_NUMBER (int): Maximum number of partitions to retain in memory (default is 10,000).

    - **Partition Limitation Logic**
      Ensures the number of tracked partitions does not exceed the specified limit to prevent memory overuse. Oldest partitions are removed when the limit is reached.

    - **Global Cursor Fallback**
      New partitions use global state as the initial state to progress the state for deleted or new partitions. The history data added after the initial sync will be missing.

    CurrentPerPartitionCursor expects the state of the ConcurrentCursor to follow the format {cursor_field: cursor_value}.
    """

    DEFAULT_MAX_PARTITIONS_NUMBER = 10000
    _NO_STATE: Mapping[str, Any] = {}
    _NO_CURSOR_STATE: Mapping[str, Any] = {}
    _GLOBAL_STATE_KEY = "state"
    _PERPARTITION_STATE_KEY = "states"
    _KEY = 0
    _VALUE = 1

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
    ) -> None:
        self._global_cursor: Optional[StreamState] = {}
        self._stream_name = stream_name
        self._stream_namespace = stream_namespace
        self._message_repository = message_repository
        self._connector_state_manager = connector_state_manager
        self._connector_state_converter = connector_state_converter
        self._cursor_field = cursor_field

        self._cursor_factory = cursor_factory
        self._partition_router = partition_router

        # The dict is ordered to ensure that once the maximum number of partitions is reached,
        # the oldest partitions can be efficiently removed, maintaining the most recent partitions.
        self._cursor_per_partition: OrderedDict[str, ConcurrentCursor] = OrderedDict()
        self._semaphore_per_partition: OrderedDict[str, threading.Semaphore] = OrderedDict()
        self._finished_partitions: set[str] = set()
        self._lock = threading.Lock()
        self._timer = Timer()
        self._new_global_cursor: Optional[StreamState] = None
        self._lookback_window: int = 0
        self._parent_state: Optional[StreamState] = None
        self._over_limit: int = 0
        self._use_global_cursor: bool = False
        self._partition_serializer = PerPartitionKeySerializer()

        self._set_initial_state(stream_state)

    @property
    def cursor_field(self) -> CursorField:
        return self._cursor_field

    @property
    def state(self) -> MutableMapping[str, Any]:
        state: dict[str, Any] = {"use_global_cursor": self._use_global_cursor}
        if not self._use_global_cursor:
            states = []
            for partition_tuple, cursor in self._cursor_per_partition.items():
                if cursor.state:
                    states.append(
                        {
                            "partition": self._to_dict(partition_tuple),
                            "cursor": copy.deepcopy(cursor.state),
                        }
                    )
            state[self._PERPARTITION_STATE_KEY] = states

        if self._global_cursor:
            state[self._GLOBAL_STATE_KEY] = self._global_cursor
        if self._lookback_window is not None:
            state["lookback_window"] = self._lookback_window
        if self._parent_state is not None:
            state["parent_state"] = self._parent_state
        return state

    def close_partition(self, partition: Partition) -> None:
        # Attempt to retrieve the stream slice
        stream_slice: Optional[StreamSlice] = partition.to_slice()  # type: ignore[assignment]

        # Ensure stream_slice is not None
        if stream_slice is None:
            raise ValueError("stream_slice cannot be None")

        partition_key = self._to_partition_key(stream_slice.partition)
        self._cursor_per_partition[partition_key].close_partition(partition=partition)
        with self._lock:
            self._semaphore_per_partition[partition_key].acquire()
            cursor = self._cursor_per_partition[partition_key]
            if (
                partition_key in self._finished_partitions
                and self._semaphore_per_partition[partition_key]._value == 0
            ):
                if (
                    self._new_global_cursor is None
                    or self._new_global_cursor[self.cursor_field.cursor_field_key]
                    < cursor.state[self.cursor_field.cursor_field_key]
                ):
                    self._new_global_cursor = copy.deepcopy(cursor.state)
            if not self._use_global_cursor:
                self._emit_state_message()

    def ensure_at_least_one_state_emitted(self) -> None:
        """
        The platform expect to have at least one state message on successful syncs. Hence, whatever happens, we expect this method to be
        called.
        """
        if not any(
            semaphore_item[1]._value for semaphore_item in self._semaphore_per_partition.items()
        ):
            self._global_cursor = self._new_global_cursor
            self._lookback_window = self._timer.finish()
            self._parent_state = self._partition_router.get_stream_state()
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
        if self._timer.is_running():
            raise RuntimeError("stream_slices has been executed more than once.")

        slices = self._partition_router.stream_slices()
        self._timer.start()
        for partition in slices:
            yield from self._generate_slices_from_partition(partition)

    def _generate_slices_from_partition(self, partition: StreamSlice) -> Iterable[StreamSlice]:
        # Ensure the maximum number of partitions is not exceeded
        self._ensure_partition_limit()

        cursor = self._cursor_per_partition.get(self._to_partition_key(partition.partition))
        if not cursor:
            cursor = self._create_cursor(
                self._global_cursor,
                self._lookback_window if self._global_cursor else 0,
            )
            with self._lock:
                self._cursor_per_partition[self._to_partition_key(partition.partition)] = cursor
            self._semaphore_per_partition[self._to_partition_key(partition.partition)] = (
                threading.Semaphore(0)
            )

        for cursor_slice, is_last_slice, _ in iterate_with_last_flag_and_state(
            cursor.stream_slices(),
            lambda: None,
        ):
            self._semaphore_per_partition[self._to_partition_key(partition.partition)].release()
            if is_last_slice:
                self._finished_partitions.add(self._to_partition_key(partition.partition))
            yield StreamSlice(
                partition=partition, cursor_slice=cursor_slice, extra_fields=partition.extra_fields
            )

    def _ensure_partition_limit(self) -> None:
        """
        Ensure the maximum number of partitions does not exceed the predefined limit.

        Steps:
        1. Attempt to remove partitions that are marked as finished in `_finished_partitions`.
           These partitions are considered processed and safe to delete.
        2. If the limit is still exceeded and no finished partitions are available for removal,
           remove the oldest partition unconditionally. We expect failed partitions to be removed.

        Logging:
        - Logs a warning each time a partition is removed, indicating whether it was finished
          or removed due to being the oldest.
        """
        with self._lock:
            while len(self._cursor_per_partition) > self.DEFAULT_MAX_PARTITIONS_NUMBER - 1:
                self._over_limit += 1
                # Try removing finished partitions first
                for partition_key in list(self._cursor_per_partition.keys()):
                    if (
                        partition_key in self._finished_partitions
                        and self._semaphore_per_partition[partition_key]._value == 0
                    ):
                        oldest_partition = self._cursor_per_partition.pop(
                            partition_key
                        )  # Remove the oldest partition
                        logger.warning(
                            f"The maximum number of partitions has been reached. Dropping the oldest finished partition: {oldest_partition}. Over limit: {self._over_limit}."
                        )
                        break
                else:
                    # If no finished partitions can be removed, fall back to removing the oldest partition
                    oldest_partition = self._cursor_per_partition.popitem(last=False)[
                        1
                    ]  # Remove the oldest partition
                    logger.warning(
                        f"The maximum number of partitions has been reached. Dropping the oldest partition: {oldest_partition}. Over limit: {self._over_limit}."
                    )

    def _set_initial_state(self, stream_state: StreamState) -> None:
        """
        Initialize the cursor's state using the provided `stream_state`.

        This method supports global and per-partition state initialization.

        - **Global State**: If `states` is missing, the `state` is treated as global and applied to all partitions.
          The `global state` holds a single cursor position representing the latest processed record across all partitions.

        - **Lookback Window**: Configured via `lookback_window`, it defines the period (in seconds) for reprocessing records.
          This ensures robustness in case of upstream data delays or reordering. If not specified, it defaults to 0.

        - **Per-Partition State**: If `states` is present, each partition's cursor state is initialized separately.

        - **Parent State**: (if available) Used to initialize partition routers based on parent streams.

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
                    "state": {
                        "last_updated": "2023-05-27T00:00:00Z"
                    },
                    lookback_window: 10,
                    "parent_state": {
                        "parent_stream_name": {
                            "last_updated": "2023-05-27T00:00:00Z"
                        }
                    }
                }
        """
        if not stream_state:
            return

        if (
            self._PERPARTITION_STATE_KEY not in stream_state
            and self._GLOBAL_STATE_KEY not in stream_state
        ):
            # We assume that `stream_state` is in a global format that can be applied to all partitions.
            # Example: {"global_state_format_key": "global_state_format_value"}
            self._set_global_state(stream_state)

        else:
            self._use_global_cursor = stream_state.get("use_global_cursor", False)

            self._lookback_window = int(stream_state.get("lookback_window", 0))

            for state in stream_state.get(self._PERPARTITION_STATE_KEY, []):
                self._cursor_per_partition[self._to_partition_key(state["partition"])] = (
                    self._create_cursor(state["cursor"])
                )
                self._semaphore_per_partition[self._to_partition_key(state["partition"])] = (
                    threading.Semaphore(0)
                )

            # set default state for missing partitions if it is per partition with fallback to global
            if self._GLOBAL_STATE_KEY in stream_state:
                self._set_global_state(stream_state[self._GLOBAL_STATE_KEY])

        # Set initial parent state
        if stream_state.get("parent_state"):
            self._parent_state = stream_state["parent_state"]

        # Set parent state for partition routers based on parent streams
        self._partition_router.set_initial_state(stream_state)

    def _set_global_state(self, stream_state: Mapping[str, Any]) -> None:
        """
        Initializes the global cursor state from the provided stream state.

        If the cursor field key is present in the stream state, its value is parsed,
        formatted, and stored as the global cursor. This ensures consistency in state
        representation across partitions.
        """
        if self.cursor_field.cursor_field_key in stream_state:
            global_state_value = stream_state[self.cursor_field.cursor_field_key]
            final_format_global_state_value = self._connector_state_converter.output_format(
                self._connector_state_converter.parse_value(global_state_value)
            )

            fixed_global_state = {
                self.cursor_field.cursor_field_key: final_format_global_state_value
            }

            self._global_cursor = deepcopy(fixed_global_state)
            self._new_global_cursor = deepcopy(fixed_global_state)

    def observe(self, record: Record) -> None:
        if not self._use_global_cursor and self.limit_reached():
            self._use_global_cursor = True

        if not record.associated_slice:
            raise ValueError(
                "Invalid state as stream slices that are emitted should refer to an existing cursor"
            )
        self._cursor_per_partition[
            self._to_partition_key(record.associated_slice.partition)
        ].observe(record)

    def _to_partition_key(self, partition: Mapping[str, Any]) -> str:
        return self._partition_serializer.to_partition_key(partition)

    def _to_dict(self, partition_key: str) -> Mapping[str, Any]:
        return self._partition_serializer.to_partition(partition_key)

    def _create_cursor(
        self, cursor_state: Any, runtime_lookback_window: int = 0
    ) -> ConcurrentCursor:
        cursor = self._cursor_factory.create(
            stream_state=deepcopy(cursor_state),
            runtime_lookback_window=timedelta(seconds=runtime_lookback_window),
        )
        return cursor

    def should_be_synced(self, record: Record) -> bool:
        return self._get_cursor(record).should_be_synced(record)

    def _get_cursor(self, record: Record) -> ConcurrentCursor:
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

    def limit_reached(self) -> bool:
        return self._over_limit > self.DEFAULT_MAX_PARTITIONS_NUMBER
