#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import copy
import datetime
import logging
import threading
import time
from collections import OrderedDict
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import dpath

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.declarative.decoders import Decoder
from airbyte_cdk.sources.declarative.incremental.concurrent_partition_cursor import (
    ConcurrentPerPartitionCursor,
)
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import (
    InterpolatedBoolean,
)
from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.declarative.partition_routers.single_partition_router import (
    SinglePartitionRouter,
)
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import PageCompleted
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.checkpoint.per_partition_key_serializer import (
    PerPartitionKeySerializer,
)
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor, CursorField
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.types import Config, Record, StreamSlice, StreamState
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

logger = logging.getLogger("airbyte")


class OpaqueTokenCursor(Cursor):
    STATE_TYPE = "opaque_token"

    def __init__(
        self,
        *,
        stream_name: str,
        stream_namespace: Optional[str],
        stream_state: Optional[StreamState],
        message_repository: MessageRepository,
        connector_state_manager: ConnectorStateManager,
        partition_router: PartitionRouter,
        token_path: list[str],
        decoder: Decoder,
        config: Config,
        parameters: Mapping[str, Any],
        completion_condition: Optional[str] = None,
        cursor_field: Optional[str] = None,
        max_partitions: int = ConcurrentPerPartitionCursor.DEFAULT_MAX_PARTITIONS_NUMBER,
    ) -> None:
        self._stream_name = stream_name
        self._stream_namespace = stream_namespace
        self._message_repository = message_repository
        self._connector_state_manager = connector_state_manager
        self._partition_router = partition_router
        self._partitioned = not isinstance(partition_router, SinglePartitionRouter)
        self._token_path = [
            InterpolatedString.create(path, parameters=parameters) for path in token_path
        ]
        self._decoder = decoder
        self._config = config
        self._completion_condition = (
            InterpolatedBoolean(completion_condition, parameters=parameters)
            if completion_condition is not None
            else None
        )
        self.cursor_field = CursorField(cursor_field) if cursor_field else None
        self._max_partitions = max_partitions
        self._serializer = PerPartitionKeySerializer()
        self._committed: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._pending: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._last_emission_time = 0.0
        self._warned_no_token = False
        self._warned_cap = False
        self._partitions_over_cap = 0
        self._load_state(stream_state or {})

    @property
    def state(self) -> MutableMapping[str, Any]:
        with self._lock:
            if not self._partitioned:
                state: dict[str, Any] = {"state_type": self.STATE_TYPE}
                if self._committed:
                    state.update(copy.deepcopy(next(iter(self._committed.values()))))
                return state
            return {
                "state_type": self.STATE_TYPE,
                "states": [
                    {
                        "partition": self._serializer.to_partition(partition_key),
                        "cursor": copy.deepcopy(cursor),
                    }
                    for partition_key, cursor in self._committed.items()
                ],
            }

    def stream_slices(self) -> Iterable[StreamSlice]:
        seen_partitions: set[str] = set()
        for stream_slice in self._partition_router.stream_slices():
            partition = dict(stream_slice.partition or {})
            if "token" in partition:
                raise ValueError(
                    f"Stream {self._stream_name}: partition router uses reserved key 'token'"
                )
            partition_key = self._serializer.to_partition_key(partition)
            if partition_key in seen_partitions:
                continue
            seen_partitions.add(partition_key)
            with self._lock:
                committed = self._committed.get(partition_key)
                token = committed["token"] if committed else None
            yield StreamSlice(
                partition=partition,
                cursor_slice={"token": token} if token else {},
                extra_fields=stream_slice.extra_fields,
            )

        if self._partitioned:
            with self._lock:
                vanished = set(self._committed) - seen_partitions
                for partition_key in vanished:
                    del self._committed[partition_key]
                if vanished:
                    logger.info(
                        "Stream %s: dropped the stored tokens of %s partitions the router no longer returns.",
                        self._stream_name,
                        len(vanished),
                    )

    def on_page_completed(self, page: PageCompleted) -> None:
        if not page.pagination_exhausted or page.response is None:
            return

        body = next(
            (
                decoded
                for decoded in self._decoder.decode(page.response)
                if isinstance(decoded, Mapping)
            ),
            {},
        )
        path = [str(part.eval(self._config)) for part in self._token_path]
        token_value = dpath.get(body, path, default=None) if isinstance(body, Mapping) else None
        token = self._coerce_token(token_value, path)
        if token is None:
            self._warn_missing_token()
            return

        if self._completion_condition is not None:
            complete = self._completion_condition.eval(
                self._config,
                response=body,
                headers=page.response.headers,
                stream_slice=page.stream_slice,
            )
        else:
            complete = True
        if not complete:
            self._warn_missing_token()
            return

        partition_key = self._serializer.to_partition_key(page.stream_slice.partition or {})
        with self._lock:
            self._pending[partition_key] = {
                "token": token,
                "committed_at": datetime.datetime.now(datetime.timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
            }

    @staticmethod
    def _coerce_token(value: Any, path: list[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            value_type = "boolean"
        elif isinstance(value, str):
            return value
        elif isinstance(value, int):
            return str(value)
        elif isinstance(value, float):
            value_type = "float"
        elif isinstance(value, Mapping):
            value_type = "object"
        elif isinstance(value, list):
            value_type = "list"
        else:
            value_type = type(value).__name__
        raise AirbyteTracedException(
            internal_message=f"Stream token_path {path} holds a {value_type}, not a string or integer.",
            message=f"Token path {path} holds a {value_type}, not a string or integer. Check token_path.",
            failure_type=FailureType.system_error,
        )

    def _warn_missing_token(self) -> None:
        if not self._warned_no_token:
            logger.warning(
                "Stream %s: pagination ended without a new change token; kept the stored token. "
                "The next sync re-reads from it.",
                self._stream_name,
            )
            self._warned_no_token = True

    def close_partition(self, partition: Partition) -> None:
        stream_slice = partition.to_slice()
        if stream_slice is None:
            raise ValueError("stream_slice cannot be None")
        partition_values = getattr(stream_slice, "partition", None)
        if partition_values is None:
            partition_values = stream_slice.get("partition", {})
        partition_key = self._serializer.to_partition_key(partition_values)
        with self._lock:
            pending = self._pending.pop(partition_key, None)
            if pending is not None:
                if partition_key in self._committed or len(self._committed) < self._max_partitions:
                    self._committed[partition_key] = pending
                else:
                    self._partitions_over_cap += 1
                    if not self._warned_cap:
                        logger.warning(
                            "Stream %s: %s partitions already store tokens; additional partitions read in full.",
                            self._stream_name,
                            len(self._committed),
                        )
                        self._warned_cap = True
            if not self._partitioned:
                self._emit_state_message()
            elif time.time() - self._last_emission_time > 600:
                self._emit_state_message()

    def _emit_state_message(self) -> None:
        self._last_emission_time = time.time()
        self._connector_state_manager.update_state_for_stream(
            self._stream_name, self._stream_namespace, self.state
        )
        self._message_repository.emit_message(
            self._connector_state_manager.create_state_message(
                self._stream_name, self._stream_namespace
            )
        )

    def ensure_at_least_one_state_emitted(self) -> None:
        self._emit_state_message()

    def observe(self, record: Record) -> None:
        return

    def should_be_synced(self, record: Record) -> bool:
        return True

    def _load_state(self, stream_state: StreamState) -> None:
        if not stream_state:
            return
        if stream_state.get("state_type") != self.STATE_TYPE:
            logger.info(
                "Stream %s: ignoring foreign state and reading from the beginning.",
                self._stream_name,
            )
            return
        if self._partitioned:
            states = stream_state.get("states")
            if not isinstance(states, list) or "token" in stream_state:
                logger.info(
                    "Stream %s: ignoring state with an incompatible partition shape.",
                    self._stream_name,
                )
                return
            committed: dict[str, dict[str, Any]] = {}
            for state in states:
                if not isinstance(state, Mapping):
                    logger.info(
                        "Stream %s: ignoring state with an incompatible opaque token partition.",
                        self._stream_name,
                    )
                    return
                partition = state.get("partition")
                cursor = state.get("cursor")
                if (
                    isinstance(partition, Mapping)
                    and isinstance(cursor, Mapping)
                    and isinstance(cursor.get("token"), str)
                    and cursor.get("token")
                ):
                    committed[self._serializer.to_partition_key(partition)] = {
                        "token": cursor["token"],
                        "committed_at": cursor.get("committed_at", ""),
                    }
                else:
                    logger.info(
                        "Stream %s: ignoring state with an incompatible opaque token partition.",
                        self._stream_name,
                    )
                    return
            self._committed.update(committed)
        else:
            if "states" in stream_state:
                logger.info(
                    "Stream %s: ignoring state with an incompatible unpartitioned shape.",
                    self._stream_name,
                )
                return
            token = stream_state.get("token")
            if isinstance(token, str) and token:
                self._committed["{}"] = {
                    "token": token,
                    "committed_at": stream_state.get("committed_at", ""),
                }
            elif token is not None:
                logger.info(
                    "Stream %s: ignoring an incompatible opaque token state.",
                    self._stream_name,
                )
