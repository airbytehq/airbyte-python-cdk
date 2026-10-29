#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import copy
import datetime
import json
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


class SyncTokenCursor(Cursor):
    """Track opaque API-issued sync tokens and commit them after a complete partition read.

    Unpartitioned state is ``{"state_type": "sync_token", "token": "..."}``; partitioned
    state stores a ``states`` list pairing each partition with its token. Tokens become committed
    state only in ``close_partition``. Partitioned state is capped at 25,000 entries.
    """

    STATE_TYPE = "sync_token"

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
        self._page_counts: dict[str, int] = {}
        self._lock = threading.RLock()
        self._last_emission_time = 0.0
        self._logged_bootstrap_reasons: set[str] = set()
        self._state_is_foreign = False
        self._state_emitted = False
        self._warned_no_token = False
        self._warned_cap = False
        self._partitions_over_cap: set[str] = set()
        self._invalid_state_reason: Optional[str] = None
        self._load_state(stream_state or {})

    @property
    def decoder(self) -> Decoder:
        return self._decoder

    @property
    def partitioned(self) -> bool:
        return self._partitioned

    @property
    def partition_router(self) -> PartitionRouter:
        return self._partition_router

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
        if self._invalid_state_reason is not None:
            message = (
                f"Stream {self._stream_name}: the saved sync token state is malformed "
                f"({self._invalid_state_reason}). Refresh the stream to start a new full sync."
            )
            raise AirbyteTracedException(
                message=message, internal_message=message, failure_type=FailureType.system_error
            )
        seen_partitions: set[str] = set()
        for stream_slice in self._partition_router.stream_slices():
            partition = dict(stream_slice.partition or {})
            if "token" in partition:
                message = f"Stream {self._stream_name}: partition router uses reserved key 'token'"
                raise AirbyteTracedException(
                    message=message,
                    internal_message=message,
                    failure_type=FailureType.config_error,
                )
            partition_key = self._serializer.to_partition_key(partition)
            if partition_key in seen_partitions:
                continue
            seen_partitions.add(partition_key)
            with self._lock:
                committed = self._committed.get(partition_key)
                token = committed["token"] if committed else None
                at_partition_cap = len(self._committed) >= self._max_partitions
                if token:
                    self._log_replay(partition, committed or {})
                elif at_partition_cap:
                    self._partitions_over_cap.add(partition_key)
                    self._log_bootstrap("partition cap")
                elif not self._state_is_foreign:
                    self._log_bootstrap("none")
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
        partition_key = self._serializer.to_partition_key(page.stream_slice.partition or {})
        with self._lock:
            self._page_counts[partition_key] = self._page_counts.get(partition_key, 0) + 1

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

        token = self._coerce_token(token_value, path)
        if token is None:
            self._warn_missing_token()
            return

        with self._lock:
            self._pending[partition_key] = {
                "token": token,
                "committed_at": datetime.datetime.now(datetime.timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
            }

    def _coerce_token(self, value: Any, path: list[str]) -> Optional[str]:
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
            value_type = "dict"
        elif isinstance(value, list):
            value_type = "list"
        else:
            value_type = type(value).__name__
        message = (
            f"Stream {self._stream_name}: token_path {json.dumps(path)} holds a {value_type}, "
            "not a string or integer. Check token_path."
        )
        raise AirbyteTracedException(
            internal_message=message,
            message=message,
            failure_type=FailureType.system_error,
        )

    def _log_bootstrap(self, reason: str) -> None:
        if reason not in self._logged_bootstrap_reasons:
            logger.info(
                "Stream %s: no stored sync token (reason: %s); reading all records.",
                self._stream_name,
                reason,
            )
            self._logged_bootstrap_reasons.add(reason)

    def _log_foreign_state(self) -> None:
        self._state_is_foreign = True
        logger.warning(
            "Stream %s: the saved state belongs to another cursor type; reading all records once.",
            self._stream_name,
        )

    def _log_replay(self, partition: Mapping[str, Any], committed: Mapping[str, Any]) -> None:
        age = self._human_age(committed.get("committed_at"))
        if self._partitioned:
            logger.debug(
                "Stream %s: replaying the sync token committed %s ago for partition %s.",
                self._stream_name,
                age,
                dict(partition),
            )
        else:
            logger.info(
                "Stream %s: replaying the sync token committed %s ago.",
                self._stream_name,
                age,
            )

    @staticmethod
    def _human_age(committed_at: Any) -> str:
        if not isinstance(committed_at, str) or not committed_at:
            return "an unknown time"
        try:
            committed_time = datetime.datetime.fromisoformat(committed_at.replace("Z", "+00:00"))
            if committed_time.tzinfo is None:
                committed_time = committed_time.replace(tzinfo=datetime.timezone.utc)
            age_seconds = max(
                0,
                int(
                    (datetime.datetime.now(datetime.timezone.utc) - committed_time).total_seconds()
                ),
            )
        except (OverflowError, ValueError):
            return "an unknown time"

        units = (
            ("day", 24 * 60 * 60),
            ("hour", 60 * 60),
            ("minute", 60),
            ("second", 1),
        )
        parts = []
        for unit, unit_seconds in units:
            count, age_seconds = divmod(age_seconds, unit_seconds)
            if count:
                parts.append(f"{count} {unit}{'' if count == 1 else 's'}")
            if len(parts) == 2:
                break
        return " ".join(parts) if parts else "0 seconds"

    def _warn_missing_token(self) -> None:
        if not self._warned_no_token:
            logger.warning(
                "Stream %s: pagination ended without a new sync token; kept the stored token. "
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
            page_count = self._page_counts.pop(partition_key, 0)
            if pending is not None:
                if partition_key in self._committed or len(self._committed) < self._max_partitions:
                    self._committed[partition_key] = pending
                    self._partitions_over_cap.discard(partition_key)
                    logger.debug(
                        "Stream %s: committed sync token %s for partition %s after %s pages.",
                        self._stream_name,
                        pending["token"],
                        self._serializer.to_partition(partition_key),
                        page_count,
                    )
                else:
                    self._partitions_over_cap.add(partition_key)
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
        self._state_emitted = True

    def ensure_at_least_one_state_emitted(self) -> None:
        if self._invalid_state_reason is not None:
            return  # leave the malformed state in place for diagnosis
        if self._partitioned or not self._state_emitted:
            self._emit_state_message()
        if self._partitions_over_cap and not self._warned_cap:
            logger.warning(
                "Stream %s: %s partitions already store tokens; %s more read in full on every sync.",
                self._stream_name,
                len(self._committed),
                len(self._partitions_over_cap),
            )
            self._warned_cap = True

    def observe(self, record: Record) -> None:
        return

    def should_be_synced(self, record: Record) -> bool:
        return True

    def _load_state(self, stream_state: StreamState) -> None:
        if not stream_state:
            self._log_bootstrap("none")
            return
        if stream_state.get("state_type") != self.STATE_TYPE:
            self._log_foreign_state()
            return
        if self._partitioned:
            states = stream_state.get("states")
            if not isinstance(states, list) or "token" in stream_state:
                if "token" not in stream_state and "states" not in stream_state:
                    self._log_bootstrap("none")
                else:
                    self._invalid_state_reason = "unpartitioned shape on a partitioned stream"
                return
            committed: dict[str, dict[str, Any]] = {}
            for state in states:
                if not isinstance(state, Mapping):
                    self._invalid_state_reason = "a partition entry is not an object"
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
                    self._invalid_state_reason = "a partition entry has no valid token"
                    return
            self._committed.update(committed)
        else:
            if "states" in stream_state:
                self._invalid_state_reason = "partitioned shape on an unpartitioned stream"
                return
            token = stream_state.get("token")
            if isinstance(token, str) and token:
                self._committed["{}"] = {
                    "token": token,
                    "committed_at": stream_state.get("committed_at", ""),
                }
            elif "token" in stream_state:
                self._invalid_state_reason = "the token is not a non-empty string"
