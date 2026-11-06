# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Checks on the cursor state that a declarative source emits during `test_basic_read`.

The declarative factory builds a stream's cursor from the manifest alone, whatever sync mode the
catalog configures, so the full-refresh read of `test_basic_read` emits the same state an
incremental sync would. These checks inspect that state, together with the records around it,
without another read:

- checkpoints: no record emitted after a state message lies below the point a sync resumed from
  that state would start at, unless a copy of it was emitted before the state message;
- final state: a stream's final `ConcurrentCursor` state is not behind its newest record;
- cursor values: records carry a cursor value that the cursor can parse (advisory);
- per-partition state shape, parent state and partition coverage;
- round trip: the source builds from its own final state.

Only streams whose cursor is a `ConcurrentCursor` or a `ConcurrentPerPartitionCursor` are checked.
Every other stream (no cursor, `FinalStateCursor`, `SyncTokenCursor`, custom or file-based
cursors) and every source that is not a `ConcurrentDeclarativeSource` is skipped.

Problems never quote record, cursor or partition values, since CI logs are public. They name
streams, fields, partition keys, formats and counts.

Checks marked as not enforced report their problems as a `CursorStateWarning` instead of failing
the test.
"""

from __future__ import annotations

import copy
import json
import math
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Iterator
from unittest import mock

import requests

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    StreamDescriptor,
    Type,
)
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.incremental.concurrent_partition_cursor import (
    ConcurrentPerPartitionCursor,
)
from airbyte_cdk.sources.declarative.partition_routers import (
    GroupingPartitionRouter,
    ListPartitionRouter,
    SubstreamPartitionRouter,
)
from airbyte_cdk.sources.streams.checkpoint.per_partition_key_serializer import (
    PerPartitionKeySerializer,
)
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor
from airbyte_cdk.sources.types import Record

_MAX_STREAMS_PER_PROBLEM = 10

_MAX_BOUNDS_PER_STREAM = 2_000
"""How many resume bounds to compute per stream. Each one builds a cursor from a state.

Bounds grow with the state, so past the cap a state uses the bound of the newest state below it
that has one. That bound is lower, so the cap can only hide problems, never invent them.
"""


class CursorStateWarning(UserWarning):
    """A cursor state problem in a `read` that the standard tests report but do not enforce yet.

    To make these problems fail the test, turn the warning into an error, either with
    `pytest -W error::airbyte_cdk.test.standard_tests.CursorStateWarning` or with
    `airbyte-cdk connector test --pytest-arg=-Werror::airbyte_cdk.test.standard_tests.CursorStateWarning`.
    """


@dataclass(frozen=True)
class CursorStateCheck:
    """A check on the cursor state of a read, and whether its problems fail the test."""

    name: str
    enforced: bool = True


CHECKPOINTS = CursorStateCheck("checkpoints do not skip records emitted after them")
FINAL_STATE = CursorStateCheck("final state is not behind the newest record")
CURSOR_VALUES = CursorStateCheck(
    "records carry a cursor value the cursor can parse", enforced=False
)
PARTITION_STATE_SHAPE = CursorStateCheck("per-partition state is well formed")
PARENT_STATE = CursorStateCheck("per-partition state holds the state of incremental parents")
PARTITION_COVERAGE = CursorStateCheck("per-partition state covers every listed partition")
ROUND_TRIP = CursorStateCheck("the source builds from its final state")
CHECK_ERRORS = CursorStateCheck("cursor state checks ran", enforced=False)

CURSOR_STATE_CHECKS: tuple[CursorStateCheck, ...] = (
    CHECKPOINTS,
    FINAL_STATE,
    CURSOR_VALUES,
    PARTITION_STATE_SHAPE,
    PARENT_STATE,
    PARTITION_COVERAGE,
    ROUND_TRIP,
    CHECK_ERRORS,
)

_SERIALIZER = PerPartitionKeySerializer()


def _partition_key(partition: Mapping[str, Any]) -> str:
    return _SERIALIZER.to_partition_key(partition)


def _has_step(cursor: ConcurrentCursor) -> bool:
    """Whether a cursor splits its range into several slices, so it can checkpoint mid-read."""
    step = cursor._slice_range  # noqa: SLF001  # The CDK's own cursor; no public accessor.
    return step is not None and step != timedelta.max


def _sub_cursor(cursor: ConcurrentPerPartitionCursor, state: Mapping[str, Any]) -> ConcurrentCursor:
    """Build the cursor that a resumed sync builds for a partition present in the state.

    A partition with an entry in `states` resumes from its own cursor, without the runtime
    `lookback_window` (`ConcurrentPerPartitionCursor._set_initial_state`).
    """
    return cursor._cursor_factory.create(  # noqa: SLF001  # The CDK's own cursor factory.
        stream_state=copy.deepcopy(dict(state)), runtime_lookback_window=None
    )


def _cursor_with_state(template: ConcurrentCursor, state: Mapping[str, Any]) -> ConcurrentCursor:
    """Copy a `ConcurrentCursor` and load a state into the copy, as its constructor does."""
    cursor = copy.copy(template)
    cursor.start, cursor._concurrent_state = cursor._get_concurrent_state(  # noqa: SLF001
        copy.deepcopy(dict(state))
    )
    return cursor


def _resume_start(cursor: ConcurrentCursor) -> Any:
    """The start of the first slice a cursor generates, parsed, or `None` without a slice."""
    first_slice = next(iter(cursor.stream_slices()), None)
    if first_slice is None:
        return None
    start_field = cursor._slice_boundary_fields_wrapper[cursor._START_BOUNDARY]  # noqa: SLF001
    value = first_slice.cursor_slice.get(start_field)
    if value is None:
        return None
    return cursor._connector_state_converter.parse_value(value)  # noqa: SLF001


def _incremental_parents(router: Any) -> list[str]:
    """Names of the parent streams whose state the router keeps (`incremental_dependency`)."""
    if isinstance(router, SubstreamPartitionRouter):
        return [
            parent.stream.name
            for parent in router.parent_stream_configs
            if parent.incremental_dependency
        ]
    if isinstance(router, GroupingPartitionRouter):
        return _incremental_parents(router.underlying_partition_router)
    return []


def _state_blob(message: AirbyteMessage) -> tuple[str, dict[str, Any]] | None:
    state = message.state
    if (
        message.type != Type.STATE
        or state is None
        or state.type != AirbyteStateType.STREAM
        or state.stream is None
    ):
        return None
    blob = state.stream.stream_state
    data = blob.__dict__ if isinstance(blob, AirbyteStateBlob) else (blob or {})
    return state.stream.stream_descriptor.name, copy.deepcopy(dict(data))


def _state_message(stream_name: str, state: Mapping[str, Any]) -> AirbyteStateMessage:
    return AirbyteStateMessage(
        type=AirbyteStateType.STREAM,
        stream=AirbyteStreamState(
            stream_descriptor=StreamDescriptor(name=stream_name),
            stream_state=AirbyteStateBlob(**copy.deepcopy(dict(state))),
        ),
    )


class _HttpRequestDuringBuild(RuntimeError):
    pass


@contextmanager
def _no_http() -> Iterator[None]:
    """Fail any HTTP request sent through `requests` while the block runs."""

    def _refuse(*_args: Any, **_kwargs: Any) -> None:
        raise _HttpRequestDuringBuild("HTTP request sent while building streams")

    with mock.patch.object(requests.Session, "send", _refuse):
        yield


class CursorStateRecorder:
    """Instance hooks on the source that runs the read. No class is patched.

    - `streams` is wrapped to keep the streams the read builds. For a per-partition stream whose
      cursor has a `step`, the state message throttle is lifted within a budget, so the read
      emits state at more partition closes (production can emit at any of them).
    - `_concurrent_source._handle_item` is wrapped to record the partition of each record of
      those streams, on the main thread, in the order the RECORD messages are emitted.

    `uninstall` removes both wrappers.
    """

    FIRST_LIFTED_CLOSES = 100
    """Partition closes that always emit state, from the start of the read."""
    LIFTED_CLOSES_GROWTH = 1.05
    """After the first closes, the gap between two lifted closes grows by this factor."""
    MAX_LIFTED_CLOSES = 300
    """Lifted closes per stream. Each emit copies the state of every partition."""

    def __init__(self, source: ConcurrentDeclarativeSource) -> None:
        self.source = source
        self.streams: dict[str, Any] = {}
        self.global_cursor_at_start: dict[str, bool] = {}
        self.lifted_streams: set[str] = set()
        self.record_partitions: dict[str, list[str | None]] = {}
        self._build_streams: Callable[..., Any] | None = None
        self._handle_item: Callable[..., Any] | None = None
        self._concurrent_source: Any = None

    @classmethod
    def install(cls, connector: object) -> CursorStateRecorder | None:
        """Install the hooks on a `ConcurrentDeclarativeSource`; return `None` for other sources."""
        if not isinstance(connector, ConcurrentDeclarativeSource):
            return None
        recorder = cls(connector)
        recorder._build_streams = connector.streams
        connector.streams = recorder._streams  # type: ignore[method-assign]
        concurrent_source = getattr(connector, "_concurrent_source", None)
        handle_item = getattr(concurrent_source, "_handle_item", None)
        if handle_item is not None:
            recorder._concurrent_source = concurrent_source
            recorder._handle_item = handle_item
            concurrent_source._handle_item = recorder._handle  # type: ignore[union-attr]
        return recorder

    def uninstall(self) -> None:
        if self._build_streams is not None:
            self.source.__dict__.pop("streams", None)
        if self._concurrent_source is not None:
            self._concurrent_source.__dict__.pop("_handle_item", None)

    @property
    def saw_read(self) -> bool:
        """Whether the hooked instance built the streams of the read."""
        return bool(self.streams)

    def _streams(self, config: Mapping[str, Any]) -> Any:
        assert self._build_streams is not None
        built = self._build_streams(config)
        for stream in built:
            cursor = getattr(stream, "cursor", None)
            self.streams[stream.name] = stream
            if isinstance(cursor, ConcurrentPerPartitionCursor):
                self.global_cursor_at_start[stream.name] = cursor._use_global_cursor  # noqa: SLF001
                if _has_step(_sub_cursor(cursor, {})):
                    self._lift_throttle(stream.name, cursor)
        return built

    def _lift_throttle(self, stream_name: str, cursor: ConcurrentPerPartitionCursor) -> None:
        self.lifted_streams.add(stream_name)
        self.record_partitions[stream_name] = []
        close_partition = cursor.close_partition
        default_interval = cursor.STATE_EMIT_INTERVAL_SECONDS
        lock = threading.Lock()
        closes = 0
        lifted = 0
        next_lifted = float(self.FIRST_LIFTED_CLOSES)

        def _close_partition(partition: Any) -> None:
            nonlocal closes, lifted, next_lifted
            with lock:
                closes += 1
                lift = lifted < self.MAX_LIFTED_CLOSES and (
                    closes <= self.FIRST_LIFTED_CLOSES or closes >= next_lifted
                )
                if lift:
                    lifted += 1
                    if closes > self.FIRST_LIFTED_CLOSES:
                        next_lifted = max(next_lifted * self.LIFTED_CLOSES_GROWTH, closes + 1)
                # A negative interval emits at once; the default keeps the production throttle.
                cursor.STATE_EMIT_INTERVAL_SECONDS = -1.0 if lift else default_interval
                close_partition(partition)

        cursor.close_partition = _close_partition  # type: ignore[method-assign]

    def _handle(self, queue_item: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(queue_item, Record) and isinstance(queue_item.data, Mapping):
            partitions = self.record_partitions.get(queue_item.stream_name)
            if partitions is not None:
                associated_slice = queue_item.associated_slice
                partitions.append(
                    _partition_key(associated_slice.partition)
                    if associated_slice is not None
                    else None
                )
        assert self._handle_item is not None
        return self._handle_item(queue_item, *args, **kwargs)


@dataclass
class _Checkpoint:
    position: int
    state: Mapping[str, Any]
    value: Any
    bound: Any = None
    has_bound: bool = False


@dataclass
class _StreamCheck:
    """What one stream's checks keep while walking the read's output."""

    name: str
    cursor: ConcurrentCursor | ConcurrentPerPartitionCursor
    template: ConcurrentCursor | ConcurrentPerPartitionCursor
    primary_key: list[list[str]]
    check_checkpoints: bool
    partitions: list[str | None] | None = None
    records: int = 0
    missing_values: int = 0
    unparseable_values: int = 0
    newest_value: Any = None
    has_newest_value: bool = False
    final_state: dict[str, Any] | None = None
    checkpoints: dict[str | None, list[_Checkpoint]] = field(default_factory=dict)
    copies: dict[int, list[tuple[int, Any]]] = field(default_factory=dict)
    bounds: dict[str, Any] = field(default_factory=dict)
    skipped_records: int = 0
    flagged_records: int = 0
    flagged_partitions: set[str | None] = field(default_factory=set)
    flagged_checkpoints: set[int] = field(default_factory=set)

    @property
    def partitioned(self) -> bool:
        return isinstance(self.cursor, ConcurrentPerPartitionCursor)

    @property
    def cursor_key(self) -> str:
        return self.cursor.cursor_field.cursor_field_key

    @property
    def converter(self) -> Any:
        return self.cursor._connector_state_converter  # noqa: SLF001

    def parse(self, value: Any) -> Any:
        return self.converter.parse_value(value)

    def record_value(self, data: Mapping[str, Any]) -> tuple[str, Any]:
        """Return `("ok", value)`, `("missing", None)` or `("unparseable", None)`."""
        record = Record(data=data, stream_name=self.name)
        try:
            raw = self.cursor.cursor_field.extract_value(record)
        except (ValueError, KeyError, TypeError):
            return "missing", None
        try:
            return "ok", self.parse(raw)
        except (ValueError, TypeError, OverflowError):
            return "unparseable", None

    def copy_key(self, data: Mapping[str, Any]) -> int:
        if self.primary_key:
            values: Any = []
            for path in self.primary_key:
                value: Any = data
                for part in path:
                    value = value.get(part) if isinstance(value, Mapping) else None
                values.append(value)
        else:
            values = data
        return hash(json.dumps(values, sort_keys=True, default=str))

    def bound(self, checkpoint: _Checkpoint, checkpoints: list[_Checkpoint], index: int) -> Any:
        """The resume start of a checkpoint, or the bound of an earlier one past the cap."""
        if checkpoint.has_bound:
            return checkpoint.bound
        state_key = json.dumps(checkpoint.state, sort_keys=True, default=str)
        if state_key not in self.bounds and len(self.bounds) < _MAX_BOUNDS_PER_STREAM:
            if isinstance(self.template, ConcurrentPerPartitionCursor):
                resumed = _sub_cursor(self.template, checkpoint.state)
            else:
                resumed = _cursor_with_state(self.template, checkpoint.state)
            self.bounds[state_key] = _resume_start(resumed)
        if state_key in self.bounds:
            checkpoint.bound, checkpoint.has_bound = self.bounds[state_key], True
            return checkpoint.bound
        for earlier in reversed(checkpoints[:index]):
            if earlier.has_bound and earlier.bound is not None:
                return earlier.bound
        return None

    def add_state(self, position: int, state: Mapping[str, Any]) -> None:
        self.final_state = dict(state)
        if self.partitioned:
            for entry in state.get("states") or []:
                if not isinstance(entry, Mapping):
                    continue
                partition, cursor_state = entry.get("partition"), entry.get("cursor")
                if isinstance(partition, Mapping) and isinstance(cursor_state, Mapping):
                    self._add_checkpoint(_partition_key(partition), position, cursor_state)
        else:
            self._add_checkpoint(None, position, state)

    def _add_checkpoint(
        self, partition: str | None, position: int, state: Mapping[str, Any]
    ) -> None:
        if self.cursor_key not in state:
            return
        try:
            value = self.parse(state[self.cursor_key])
        except (ValueError, TypeError, OverflowError):
            return
        checkpoints = self.checkpoints.setdefault(partition, [])
        # Bounds grow with the state, so only a checkpoint that moves the state forward can
        # raise the bound; the earliest checkpoint at a value is the one that matters.
        if not checkpoints or value > checkpoints[-1].value:
            checkpoints.append(_Checkpoint(position, dict(state), value))

    def add_record(self, position: int, data: Mapping[str, Any]) -> None:
        record_number = self.records
        self.records += 1
        status, value = self.record_value(data)
        if status == "missing":
            self.missing_values += 1
        elif status == "unparseable":
            self.unparseable_values += 1
        if status != "ok":
            return
        if not self.has_newest_value or value > self.newest_value:
            self.newest_value, self.has_newest_value = value, True
        if not self.check_checkpoints:
            return

        partition: str | None = None
        if self.partitioned:
            partitions = self.partitions or []
            if record_number >= len(partitions):
                return
            partition = partitions[record_number]
        copy_key = self.copy_key(data)
        self._check_record(partition, position, value, copy_key)
        self.copies.setdefault(copy_key, []).append((position, value))

    def _check_record(
        self, partition: str | None, position: int, value: Any, copy_key: int
    ) -> None:
        checkpoints = self.checkpoints.get(partition)
        if not checkpoints:
            return
        last = self.bound(checkpoints[-1], checkpoints, len(checkpoints) - 1)
        if last is None or not value < last:
            return
        # The earliest checkpoint whose resume would skip the record has the fewest copies
        # before it. Bounds grow along the checkpoints, so search for it.
        low, high = 0, len(checkpoints) - 1
        while low < high:
            middle = (low + high) // 2
            bound = self.bound(checkpoints[middle], checkpoints, middle)
            if bound is not None and value < bound:
                high = middle
            else:
                low = middle + 1
        checkpoint = checkpoints[low]
        if any(
            copy_position < checkpoint.position and copy_value >= value
            for copy_position, copy_value in self.copies.get(copy_key, ())
        ):
            return
        self.flagged_records += 1
        self.flagged_partitions.add(partition)
        self.flagged_checkpoints.add(checkpoint.position)


def _stream_list(names: Iterable[str]) -> str:
    ordered = sorted(names)
    shown = ", ".join(repr(name) for name in ordered[:_MAX_STREAMS_PER_PROBLEM])
    hidden = len(ordered) - _MAX_STREAMS_PER_PROBLEM
    return shown + (f" and {hidden} more" if hidden > 0 else "")


def _build_with_states(
    source: ConcurrentDeclarativeSource, states: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    """Build the named static streams of a source from the given states, without HTTP.

    Return the streams by name. A stream the source cannot build alone (a dynamic stream, or a
    source that overrides `streams()`) is missing from the result. Raise what the build raises.
    """
    constructor = source._constructor  # noqa: SLF001
    try:
        static_names = {
            stream_config.get("name")
            for stream_config in source._stream_configs(source._source_config)  # noqa: SLF001
        }
    except Exception:  # noqa: BLE001  # Without static streams there is nothing to rebuild.
        return {}
    names = [name for name in states if name in static_names]
    if not names:
        return {}
    previous = constructor._connector_state_manager  # noqa: SLF001
    constructor._connector_state_manager = ConnectorStateManager(  # noqa: SLF001
        state=[_state_message(name, states[name]) for name in names]
    )
    try:
        with _no_http():
            built = source._streams_by_name(names)  # noqa: SLF001
    finally:
        constructor._connector_state_manager = previous  # noqa: SLF001
    return {stream.name: stream for stream in built or []}


def _rebuild(
    source: ConcurrentDeclarativeSource,
    states: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Rebuild streams from their final states; return the streams and each build error."""
    errors: dict[str, str] = {}
    try:
        return _build_with_states(source, states), errors
    except Exception:  # noqa: BLE001  # Build the streams one by one to tell which one fails.
        pass
    rebuilt: dict[str, Any] = {}
    for name, state in states.items():
        try:
            rebuilt.update(_build_with_states(source, {name: state}))
        except _HttpRequestDuringBuild:
            errors[name] = "it sent an HTTP request"
        except Exception as error:  # noqa: BLE001  # Reported by type only: no values.
            errors[name] = f"it raised `{type(error).__name__}`"
    return rebuilt, errors


def find_cursor_state_problems(
    *,
    messages: Callable[[], Iterable[AirbyteMessage]],
    configured_catalog: ConfiguredAirbyteCatalog,
    recorder: CursorStateRecorder | None,
    read_seconds: float,
    reference_source: Callable[[], object] | None = None,
) -> dict[CursorStateCheck, list[str]]:
    """Run every cursor state check on a read; return the problems found per check.

    Args:
        messages: Returns the read's messages in output order; it is called twice.
        configured_catalog: The catalog the read ran with.
        recorder: The hooks installed on the source that ran the read, or `None` when the
            source is not a `ConcurrentDeclarativeSource` (nothing is checked then).
        read_seconds: How long the read took, which bounds the per-partition `lookback_window`.
        reference_source: Creates the source again, for suites whose `read` builds another
            source than the hooked one. Its streams give the cursors; the checks that need the
            hooks (per-partition checkpoints) are skipped.
    """
    if recorder is None:
        return {}
    problems: dict[CursorStateCheck, list[str]] = {}

    def _add(check: CursorStateCheck, problem: str) -> None:
        problems.setdefault(check, []).append(problem)

    source: object = recorder.source
    streams = recorder.streams
    global_at_start = recorder.global_cursor_at_start
    hooked = recorder.saw_read
    if not hooked:
        source = reference_source() if reference_source is not None else None
        if not isinstance(source, ConcurrentDeclarativeSource):
            return {}
        try:
            streams = {stream.name: stream for stream in source.streams(source._config)}  # noqa: SLF001
        except Exception as error:  # noqa: BLE001
            _add(
                CHECK_ERRORS,
                f"- building the source's streams failed with `{type(error).__name__}`; no "
                "cursor state was checked.",
            )
            return problems
        global_at_start = {
            name: stream.cursor._use_global_cursor  # noqa: SLF001
            for name, stream in streams.items()
            if isinstance(getattr(stream, "cursor", None), ConcurrentPerPartitionCursor)
        }
    assert isinstance(source, ConcurrentDeclarativeSource)

    configured = {
        configured_stream.stream.name: configured_stream
        for configured_stream in configured_catalog.streams
    }
    final_states: dict[str, dict[str, Any]] = {}
    for message in messages():
        parsed = _state_blob(message)
        if parsed is not None and parsed[0] in configured:
            final_states[parsed[0]] = parsed[1]

    checked: dict[str, _StreamCheck] = {}
    for name, configured_stream in configured.items():
        stream = streams.get(name)
        cursor = getattr(stream, "cursor", None)
        if type(cursor) is ConcurrentCursor:
            # Without `step` the stream's only state follows all of its records, so the
            # checkpoint check finds nothing there by construction.
            check_checkpoints = True
        elif isinstance(cursor, ConcurrentPerPartitionCursor):
            check_checkpoints = hooked and name in recorder.lifted_streams
        else:
            continue
        primary_key = [
            list(path)
            for path in (
                configured_stream.stream.source_defined_primary_key
                or configured_stream.primary_key
                or []
            )
        ]
        checked[name] = _StreamCheck(
            name=name,
            cursor=cursor,
            template=cursor,
            primary_key=primary_key,
            check_checkpoints=check_checkpoints,
            partitions=recorder.record_partitions.get(name) if hooked else None,
        )
    if not checked:
        return {}

    rebuilt, build_errors = _rebuild(
        source, {name: final_states[name] for name in checked if name in final_states}
    )
    for name, reason in sorted(build_errors.items()):
        _add(
            ROUND_TRIP,
            f"- stream {name!r}: building the source from the read's final state failed: "
            f"{reason}. A sync that resumes from this state would fail the same way.",
        )
    for name, stream_check in checked.items():
        resumed = getattr(rebuilt.get(name), "cursor", None)
        if isinstance(resumed, (ConcurrentCursor, ConcurrentPerPartitionCursor)):
            # A resumed sync builds the stream from its state: a `StateDelegatingStream` then
            # builds its `incremental_stream`, so its cursor sets the resume start.
            stream_check.template = resumed

    for position, message in enumerate(messages()):
        if message.type == Type.RECORD and message.record is not None:
            record_check = checked.get(message.record.stream)
            if record_check is not None:
                try:
                    record_check.add_record(position, message.record.data)
                except Exception as error:  # noqa: BLE001
                    _add(
                        CHECK_ERRORS,
                        f"- stream {record_check.name!r}: checking a record failed with "
                        f"`{type(error).__name__}`.",
                    )
                    del checked[record_check.name]
        elif message.type == Type.STATE:
            parsed = _state_blob(message)
            state_check = checked.get(parsed[0]) if parsed is not None else None
            if parsed is not None and state_check is not None:
                try:
                    state_check.add_state(position, parsed[1])
                except Exception as error:  # noqa: BLE001
                    _add(
                        CHECK_ERRORS,
                        f"- stream {state_check.name!r}: checking a state message failed with "
                        f"`{type(error).__name__}`.",
                    )
                    del checked[state_check.name]

    lookback_limit = math.ceil(read_seconds)
    for name in sorted(checked):
        stream_check = checked[name]
        try:
            _report_checkpoints(stream_check, _add)
            _report_cursor_values(stream_check, _add)
            if stream_check.partitioned:
                _report_partition_state(
                    stream_check,
                    rebuilt.get(name),
                    global_at_start.get(name, False),
                    lookback_limit,
                    _add,
                )
            else:
                _report_final_state(stream_check, _add)
        except Exception as error:  # noqa: BLE001
            _add(
                CHECK_ERRORS,
                f"- stream {name!r}: checking its final state failed with "
                f"`{type(error).__name__}`.",
            )
    return problems


def _report_checkpoints(
    stream_check: _StreamCheck, add: Callable[[CursorStateCheck, str], None]
) -> None:
    name = stream_check.name
    if (
        stream_check.partitioned
        and stream_check.check_checkpoints
        and len(stream_check.partitions or ()) != stream_check.records
    ):
        add(
            CHECK_ERRORS,
            f"- stream {name!r}: its records could not be matched to their partitions, so its "
            "checkpoints were not checked.",
        )
        return
    if not stream_check.flagged_records:
        return
    where = (
        f" in {len(stream_check.flagged_partitions)} partition(s)"
        if stream_check.partitioned
        else ""
    )
    add(
        CHECKPOINTS,
        f"- stream {name!r}: {stream_check.flagged_records} record(s){where} were emitted after "
        f"{len(stream_check.flagged_checkpoints)} state message(s) whose resume would start past "
        f"their `{stream_check.cursor_key}` value, and no copy of them (same primary key, cursor "
        "value at least as high) was emitted before. A sync that fails after such a state "
        "message and resumes from it never reads these records. This happens when records "
        "carry a cursor value past the end of the slice that returned them (the API filters on "
        "another field, or reads the window in another timezone).",
    )


def _report_cursor_values(
    stream_check: _StreamCheck, add: Callable[[CursorStateCheck, str], None]
) -> None:
    parts = []
    if stream_check.missing_values:
        parts.append(f"{stream_check.missing_values} have no `{stream_check.cursor_key}` value")
    if stream_check.unparseable_values:
        formats = getattr(stream_check.converter, "_input_datetime_formats", None) or []
        shown = ", ".join(f"`{fmt}`" for fmt in formats)
        parts.append(
            f"{stream_check.unparseable_values} have a `{stream_check.cursor_key}` value the "
            "cursor cannot parse" + (f" (formats {shown})" if shown else "")
        )
    if parts:
        add(
            CURSOR_VALUES,
            f"- stream {stream_check.name!r}: of {stream_check.records} records, "
            + " and ".join(parts)
            + ". The cursor does not advance on these records.",
        )


def _report_final_state(
    stream_check: _StreamCheck, add: Callable[[CursorStateCheck, str], None]
) -> None:
    final_state = stream_check.final_state
    if final_state is None or not stream_check.has_newest_value:
        return
    name, key = stream_check.name, stream_check.cursor_key
    if key not in final_state:
        add(
            FINAL_STATE,
            f"- stream {name!r}: the final state has no `{key}` value, although records carry one.",
        )
        return
    converter = stream_check.converter
    try:
        state_value = stream_check.parse(final_state[key])
    except (ValueError, TypeError, OverflowError):
        add(
            FINAL_STATE,
            f"- stream {name!r}: the cursor cannot parse the `{key}` value of the final state.",
        )
        return
    # State is written with the cursor's `datetime_format`, which can be coarser than the
    # records' values: compare at the state's precision.
    newest = stream_check.parse(converter.output_format(stream_check.newest_value))
    if state_value < newest:
        add(
            FINAL_STATE,
            f"- stream {name!r}: the final state is behind the newest `{key}` value among the "
            "stream's records, at the precision of the state's format. The next sync would read "
            "those records again.",
        )


def _report_partition_state(
    stream_check: _StreamCheck,
    rebuilt_stream: Any,
    global_at_start: bool,
    lookback_limit: int,
    add: Callable[[CursorStateCheck, str], None],
) -> None:
    name, key = stream_check.name, stream_check.cursor_key
    state = stream_check.final_state
    if state is None:
        return
    cursor = stream_check.cursor
    assert isinstance(cursor, ConcurrentPerPartitionCursor)

    use_global = state.get("use_global_cursor")
    entries = state.get("states")
    shape: list[str] = []
    if global_at_start and use_global is not True:
        shape.append(
            "the manifest sets `global_substream_cursor`, but `use_global_cursor` is not true"
        )
    if isinstance(entries, list):
        without_partition = sum(
            1
            for entry in entries
            if not isinstance(entry, Mapping) or not isinstance(entry.get("partition"), Mapping)
        )
        without_cursor = sum(
            1
            for entry in entries
            if not isinstance(entry, Mapping)
            or not isinstance(entry.get("cursor"), Mapping)
            or key not in entry["cursor"]
        )
        if without_partition:
            shape.append(
                f"{without_partition} of {len(entries)} `states` entries have no partition"
            )
        if without_cursor:
            shape.append(
                f"{without_cursor} of {len(entries)} `states` entries have no `cursor.{key}`"
            )
        key_sets = {
            tuple(sorted(entry["partition"]))
            for entry in entries
            if isinstance(entry, Mapping) and isinstance(entry.get("partition"), Mapping)
        }
        if len(key_sets) > 1:
            shown = "; ".join(", ".join(keys) for keys in sorted(key_sets)[:5])
            shape.append(f"`states` entries use different partition keys ({shown})")
    elif stream_check.has_newest_value:
        global_state = state.get("state")
        if not isinstance(global_state, Mapping) or key not in global_state:
            shape.append(f"there is no `states` list and the global `state` has no `{key}`")
    if "lookback_window" in state:
        lookback = state["lookback_window"]
        if (
            isinstance(lookback, bool)
            or not isinstance(lookback, int)
            or not 0 <= lookback <= lookback_limit
        ):
            shape.append(
                "`lookback_window` is not a whole number of seconds between 0 and the read's "
                f"duration ({lookback_limit} s)"
            )
    if shape:
        add(
            PARTITION_STATE_SHAPE,
            f"- stream {name!r}: in the final state, " + "; ".join(shape) + ".",
        )

    parents = _incremental_parents(cursor.partition_router)
    parent_state = state.get("parent_state")
    missing_parents = [
        parent
        for parent in parents
        if not isinstance(parent_state, Mapping) or parent not in parent_state
    ]
    if missing_parents:
        add(
            PARENT_STATE,
            f"- stream {name!r}: the final state has no `parent_state` for "
            f"{_stream_list(missing_parents)}, which the stream reads with "
            "`incremental_dependency: true`. The next sync would read those parents in full.",
        )

    router = cursor.partition_router
    if type(router) is ListPartitionRouter and isinstance(entries, list):
        listed = {_partition_key(stream_slice.partition) for stream_slice in router.stream_slices()}
        present = {
            _partition_key(entry["partition"])
            for entry in entries
            if isinstance(entry, Mapping) and isinstance(entry.get("partition"), Mapping)
        }
        missing = len(listed - present)
        if missing:
            add(
                PARTITION_COVERAGE,
                f"- stream {name!r}: {missing} of {len(listed)} partitions of its "
                "`ListPartitionRouter` have no `states` entry in the final state.",
            )

    resumed = getattr(rebuilt_stream, "cursor", None)
    if isinstance(resumed, ConcurrentPerPartitionCursor) and isinstance(entries, list):
        distinct = {
            _partition_key(entry["partition"])
            for entry in entries
            if isinstance(entry, Mapping)
            and isinstance(entry.get("partition"), Mapping)
            and isinstance(entry.get("cursor"), Mapping)
        }
        loaded = len(resumed._cursor_per_partition)  # noqa: SLF001
        if loaded != len(distinct):
            add(
                ROUND_TRIP,
                f"- stream {name!r}: built from its final state, the cursor holds {loaded} "
                f"partition(s), but the state has {len(distinct)} distinct `states` entries.",
            )


def format_cursor_state_problems(problems: Mapping[CursorStateCheck, Sequence[str]]) -> str:
    """Render problems as one block per check."""
    return "\n".join(
        f"{check.name}:\n" + "\n".join(f"  {line}" for line in lines)
        for check, lines in problems.items()
        if lines
    )
