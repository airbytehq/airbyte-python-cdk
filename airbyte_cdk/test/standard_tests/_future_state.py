# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""The future-state read: an incremental `read` that starts from a state far in the future.

This is CAT's `test_state_with_abnormally_large_values`. An `incremental` entry of
`acceptance-test-config.yml` names a state file (`future_state.future_state_path`) whose cursors are
far in the future. A read from it must succeed, return no records, and emit state for every stream
it reads. A connector that ignores the state it is given, or cannot parse the state its previous
version wrote, returns records or fails.

Failure messages name the state file, streams and fields, never record or state values: CI logs are
public.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

import orjson

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateMessageSerializer,
    AirbyteStateType,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.streams.concurrent.state_converters.datetime_stream_state_converter import (
    CustomFormatConcurrentStreamStateConverter,
    DateTimeStreamStateConverter,
    EpochValueConcurrentStreamStateConverter,
)
from airbyte_cdk.test.models import ConnectorTestScenario

STATE_MESSAGE_EXAMPLE = (
    '[{"type": "STREAM", "stream": {"stream_descriptor": {"name": "<stream>"}, '
    '"stream_state": {"<cursor field>": "<value far in the future>"}}}]'
)

_NOT_CURSOR_VALUES = frozenset({"parent_state", "partition"})
"""Keys of a declarative stream's state whose content is not the stream's own cursor value."""


def resolve_future_state_path(scenario: ConnectorTestScenario, connector_root: Path) -> Path:
    """The absolute path of the scenario's future state file."""
    assert scenario.future_state is not None and scenario.future_state.future_state_path
    path = scenario.future_state.future_state_path
    return path if path.is_absolute() else connector_root / path


def load_future_state(state_path: Path, *, display_path: Path) -> list[dict[str, Any]]:
    """Load a future state file: a JSON list of `STREAM` or `GLOBAL` state messages.

    Fails, naming `display_path`, when the file is missing, is not JSON, uses the legacy layout (a
    mapping of stream names to states), or holds a message that is not a valid `STREAM` or
    `GLOBAL` state message.
    """
    if not state_path.is_file():
        raise AssertionError(
            f"The future state file `{display_path}` does not exist. Fix "
            "`future_state.future_state_path` in `acceptance-test-config.yml`."
        )
    try:
        state = orjson.loads(state_path.read_bytes())
    except orjson.JSONDecodeError as error:
        raise AssertionError(
            f"The future state file `{display_path}` is not valid JSON "
            f"(line {error.lineno}, column {error.colno})."
        ) from None

    layout_hint = f"Write it as a list of state messages: `{STATE_MESSAGE_EXAMPLE}`."
    if isinstance(state, dict):
        raise AssertionError(
            f"The future state file `{display_path}` uses the legacy layout, a mapping of "
            f"stream names to states, which connectors no longer read. {layout_hint}"
        )
    if not isinstance(state, list) or not state:
        found = "an empty list" if isinstance(state, list) else f"a JSON {type(state).__name__}"
        raise AssertionError(f"The future state file `{display_path}` holds {found}. {layout_hint}")

    problems: list[str] = []
    for index, message in enumerate(state):
        where = f"message {index}"
        if not isinstance(message, dict):
            problems.append(f"{where} is a JSON {type(message).__name__}, not an object")
            continue
        try:
            parsed = AirbyteStateMessageSerializer.load(message)
        except Exception:  # noqa: BLE001  # The serializer's errors repeat the input.
            problems.append(f"{where} is not a valid state message")
            continue
        if parsed.type == AirbyteStateType.STREAM and parsed.stream is not None:
            continue
        if parsed.type == AirbyteStateType.GLOBAL and parsed.global_ is not None:
            continue
        problems.append(
            f"{where} has `type` {message.get('type')!r} and no matching `stream` or `global` "
            "object; only `STREAM` and `GLOBAL` state is read"
            if message.get("type") in {None, "STREAM", "GLOBAL", "LEGACY"}
            else f"{where} has an unknown `type`"
        )
    if problems:
        raise AssertionError(
            f"The future state file `{display_path}` is not a list of `STREAM` or `GLOBAL` "
            "state messages:\n"
            + "\n".join(f"- {problem}" for problem in problems)
            + f"\n{layout_hint}"
        )
    return state


def _streams_with_state(state: Iterable[Mapping[str, Any]]) -> set[str]:
    names: set[str] = set()
    for message in state:
        if message.get("type") == "STREAM":
            names.add(message["stream"]["stream_descriptor"]["name"])
        elif message.get("type") == "GLOBAL":
            for stream_state in message["global"].get("stream_states") or []:
                names.add(stream_state["stream_descriptor"]["name"])
    return names


def build_future_state_catalog(
    scenario: ConnectorTestScenario,
    read_streams: list[AirbyteStream],
    connector_root: Path,
) -> ConfiguredAirbyteCatalog:
    """The incremental catalog the future-state read of `scenario` reads.

    `read_streams` are the streams `get_read_streams` returns for the scenario: those of the
    entry's `configured_catalog_path` (or every discovered stream without one), minus
    `empty_streams`. The read keeps those that are incremental: configured `incremental` in the declared catalog, or supporting it in
    `discover` without one. The entry's `future_state.missing_streams` are left out. Each stream
    keeps the cursor field of the declared catalog, or its default cursor field.

    Fails when the selected streams hold no incremental stream: the entry has nothing to read.
    """
    declared: dict[str, Mapping[str, Any]] = {}
    if scenario.configured_catalog_path:
        catalog_path = scenario.configured_catalog_path
        if not catalog_path.is_absolute():
            catalog_path = connector_root / catalog_path
        # `get_read_streams` has failed, naming the catalog, when it does not exist.
        if catalog_path.is_file():
            for configured in orjson.loads(catalog_path.read_bytes())["streams"]:
                declared.setdefault(configured["stream"]["name"], configured)

    def is_incremental(stream: AirbyteStream) -> bool:
        if SyncMode.incremental not in (stream.supported_sync_modes or []):
            return False
        if not declared:
            return True
        return bool(declared[stream.name].get("sync_mode") == SyncMode.incremental.value)

    incremental = [stream for stream in read_streams if is_incremental(stream)]
    if not incremental:
        source = (
            f"its catalog `{scenario.configured_catalog_path}` configures"
            if scenario.configured_catalog_path
            else "`discover` reports"
        )
        raise AssertionError(
            f"The `incremental` entry for config '{scenario.id}' has no incremental stream to read "
            f"from its future state: {source} none outside its `empty_streams`. Remove the entry, "
            "or point it at a catalog with incremental streams."
        )

    assert scenario.future_state is not None
    missing = {stream.name for stream in scenario.future_state.missing_streams}
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=stream,
                sync_mode=SyncMode.incremental,
                destination_sync_mode=DestinationSyncMode.append,
                cursor_field=(
                    declared.get(stream.name, {}).get("cursor_field")
                    or stream.default_cursor_field
                    or None
                ),
            )
            for stream in incremental
            if stream.name not in missing
        ]
    )


def assert_future_state_covers_streams(
    state: list[dict[str, Any]],
    catalog: ConfiguredAirbyteCatalog,
    *,
    display_path: Path,
) -> None:
    """Fail when a stream the future-state read reads has no state in the file.

    A stream without state reads from its start date, so the read could not tell whether it
    honours state.
    """
    with_state = _streams_with_state(state)
    uncovered = [
        configured.stream.name
        for configured in catalog.streams
        if configured.stream.name not in with_state
    ]
    if uncovered:
        raise AssertionError(
            f"The future state file `{display_path}` has no state for the incremental stream(s) "
            f"{', '.join(repr(name) for name in uncovered)}. Add a state for each, with cursor "
            "values far in the future in the format the stream emits, or list the stream under "
            "`future_state.missing_streams` with a `bypass_reason`."
        )


def find_future_state_read_problems(
    records: Iterator[AirbyteMessage],
    state_messages: list[AirbyteMessage],
    catalog: ConfiguredAirbyteCatalog,
) -> list[str]:
    """One problem per stream that returned records or emitted no state, in catalog order."""
    record_counts = Counter(message.record.stream for message in records if message.record)
    stated = Counter[str]()
    for message in state_messages:
        state = message.state
        if state is None:
            continue
        if state.stream is not None:
            stated[state.stream.stream_descriptor.name] += 1
        elif state.global_ is not None:
            for stream_state in state.global_.stream_states or []:
                stated[stream_state.stream_descriptor.name] += 1

    problems: list[str] = []
    for configured in catalog.streams:
        name = configured.stream.name
        if record_counts[name]:
            problems.append(
                f"stream '{name}' returned {record_counts[name]} record(s): it ignored its state, "
                "could not parse it, or the state's cursor is not far enough in the future"
            )
        if not stated[name]:
            problems.append(f"stream '{name}' emitted no `STREAM` or `GLOBAL` state message")
    return problems


def _format_name(converter: DateTimeStreamStateConverter) -> str:
    if isinstance(converter, CustomFormatConcurrentStreamStateConverter):
        return f"`{converter._datetime_format}`"  # noqa: SLF001  # The declared `datetime_format`.
    if isinstance(converter, EpochValueConcurrentStreamStateConverter):
        return "epoch seconds"
    return "ISO 8601 with milliseconds (`%Y-%m-%dT%H:%M:%S.%fZ`)"


def _cursor_values(node: Any, key: str) -> Iterator[Any]:
    """The values under `key` in a stream state, outside its parent states and partition keys."""
    if isinstance(node, Mapping):
        for child_key, value in node.items():
            if child_key in _NOT_CURSOR_VALUES:
                continue
            if child_key == key:
                yield value
            else:
                yield from _cursor_values(value, key)
    elif isinstance(node, list):
        for item in node:
            yield from _cursor_values(item, key)


def _stream_states(state_messages: list[AirbyteMessage]) -> Iterator[tuple[str, Any]]:
    for message in state_messages:
        state = message.state
        if state is None:
            continue
        if state.stream is not None and state.stream.stream_state is not None:
            yield (
                state.stream.stream_descriptor.name,
                AirbyteStateMessageSerializer.dump(state)["stream"]["stream_state"],
            )
        elif state.global_ is not None:
            dumped = AirbyteStateMessageSerializer.dump(state)["global"]
            for stream_state in dumped.get("stream_states") or []:
                yield stream_state["stream_descriptor"]["name"], stream_state.get("stream_state")


def find_cursor_format_problems(
    source: object,
    catalog: ConfiguredAirbyteCatalog,
    state_messages: list[AirbyteMessage],
) -> list[str]:
    """One problem per stream whose emitted state holds a cursor value not in its declared format.

    Only declarative streams declare a format: the cursor's `datetime_format`, or the ISO 8601
    and epoch formats of the other datetime cursors. A value is in the format when the cursor
    writes it back unchanged after parsing it. Streams of other sources, and streams without a
    datetime cursor, are not checked.
    """
    if not isinstance(source, ConcurrentDeclarativeSource):
        return []

    wanted = {configured.stream.name for configured in catalog.streams}
    cursors: dict[str, tuple[str, DateTimeStreamStateConverter]] = {}
    for stream in source.streams(config={}):
        cursor = getattr(stream, "cursor", None)
        converter = getattr(cursor, "_connector_state_converter", None)
        cursor_field = getattr(cursor, "cursor_field", None)
        if stream.name in wanted and isinstance(converter, DateTimeStreamStateConverter):
            cursors[stream.name] = (cursor_field.cursor_field_key, converter)  # type: ignore[union-attr]

    bad: dict[str, set[str]] = {}
    for name, stream_state in _stream_states(state_messages):
        if name not in cursors:
            continue
        key, converter = cursors[name]
        for value in _cursor_values(stream_state, key):
            if value is None:
                continue
            try:
                in_format = converter.output_format(converter.parse_timestamp(value)) == value
            except Exception:  # noqa: BLE001  # A value the cursor cannot parse is not in format.
                in_format = False
            if not in_format:
                bad.setdefault(name, set()).add(key)

    return [
        f"stream '{name}': a `{key}` value in its state is not in the cursor's format "
        f"{_format_name(cursors[name][1])}"
        for name, keys in bad.items()
        for key in sorted(keys)
    ]


def assert_future_state_read(
    *,
    records: Iterator[AirbyteMessage],
    state_messages: list[AirbyteMessage],
    catalog: ConfiguredAirbyteCatalog,
    display_path: Path,
    cursor_format_problems: list[str] | None = None,
) -> None:
    """Fail with every problem of a read from the future state `display_path`, one per line."""
    problems = find_future_state_read_problems(records, state_messages, catalog)
    problems += cursor_format_problems or []
    if problems:
        raise AssertionError(
            f"The read from the future state `{display_path}` did not honour it:\n"
            + "\n".join(f"- {problem}" for problem in problems)
        )
