# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for the cursor state checks that `test_basic_read` runs on a declarative source's read.

Each test reads an inline manifest against a mocked API, then runs the checks on the output. A
check's failing case either reads from an API that breaks the cursor's assumptions or edits the
read's state messages the way a broken connector or CDK would emit them.
"""

from __future__ import annotations

import copy
import datetime as dt
import re
import warnings
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import requests_mock
import yaml

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.incremental.concurrent_partition_cursor import (
    ConcurrentPerPartitionCursor,
)
from airbyte_cdk.sources.declarative.partition_routers import SubstreamPartitionRouter
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor
from airbyte_cdk.test.entrypoint_wrapper import AirbyteEntrypointException, read
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.standard_tests import CursorStateWarning, DeclarativeSourceTestSuite
from airbyte_cdk.test.standard_tests._cursor_state_checks import (
    CHECK_ERRORS,
    CHECKPOINTS,
    CURSOR_VALUES,
    FINAL_STATE,
    PARENT_STATE,
    PARTITION_COVERAGE,
    PARTITION_STATE_SHAPE,
    ROUND_TRIP,
    CursorStateCheck,
    CursorStateRecorder,
    _no_http,
    _StreamCheck,
    find_cursor_state_problems,
    format_cursor_state_problems,
)
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG

CHECKS_MODULE = "airbyte_cdk.test.standard_tests._cursor_state_checks"

BASE = "https://api.test"
START = "2024-01-01T00:00:00Z"
END = dt.datetime(2024, 1, 31, tzinfo=dt.timezone.utc)
CONFIG = {"start_date": START}
FIRST_CREATED = dt.datetime(2024, 1, 1, 1, tzinfo=dt.timezone.utc)
ISO = "%Y-%m-%dT%H:%M:%SZ"


def _iso(value: dt.datetime) -> str:
    return value.strftime(ISO)


def _parse(value: str) -> dt.datetime:
    for fmt in (ISO, "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(value, fmt).replace(tzinfo=dt.timezone.utc)
        except ValueError:
            pass
    raise ValueError("unexpected date format")


# --- Manifest ---------------------------------------------------------------------------------


def _cursor(
    *,
    step: str | None = None,
    lookback: str | None = None,
    end_option: bool = True,
    datetime_format: str = ISO,
    record_formats: list[str] | None = None,
    global_cursor: bool = False,
    client_side: bool = False,
) -> dict[str, Any]:
    cursor: dict[str, Any] = {
        "type": "DatetimeBasedCursor",
        "cursor_field": "updated_at",
        "datetime_format": datetime_format,
        "start_datetime": "{{ config['start_date'] }}",
        "end_datetime": _iso(END),
        "start_time_option": {
            "type": "RequestOption",
            "inject_into": "request_parameter",
            "field_name": "since",
        },
    }
    if end_option:
        cursor["end_time_option"] = {
            "type": "RequestOption",
            "inject_into": "request_parameter",
            "field_name": "until",
        }
    if step:
        cursor["step"] = step
        cursor["cursor_granularity"] = "PT1S"
    if lookback:
        cursor["lookback_window"] = lookback
    if record_formats:
        cursor["cursor_datetime_formats"] = record_formats
    if global_cursor:
        cursor["global_substream_cursor"] = True
    if client_side:
        cursor["is_client_side_incremental"] = True
    return cursor


def _stream(
    name: str,
    path: str,
    cursor: Mapping[str, Any] | None = None,
    router: Mapping[str, Any] | None = None,
    primary_key: list[str] | None = None,
) -> dict[str, Any]:
    retriever: dict[str, Any] = {
        "type": "SimpleRetriever",
        "requester": {"type": "HttpRequester", "url_base": BASE, "path": path},
        "record_selector": {
            "type": "RecordSelector",
            "extractor": {"type": "DpathExtractor", "field_path": ["data"]},
        },
    }
    if router:
        retriever["partition_router"] = router
    stream: dict[str, Any] = {
        "type": "DeclarativeStream",
        "name": name,
        "primary_key": ["id"] if primary_key is None else primary_key,
        "schema_loader": {
            "type": "InlineSchemaLoader",
            "schema": {"type": "object", "properties": {}},
        },
        "retriever": retriever,
    }
    if cursor:
        stream["incremental_sync"] = dict(cursor)
    return stream


def _parents() -> dict[str, Any]:
    return _stream("parents", "/parents", _cursor())


def _children(
    cursor: Mapping[str, Any] | None = None,
    *,
    incremental_dependency: bool = True,
    group_size: int | None = None,
) -> dict[str, Any]:
    router: dict[str, Any] = {
        "type": "SubstreamPartitionRouter",
        "parent_stream_configs": [
            {
                "type": "ParentStreamConfig",
                "parent_key": "id",
                "partition_field": "parent_id",
                "incremental_dependency": incremental_dependency,
                "stream": _parents(),
            }
        ],
    }
    if group_size:
        router = {
            "type": "GroupingPartitionRouter",
            "group_size": group_size,
            "underlying_partition_router": router,
        }
    return _stream(
        "children",
        "/parents/{{ stream_partition.parent_id }}/children",
        cursor or _cursor(),
        router,
    )


def _regions() -> dict[str, Any]:
    router = {"type": "ListPartitionRouter", "values": ["eu", "us"], "cursor_field": "region"}
    return _stream("regions", "/regions/{{ stream_partition.region }}", _cursor(), router)


def _manifest(*streams: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "version": "6.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": [streams[0]["name"]]},
        "concurrency_level": {
            "type": "ConcurrencyLevel",
            "default_concurrency": 4,
            "max_concurrency": 4,
        },
        "streams": list(streams),
        "spec": {
            "type": "Spec",
            "connection_specification": {
                "type": "object",
                "properties": {"start_date": {"type": "string"}},
            },
        },
    }


# --- Mocked API -------------------------------------------------------------------------------


class _Api:
    """A fixed set of records, one every 6 hours, filtered on their creation time.

    - `late_days`: records created on a day divisible by 3 carry an `updated_at` that many days
      after their creation, so they land past the end of the slice that returns them.
    - `early_days`: records created on a day divisible by 3 carry an `updated_at` that many days
      before their creation, so they land before the start of the slice that returns them.
    - `future_first`: the first record carries an `updated_at` years after the cursor's end.
    - `ignore_until`: the API ignores `until`, so every slice returns everything up to the end.
    - `bad_values`: the first record has no `updated_at` and the second one is not a date.
    - `no_values`: no record has an `updated_at`.
    """

    def __init__(
        self,
        *,
        late_days: int = 0,
        early_days: int = 0,
        future_first: bool = False,
        ignore_until: bool = False,
        bad_values: bool = False,
        no_values: bool = False,
    ) -> None:
        self.late_days = late_days
        self.early_days = early_days
        self.future_first = future_first
        self.ignore_until = ignore_until
        self.bad_values = bad_values
        self.no_values = no_values

    def _records(self, request: Any, id_offset: int = 0) -> dict[str, Any]:
        query = parse_qs(urlparse(request.url).query)
        since = _parse(query["since"][0]) if "since" in query else None
        until = _parse(query["until"][0]) if "until" in query else None
        upper = END if self.ignore_until or until is None else until
        records = []
        created = FIRST_CREATED
        while created <= END:
            if (since is None or created >= since) and created <= upper:
                shift = self.late_days - self.early_days if created.day % 3 == 0 else 0
                updated = created + dt.timedelta(days=shift)
                if self.future_first and created == FIRST_CREATED:
                    updated = dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc)
                records.append(
                    {"id": id_offset + int(created.timestamp()), "updated_at": _iso(updated)}
                )
            created += dt.timedelta(hours=6)
        if self.bad_values and records and since is not None and since <= _parse(START):
            del records[0]["updated_at"]
            records[1]["updated_at"] = "not a date"
        if self.no_values:
            for record in records:
                del record["updated_at"]
        return {"data": records}

    def install(self, mocker: requests_mock.Mocker) -> None:
        mocker.get(
            re.compile(r"https://api\.test/parents(\?.*)?$"),
            json=lambda request, context: {
                "data": [{"id": i, "updated_at": "2024-01-02T00:00:00Z"} for i in (1, 2, 3)]
            },
        )
        mocker.get(
            re.compile(r"https://api\.test/parents/[^?]+/children.*"),
            json=lambda request, context: self._records(
                request, sum(map(int, re.findall(r"\d+", request.path.split("/")[2]))) * 10**10
            ),
        )
        mocker.get(
            re.compile(r"https://api\.test/(events|regions/).*"),
            json=lambda request, context: self._records(request),
        )


# --- Running the checks -----------------------------------------------------------------------


def _catalog(names: Iterable[str], *, primary_key: bool = True) -> ConfiguredAirbyteCatalog:
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=name,
                    json_schema={},
                    supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
                    source_defined_primary_key=[["id"]] if primary_key else None,
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append_dedup,
            )
            for name in names
        ]
    )


def _source(manifest: Mapping[str, Any]) -> ConcurrentDeclarativeSource:
    return ConcurrentDeclarativeSource(
        source_config=dict(manifest), config=CONFIG, catalog=None, state=None
    )


class _Read:
    def __init__(
        self,
        manifest: Mapping[str, Any],
        api: _Api,
        names: Iterable[str] | None = None,
        *,
        primary_key: bool = True,
    ) -> None:
        self.manifest = manifest
        self.names = list(names or [stream["name"] for stream in manifest["streams"]])
        self.catalog = _catalog(self.names, primary_key=primary_key)
        source = _source(manifest)
        recorder = CursorStateRecorder.install(source)
        assert recorder is not None
        self.recorder = recorder
        with requests_mock.Mocker() as mocker:
            api.install(mocker)
            output = read(source, CONFIG, self.catalog)
        recorder.uninstall()
        assert not output.errors
        self.messages = list(output.get_message_iterator())

    def problems(
        self,
        messages: list[AirbyteMessage] | None = None,
        *,
        read_seconds: float = 60,
        recorder: CursorStateRecorder | None = None,
        reference_source: Callable[[], object] | None = None,
    ) -> dict[CursorStateCheck, list[str]]:
        found = messages if messages is not None else self.messages
        # The checks run after the read, so they must not reach the API.
        with _no_http():
            return find_cursor_state_problems(
                messages=lambda: iter(found),
                configured_catalog=self.catalog,
                recorder=recorder or self.recorder,
                read_seconds=read_seconds,
                reference_source=reference_source,
            )

    def edited_problems(self, messages: list[AirbyteMessage]) -> dict[CursorStateCheck, list[str]]:
        """Run the checks on edited messages. Their records come from no slice of the read, so
        each one counts from its own value."""
        counts = Counter(
            message.record.stream
            for message in messages
            if message.type == Type.RECORD and message.record is not None
        )
        recorder = copy.copy(self.recorder)
        recorder.record_slices = {
            name: [(None, None)] * counts[name] for name in self.recorder.record_slices
        }
        return self.problems(messages, recorder=recorder)

    def states(self, stream: str) -> list[AirbyteMessage]:
        return [
            message
            for message in self.messages
            if message.type == Type.STATE
            and message.state is not None
            and message.state.stream is not None
            and message.state.stream.stream_descriptor.name == stream
        ]

    def with_final_state(
        self, stream: str, edit: Callable[[dict[str, Any]], None]
    ) -> list[AirbyteMessage]:
        """The read's messages, with `edit` applied to a copy of the stream's final state."""
        final = self.states(stream)[-1]
        messages = list(self.messages)
        edited = copy.deepcopy(final)
        assert edited.state is not None and edited.state.stream is not None
        state = copy.deepcopy(edited.state.stream.stream_state.__dict__)
        edit(state)
        edited.state.stream.stream_state = AirbyteStateBlob(**state)
        # Equal state messages can repeat; the final one is the last of them.
        messages[len(messages) - 1 - messages[::-1].index(final)] = edited
        return messages


def _text(problems: Mapping[CursorStateCheck, list[str]]) -> str:
    return format_cursor_state_problems(problems)


def _assert_no_values(problems: Mapping[CursorStateCheck, list[str]]) -> None:
    """Problems name streams, fields and counts, never record, cursor or partition values."""
    text = _text(problems)
    assert "2024" not in text
    assert not re.search(r"\d{9,}", text), "a record id leaked into the problems"


def test_with_final_state_edits_the_last_state_message() -> None:
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api(late_days=5))
    final = result.states("events")[-1]
    # Concurrent reads can emit equal state messages; repeat the final one.
    result.messages.insert(result.messages.index(final), copy.deepcopy(final))
    messages = result.with_final_state(
        "events", lambda state: state.update(updated_at="2024-01-02T00:00:00Z")
    )
    assert FINAL_STATE in result.problems(messages)


# --- Checkpoint check -------------------------------------------------------------------------


def test_checkpoint_check_flags_records_past_their_slice() -> None:
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api(late_days=5))
    problems = result.problems()
    assert list(problems) == [CHECKPOINTS]
    assert "stream 'events'" in problems[CHECKPOINTS][0]
    _assert_no_values(problems)


def test_checkpoint_check_passes_a_correct_api() -> None:
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api())
    assert result.problems() == {}


@pytest.mark.parametrize("late_days", [0, 5])
def test_a_future_dated_record_does_not_silence_the_checkpoint_check(late_days: int) -> None:
    # The future value moves the state past the cursor's end, so a cursor built from that
    # state has no slice. A sync resumed from it reads nothing until its end passes the state,
    # then starts there: it never reads the records emitted after that state.
    result = _Read(
        _manifest(_stream("events", "/events", _cursor(step="P1D"))),
        _Api(late_days=late_days, future_first=True),
    )
    problems = result.problems()
    assert list(problems) == [CHECKPOINTS]
    assert "stream 'events'" in problems[CHECKPOINTS][0]
    _assert_no_values(problems)


def test_a_resume_past_the_end_starts_a_lookback_window_before_the_state() -> None:
    # A state past the cursor's end generates no slice; once the end passes it, the resumed
    # sync starts a lookback window before it and reads the first record again.
    result = _Read(
        _manifest(_stream("events", "/events", _cursor(step="P1D", lookback="P7D"))), _Api()
    )
    state = result.states("events")[0]
    record = next(message for message in result.messages if message.type == Type.RECORD)
    other = copy.deepcopy(record)
    assert other.record is not None
    other.record.data = {**other.record.data, "id": -1}
    messages = [
        _state(state, "2030-01-01T00:00:00Z"),
        _record(record, "2029-12-28T00:00:00Z"),
        _record(other, "2029-12-20T00:00:00Z"),
    ]
    assert "'events': 1 record(s)" in result.edited_problems(messages)[CHECKPOINTS][0]


def test_records_below_their_slice_start_are_not_flagged() -> None:
    # The API returns records whose cursor value lies before the window that returned them. A
    # resumed sync requests those windows again, so it reads them again.
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api(early_days=2))
    assert result.problems() == {}


def test_records_below_their_slice_start_are_flagged_under_client_side_incremental() -> None:
    # The resumed sync drops records below its start by value, whatever window returns them.
    result = _Read(
        _manifest(_stream("events", "/events", _cursor(step="P1D", client_side=True))),
        _Api(early_days=2),
    )
    problems = result.problems()
    assert list(problems) == [CHECKPOINTS]
    assert "stream 'events'" in problems[CHECKPOINTS][0]


def test_records_that_do_not_match_the_read_skip_the_checkpoint_check() -> None:
    # Messages that are not the read's output cannot be matched to the read's slices. Checking
    # each record from its own value would flag the records below their slice start, which a
    # resume reads again.
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api(early_days=2))
    record = next(message for message in result.messages if message.type == Type.RECORD)
    problems = result.problems([*result.messages, record])
    assert problems == {
        CHECK_ERRORS: [
            "- stream 'events': its records could not be matched to the read's slices, so its "
            "checkpoints were not checked."
        ]
    }


def test_client_side_incremental_comes_from_the_incremental_stream_of_a_state_delegating_stream() -> (
    None
):
    stream = {
        "type": "StateDelegatingStream",
        "name": "events",
        "full_refresh_stream": _stream("events", "/events", _cursor(step="P1D")),
        "incremental_stream": _stream("events", "/events", _cursor(step="P1D", client_side=True)),
    }
    assert list(_Read(_manifest(stream), _Api(early_days=2)).problems()) == [CHECKPOINTS]


def test_per_partition_records_below_their_slice_start_are_not_flagged() -> None:
    result = _Read(
        _manifest(_children(_cursor(step="P3D"))), _Api(early_days=4), names=["children"]
    )
    assert result.problems() == {}


@pytest.mark.parametrize(
    "cursor, api",
    [
        pytest.param(_cursor(step="P1D"), _Api(ignore_until=True), id="api_ignores_until"),
        pytest.param(_cursor(step="P1D", end_option=False), _Api(), id="no_end_option"),
    ],
)
def test_checkpoint_check_does_not_flag_overlapping_slices(
    cursor: dict[str, Any], api: _Api
) -> None:
    # Every later slice repeats records already emitted before the checkpoint: duplicates,
    # not data loss.
    result = _Read(_manifest(_stream("events", "/events", cursor)), api)
    assert result.problems() == {}


def test_checkpoint_bound_comes_from_the_resumed_cursor() -> None:
    # A lookback window longer than the late updates makes a resume read them again, so they
    # are not lost; comparing with the raw state value would flag them.
    result = _Read(
        _manifest(_stream("events", "/events", _cursor(step="P1D", lookback="P7D"))),
        _Api(late_days=5),
    )
    assert result.problems() == {}


def test_checkpoint_check_flags_a_per_partition_stream_with_step() -> None:
    result = _Read(_manifest(_children(_cursor(step="P3D"))), _Api(late_days=5), names=["children"])
    assert result.recorder.lifted_streams == {"children"}
    problems = result.problems()
    assert list(problems) == [CHECKPOINTS]
    assert "stream 'children'" in problems[CHECKPOINTS][0]
    assert "partition(s)" in problems[CHECKPOINTS][0]
    _assert_no_values(problems)


def test_checkpoint_check_does_not_flag_a_per_partition_stream_without_step() -> None:
    # Without `step` each partition is one slice, whose state follows all of its records.
    result = _Read(_manifest(_children()), _Api(late_days=5), names=["children"])
    assert result.recorder.lifted_streams == set()
    assert result.problems() == {}


def test_throttle_lift_emits_more_per_partition_states_within_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _manifest(_children(_cursor(step="P3D")))
    lifted = _Read(manifest, _Api(), names=["children"])
    monkeypatch.setattr(CursorStateRecorder, "MAX_LIFTED_CLOSES", 0)
    throttled = _Read(manifest, _Api(), names=["children"])
    # Without the lift: the first close and the final state.
    assert len(throttled.states("children")) <= 2
    assert len(lifted.states("children")) > 10
    monkeypatch.setattr(CursorStateRecorder, "MAX_LIFTED_CLOSES", 5)
    budgeted = _Read(manifest, _Api(), names=["children"])
    assert len(budgeted.states("children")) <= 5 + 2
    # Lifting the throttle changes only the number of state messages.
    lifted_final = lifted.states("children")[-1].state.stream.stream_state.__dict__  # type: ignore[union-attr]
    throttled_final = throttled.states("children")[-1].state.stream.stream_state.__dict__  # type: ignore[union-attr]
    lifted_final.pop("lookback_window")
    throttled_final.pop("lookback_window")
    assert lifted_final == throttled_final
    assert _record_ids(lifted) == _record_ids(throttled)


def test_lifted_closes_thin_out_after_the_first_ones(monkeypatch: pytest.MonkeyPatch) -> None:
    # 3 partitions of 10 slices: 30 closes. With 2 first closes and a gap that doubles, the
    # lift covers closes 1, 2, 3, 4, 8 and 16.
    monkeypatch.setattr(CursorStateRecorder, "FIRST_LIFTED_CLOSES", 2)
    monkeypatch.setattr(CursorStateRecorder, "LIFTED_CLOSES_GROWTH", 2.0)
    result = _Read(_manifest(_children(_cursor(step="P3D"))), _Api(), names=["children"])
    assert 5 <= len(result.states("children")) <= 7


def _record_ids(result: _Read) -> list[Any]:
    return sorted(
        message.record.data["id"]
        for message in result.messages
        if message.type == Type.RECORD and message.record is not None
    )


def test_suites_whose_read_builds_another_source_skip_the_checkpoint_check() -> None:
    manifest = _manifest(
        _stream("events", "/events", _cursor(step="P1D")), _children(_cursor(step="P3D"))
    )
    result = _Read(manifest, _Api(late_days=5))
    # The hooks sit on a source that never reads, as in a suite that replaces `read`.
    idle = CursorStateRecorder.install(_source(manifest))
    assert idle is not None
    problems = result.problems(recorder=idle, reference_source=lambda: _source(manifest))
    assert problems == {
        CHECK_ERRORS: [
            "- stream 'events': the read ran on another instance of the source, so its slices "
            "were not recorded and its checkpoints were not checked."
        ]
    }
    # The other checks still run on the reference source's cursors.
    messages = result.with_final_state(
        "events", lambda state: state.update(updated_at="2024-01-02T00:00:00Z")
    )
    edited = result.problems(messages, recorder=idle, reference_source=lambda: _source(manifest))
    assert set(edited) == {CHECK_ERRORS, FINAL_STATE}


def test_unmatched_partitions_skip_the_per_partition_checkpoint_check() -> None:
    result = _Read(_manifest(_children(_cursor(step="P3D"))), _Api(late_days=5), names=["children"])
    result.recorder.record_slices["children"].pop()
    problems = result.problems()
    assert list(problems) == [CHECK_ERRORS]
    assert problems[CHECK_ERRORS] == [
        "- stream 'children': its records could not be matched to the read's slices, so its "
        "checkpoints were not checked."
    ]
    assert not CHECK_ERRORS.enforced


def test_checkpoint_check_compares_whole_records_without_a_primary_key() -> None:
    late = _Read(
        _manifest(_stream("events", "/events", _cursor(step="P1D"), primary_key=[])),
        _Api(late_days=5),
        primary_key=False,
    )
    assert list(late.problems()) == [CHECKPOINTS]
    overlapping = _Read(
        _manifest(_stream("events", "/events", _cursor(step="P1D"), primary_key=[])),
        _Api(ignore_until=True),
        primary_key=False,
    )
    assert overlapping.problems() == {}


def test_checkpoint_bound_of_a_state_delegating_stream_comes_from_its_incremental_stream() -> None:
    # A resumed sync has state, so it builds the incremental stream, whose lookback window
    # reads the late records again. The full-refresh stream that ran the read has none.
    stream = {
        "type": "StateDelegatingStream",
        "name": "events",
        "full_refresh_stream": _stream("events", "/events", _cursor(step="P1D")),
        "incremental_stream": _stream("events", "/events", _cursor(step="P1D", lookback="P7D")),
    }
    assert _Read(_manifest(stream), _Api(late_days=5)).problems() == {}
    stream["incremental_stream"] = _stream("events", "/events", _cursor(step="P1D"))
    assert list(_Read(_manifest(stream), _Api(late_days=5)).problems()) == [CHECKPOINTS]


def _delegating(definition: dict[str, Any], retention: str | None) -> dict[str, Any]:
    stream = {
        "type": "StateDelegatingStream",
        "name": definition["name"],
        "full_refresh_stream": definition,
        "incremental_stream": copy.deepcopy(definition),
    }
    if retention:
        stream["api_retention_period"] = retention
    return stream


@pytest.mark.parametrize(
    "definition, names",
    [
        pytest.param(_stream("events", "/events", _cursor(step="P1D")), None, id="concurrent"),
        pytest.param(_children(_cursor(step="P3D")), ["children"], id="per_partition"),
    ],
)
@pytest.mark.parametrize(
    "retention, expected",
    [
        # The read's states are older than the period, so a resume from any of them reruns
        # `full_refresh_stream` without state and reads the late records again. The final
        # state is past the period too, so the source rebuilt from it holds no state.
        pytest.param("P30D", [], id="states_past_retention"),
        # A resume reads from the state with `incremental_stream` and skips the late records.
        pytest.param(None, [CHECKPOINTS], id="no_retention"),
    ],
)
def test_checkpoint_bound_of_a_state_delegating_stream_follows_its_retention_period(
    definition: dict[str, Any],
    names: list[str] | None,
    retention: str,
    expected: list[CursorStateCheck],
) -> None:
    result = _Read(_manifest(_delegating(definition, retention)), _Api(late_days=5), names=names)
    assert list(result.problems()) == expected


def test_states_within_the_retention_period_resume_incrementally() -> None:
    stream = _delegating(_stream("events", "/events", _cursor(step="P1D")), "P100000D")
    assert list(_Read(_manifest(stream), _Api(late_days=5)).problems()) == [CHECKPOINTS]


def test_per_partition_state_delegating_bound_comes_from_its_incremental_stream() -> None:
    # A resumed sync builds `incremental_stream`, whose lookback window reads the late records again.
    stream = _delegating(_children(_cursor(step="P3D")), None)
    stream["incremental_stream"] = _children(_cursor(step="P3D", lookback="P7D"))
    assert _Read(_manifest(stream), _Api(late_days=5), names=["children"]).problems() == {}


def test_rebuilding_a_state_delegating_stream_emits_nothing_on_the_read_queue() -> None:
    # Past the retention period, each build emits a cleared state. Nothing drains the read's
    # bounded queue after the read.
    stream = _delegating(_stream("events", "/events", _cursor(step="P1D")), "P30D")
    result = _Read(_manifest(stream), _Api(late_days=5))
    queue = result.recorder.source._constructor._message_repository._queue  # type: ignore[attr-defined]  # noqa: SLF001
    queued = queue.qsize()
    assert result.problems() == {}
    assert queue.qsize() == queued


def test_a_resume_without_a_concurrent_cursor_reads_in_full(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A stream without a concurrent cursor ignores the state and reads everything.
    stream = _delegating(_stream("events", "/events", _cursor(step="P1D")), None)
    result = _Read(_manifest(stream), _Api(late_days=5))
    assert list(result.problems()) == [CHECKPOINTS]
    monkeypatch.setattr(
        f"{CHECKS_MODULE}._resumed_cursor", lambda source, name, state: (object(), False)
    )
    assert result.problems() == {}


def test_equal_partition_states_in_different_state_messages_resume_apart() -> None:
    # Two partitions hold the same cursor state, in state messages that select different
    # streams: past the retention period the first one resumes in full, the second does not.
    result = _Read(
        _manifest(_delegating(_children(_cursor(step="P3D")), None)),
        _Api(),
        names=["children"],
    )
    cursor = result.recorder.streams["children"].cursor
    stream_check = _StreamCheck(
        name="children",
        cursor=cursor,
        primary_key=[["id"]],
        check_checkpoints=True,
        resume_build=lambda state: (cursor, bool(state.get("past_retention"))),
    )
    partition_state = {"updated_at": "2024-01-10T00:00:00Z"}
    for position, (parent_id, past_retention) in enumerate([(1, True), (2, False)]):
        entry = {
            "partition": {"parent_id": parent_id, "parent_slice": {}},
            "cursor": partition_state,
        }
        stream_check.add_state(position, {"states": [entry], "past_retention": past_retention})
    first, second = (checkpoints[0] for checkpoints in stream_check.checkpoints.values())
    assert stream_check.bound(first, [first], 0) is None
    assert stream_check.bound(second, [second], 0) is not None


def test_rebuilds_of_a_state_delegating_stream_past_the_cap_never_flag_more(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _delegating(_stream("events", "/events", _cursor(step="P1D")), "P100000D")
    result = _Read(_manifest(stream), _Api(late_days=5))
    monkeypatch.setattr(f"{CHECKS_MODULE}._MAX_REBUILDS_PER_STREAM", 0)
    assert result.problems() == {}


def _state(message: AirbyteMessage, updated_at: str) -> AirbyteMessage:
    edited = copy.deepcopy(message)
    assert edited.state is not None and edited.state.stream is not None
    edited.state.stream.stream_state = AirbyteStateBlob(updated_at=updated_at)
    return edited


def _record(message: AirbyteMessage, updated_at: str) -> AirbyteMessage:
    edited = copy.deepcopy(message)
    assert edited.record is not None
    edited.record.data = {**edited.record.data, "updated_at": updated_at}
    return edited


def test_a_state_that_moves_back_does_not_lower_the_bound() -> None:
    # A resume from the first state skips both copies of the record: the second copy comes
    # after a lower state, but the first state message was already emitted before it.
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api())
    state = result.states("events")[0]
    record = next(message for message in result.messages if message.type == Type.RECORD)
    messages = [
        _state(state, "2024-01-20T00:00:00Z"),
        _record(record, "2024-01-10T00:00:00Z"),
        _state(state, "2024-01-05T00:00:00Z"),
        _record(record, "2024-01-10T00:00:00Z"),
    ]
    problems = result.edited_problems(messages)
    assert "'events': 2 record(s)" in problems[CHECKPOINTS][0]


def test_a_copy_after_the_first_skipping_state_does_not_exempt_a_record() -> None:
    # A resume from the first state skips the record, and its only earlier copy came after
    # that state, so the copy does not help.
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api())
    state = result.states("events")[0]
    record = next(message for message in result.messages if message.type == Type.RECORD)
    messages = [
        _state(state, "2024-01-20T00:00:00Z"),
        _record(record, "2024-01-10T00:00:00Z"),
        _state(state, "2024-01-25T00:00:00Z"),
        _record(record, "2024-01-10T00:00:00Z"),
    ]
    assert "'events': 2 record(s)" in result.edited_problems(messages)[CHECKPOINTS][0]


def test_a_record_at_the_resume_start_is_not_flagged() -> None:
    # A resume starts at the state value itself, so it reads a record at that value again.
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api())
    state = result.states("events")[0]
    record = next(message for message in result.messages if message.type == Type.RECORD)
    messages = [_state(state, "2024-01-20T00:00:00Z"), _record(record, "2024-01-20T00:00:00Z")]
    assert result.edited_problems(messages) == {}
    messages = [_state(state, "2024-01-20T00:00:00Z"), _record(record, "2024-01-19T23:59:59Z")]
    assert list(result.edited_problems(messages)) == [CHECKPOINTS]


def test_bounds_past_the_cap_use_the_bound_of_an_earlier_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api())
    state = result.states("events")[0]
    record = next(message for message in result.messages if message.type == Type.RECORD)
    other = copy.deepcopy(record)
    assert other.record is not None
    other.record.data = {**other.record.data, "id": -1}
    messages = [
        _state(state, "2024-01-20T00:00:00Z"),
        _record(record, "2024-01-03T00:00:00Z"),
        _state(state, "2024-01-25T00:00:00Z"),
        _record(other, "2024-01-10T00:00:00Z"),
    ]
    monkeypatch.setattr(f"{CHECKS_MODULE}._MAX_BOUNDS_PER_STREAM", 1)
    # The second state gets no bound of its own; the bound of the first one still flags the
    # second record.
    assert "'events': 2 record(s)" in result.edited_problems(messages)[CHECKPOINTS][0]


@pytest.mark.parametrize("retention", [None, "P100000D"], ids=["plain", "state_delegating"])
def test_bounds_past_the_cap_never_flag_more(
    monkeypatch: pytest.MonkeyPatch, retention: str | None
) -> None:
    stream = _stream("events", "/events", _cursor(step="P1D"))
    if retention:
        stream = _delegating(stream, retention)
    result = _Read(_manifest(stream), _Api(late_days=5))
    monkeypatch.setattr(f"{CHECKS_MODULE}._MAX_BOUNDS_PER_STREAM", 0)
    assert result.problems() == {}


# --- Final-state check ------------------------------------------------------------------------


def test_final_state_behind_the_newest_record_fails() -> None:
    result = _Read(_manifest(_stream("events", "/events", _cursor())), _Api())
    messages = result.with_final_state(
        "events", lambda state: state.update(updated_at="2024-01-02T00:00:00Z")
    )
    problems = result.problems(messages)
    assert list(problems) == [FINAL_STATE]
    _assert_no_values(problems)


def test_final_state_without_the_cursor_field_fails() -> None:
    result = _Read(_manifest(_stream("events", "/events", _cursor())), _Api())
    messages = result.with_final_state("events", lambda state: state.pop("updated_at"))
    assert list(result.problems(messages)) == [FINAL_STATE]


def test_final_state_the_cursor_cannot_parse_fails() -> None:
    result = _Read(_manifest(_stream("events", "/events", _cursor())), _Api())
    messages = result.with_final_state("events", lambda state: state.update(updated_at="x"))
    problems = result.problems(messages)
    assert FINAL_STATE in problems
    assert "cannot parse the `updated_at` value" in problems[FINAL_STATE][0]


def test_final_state_is_compared_at_the_state_precision() -> None:
    # The state is written per day while records carry a time: it sits behind on purpose.
    cursor = _cursor(datetime_format="%Y-%m-%d", record_formats=[ISO])
    for key, value in (
        ("start_datetime", "{{ config['start_date'] }}"),
        ("end_datetime", _iso(END)),
    ):
        cursor[key] = {"type": "MinMaxDatetime", "datetime": value, "datetime_format": ISO}
    result = _Read(_manifest(_stream("events", "/events", cursor)), _Api())
    assert result.problems() == {}


def test_final_state_check_skips_per_partition_streams() -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    messages = result.with_final_state(
        "children", lambda state: state.update(state={"updated_at": "2024-01-02T00:00:00Z"})
    )
    assert result.problems(messages) == {}


# --- Cursor-value check (advisory) ------------------------------------------------------------


def test_missing_and_unparseable_cursor_values_are_advisory() -> None:
    result = _Read(_manifest(_stream("events", "/events", _cursor())), _Api(bad_values=True))
    problems = result.problems()
    assert list(problems) == [CURSOR_VALUES]
    assert not CURSOR_VALUES.enforced
    assert "1 have no `updated_at` value" in problems[CURSOR_VALUES][0]
    assert "1 have a `updated_at` value the cursor cannot parse" in problems[CURSOR_VALUES][0]
    _assert_no_values(problems)


# --- Per-partition state shape ----------------------------------------------------------------


def _drop_first_cursor(state: dict[str, Any]) -> None:
    del state["states"][0]["cursor"]


def _rename_first_partition_key(state: dict[str, Any]) -> None:
    state["states"][0]["partition"] = {"other": 1}


@pytest.mark.parametrize(
    "edit, expected",
    [
        pytest.param(_drop_first_cursor, "have no `cursor.updated_at`", id="entry_without_cursor"),
        pytest.param(
            lambda state: state["states"][0].pop("partition"),
            "have no partition",
            id="entry_without_partition",
        ),
        pytest.param(
            _rename_first_partition_key,
            "use different partition keys (other; parent_id, parent_slice)",
            id="different_partition_keys",
        ),
        pytest.param(
            lambda state: state.update(lookback_window=3600),
            "`lookback_window` is not",
            id="lookback_longer_than_the_read",
        ),
        pytest.param(
            lambda state: state.update(lookback_window=-1),
            "`lookback_window` is not",
            id="negative",
        ),
        pytest.param(
            lambda state: state.update(lookback_window="1"), "`lookback_window` is not", id="string"
        ),
        pytest.param(
            lambda state: state.update(lookback_window=True), "`lookback_window` is not", id="bool"
        ),
    ],
)
def test_malformed_per_partition_state_fails(
    edit: Callable[[dict[str, Any]], None], expected: str
) -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    problems = result.problems(result.with_final_state("children", edit))
    assert PARTITION_STATE_SHAPE in problems
    assert expected in problems[PARTITION_STATE_SHAPE][0]
    _assert_no_values({PARTITION_STATE_SHAPE: problems[PARTITION_STATE_SHAPE]})


def _two_parent_children() -> dict[str, Any]:
    children = _children()
    router = children["retriever"]["partition_router"]
    second = copy.deepcopy(router["parent_stream_configs"][0])
    second["partition_field"] = "other_id"
    router["parent_stream_configs"].append(second)
    for parent in router["parent_stream_configs"]:
        parent["incremental_dependency"] = False
    children["retriever"]["requester"]["path"] = (
        "/parents/{{ stream_partition.parent_id or stream_partition.other_id }}/children"
    )
    return children


def _drop_first_parent_slice(state: dict[str, Any]) -> None:
    state["states"][0]["partition"].pop("parent_slice")


@pytest.mark.parametrize(
    "edit, expected",
    [
        # Each parent names its partitions with its own `partition_field`.
        pytest.param(None, None, id="one_key_set_per_parent"),
        pytest.param(
            _drop_first_parent_slice,
            "use different partition keys",
            id="different_keys_for_one_parent",
        ),
        pytest.param(
            _rename_first_partition_key, "use different partition keys", id="no_parent_field"
        ),
    ],
)
def test_partition_keys_are_compared_per_parent(
    edit: Callable[[dict[str, Any]], None] | None, expected: str | None
) -> None:
    result = _Read(_manifest(_two_parent_children()), _Api(), names=["children"])
    final = result.states("children")[-1].state.stream.stream_state.__dict__  # type: ignore[union-attr]
    assert {tuple(sorted(entry["partition"])) for entry in final["states"]} == {
        ("other_id", "parent_slice"),
        ("parent_id", "parent_slice"),
    }
    problems = result.problems(result.with_final_state("children", edit) if edit else None)
    if expected is None:
        assert problems == {}
    else:
        assert expected in problems[PARTITION_STATE_SHAPE][0]


def test_partition_entry_whose_cursor_lacks_the_cursor_field_fails() -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    messages = result.with_final_state(
        "children", lambda state: state["states"][0].update(cursor={"other": 1})
    )
    assert "have no `cursor.updated_at`" in result.problems(messages)[PARTITION_STATE_SHAPE][0]


def test_a_single_empty_partition_is_well_formed() -> None:
    # A router can yield one empty partition (for example source-jira's
    # `SubstreamOrSinglePartitionRouter` without projects); the CDK stores it as `{}`.
    result = _Read(_manifest(_children()), _Api(), names=["children"])

    def _single_empty_partition(state: dict[str, Any]) -> None:
        state["states"] = [{"partition": {}, "cursor": state["states"][0]["cursor"]}]

    messages = result.with_final_state("children", _single_empty_partition)
    assert PARTITION_STATE_SHAPE not in result.problems(messages)


def test_lookback_window_up_to_the_read_duration_rounded_up_passes() -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    messages = result.with_final_state("children", lambda state: state.update(lookback_window=2))
    assert result.problems(messages, read_seconds=1.2) == {}
    assert PARTITION_STATE_SHAPE in result.problems(messages, read_seconds=0.9)


def test_global_substream_cursor_must_be_used() -> None:
    result = _Read(_manifest(_children(_cursor(global_cursor=True))), _Api(), names=["children"])
    assert result.problems() == {}
    messages = result.with_final_state(
        "children", lambda state: state.update(use_global_cursor=False)
    )
    problems = result.problems(messages)
    assert list(problems) == [PARTITION_STATE_SHAPE]
    assert "`global_substream_cursor`" in problems[PARTITION_STATE_SHAPE][0]


def test_global_state_must_hold_the_cursor() -> None:
    result = _Read(_manifest(_children(_cursor(global_cursor=True))), _Api(), names=["children"])
    messages = result.with_final_state("children", lambda state: state.pop("state"))
    problems = result.problems(messages)
    assert list(problems) == [PARTITION_STATE_SHAPE]
    assert "the global `state` has no `updated_at`" in problems[PARTITION_STATE_SHAPE][0]


def test_global_state_is_not_required_when_no_record_has_a_cursor_value() -> None:
    result = _Read(
        _manifest(_children(_cursor(global_cursor=True))), _Api(no_values=True), names=["children"]
    )
    assert list(result.problems()) == [CURSOR_VALUES]


# --- Parent state -----------------------------------------------------------------------------


def test_missing_parent_state_fails() -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    assert result.problems() == {}
    messages = result.with_final_state("children", lambda state: state.pop("parent_state"))
    problems = result.problems(messages)
    assert list(problems) == [PARENT_STATE]
    assert "'parents'" in problems[PARENT_STATE][0]


def test_parent_state_is_required_behind_a_grouping_router() -> None:
    result = _Read(_manifest(_children(group_size=2)), _Api(), names=["children"])
    messages = result.with_final_state("children", lambda state: state.pop("parent_state"))
    assert list(result.problems(messages)) == [PARENT_STATE]


def test_parent_state_is_not_required_without_incremental_dependency() -> None:
    result = _Read(_manifest(_children(incremental_dependency=False)), _Api(), names=["children"])
    messages = result.with_final_state("children", lambda state: state.pop("parent_state", None))
    assert result.problems(messages) == {}


def test_parent_state_without_the_incremental_parent_fails() -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    messages = result.with_final_state(
        "children", lambda state: state.update(parent_state={"unrelated": {}})
    )
    assert list(result.problems(messages)) == [PARENT_STATE]


# --- Partition coverage -----------------------------------------------------------------------


def test_listed_partition_without_state_fails() -> None:
    result = _Read(_manifest(_regions()), _Api())
    assert result.problems() == {}
    messages = result.with_final_state("regions", lambda state: state["states"].pop())
    problems = result.problems(messages)
    assert list(problems) == [PARTITION_COVERAGE]
    assert "1 of 2 partitions" in problems[PARTITION_COVERAGE][0]
    _assert_no_values(problems)


def test_partition_coverage_is_only_checked_for_a_list_router(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    messages = result.with_final_state("children", lambda state: state["states"].pop())

    def _list_parents(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a substream router lists its partitions by reading its parent")

    monkeypatch.setattr(SubstreamPartitionRouter, "stream_slices", _list_parents)
    assert result.problems(messages) == {}


# --- Round trip -------------------------------------------------------------------------------


def test_a_final_state_the_source_cannot_load_fails() -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    messages = result.with_final_state(
        "children", lambda state: state["states"][0]["cursor"].update(updated_at="not a date")
    )
    problems = result.problems(messages)
    assert ROUND_TRIP in problems
    assert "raised `ValueError`" in problems[ROUND_TRIP][0]
    _assert_no_values({ROUND_TRIP: problems[ROUND_TRIP]})


def test_a_cursor_that_drops_partitions_on_load_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    load = ConcurrentPerPartitionCursor._set_initial_state

    def _load_all_but_one(self: ConcurrentPerPartitionCursor, stream_state: Any) -> None:
        load(self, stream_state)
        if self._cursor_per_partition:
            self._cursor_per_partition.popitem()

    monkeypatch.setattr(ConcurrentPerPartitionCursor, "_set_initial_state", _load_all_but_one)
    problems = result.problems()
    assert list(problems) == [ROUND_TRIP]
    assert "holds 2 partition(s), but the state has 3" in problems[ROUND_TRIP][0]


def test_a_build_that_sends_http_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    load = ConcurrentPerPartitionCursor._set_initial_state

    def _load_over_http(self: ConcurrentPerPartitionCursor, stream_state: Any) -> None:
        if stream_state:
            requests.get(f"{BASE}/parents")
        load(self, stream_state)

    monkeypatch.setattr(ConcurrentPerPartitionCursor, "_set_initial_state", _load_over_http)
    problems = result.problems()
    assert list(problems) == [ROUND_TRIP]
    assert "it sent an HTTP request" in problems[ROUND_TRIP][0]


def test_building_streams_refuses_http() -> None:
    with _no_http(), pytest.raises(RuntimeError, match="HTTP request"):
        requests.get(f"{BASE}/events")


# --- Skipped streams and sources --------------------------------------------------------------


def test_streams_without_a_cursor_are_skipped() -> None:
    result = _Read(_manifest(_stream("events", "/events")), _Api())
    messages = result.with_final_state("events", lambda state: state.clear())
    assert result.problems(messages) == {}


def test_custom_cursor_subclasses_are_skipped() -> None:
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api(late_days=5))
    assert list(result.problems()) == [CHECKPOINTS]
    cursor = result.recorder.streams["events"].cursor
    cursor.__class__ = type("CustomCursor", (ConcurrentCursor,), {})
    assert result.problems() == {}


def test_other_sources_are_not_checked() -> None:
    assert CursorStateRecorder.install(object()) is None
    assert (
        find_cursor_state_problems(
            messages=lambda: [],
            configured_catalog=_catalog([]),
            recorder=None,
            read_seconds=1,
        )
        == {}
    )


# --- Errors in the checks themselves ---------------------------------------------------------


@pytest.mark.parametrize("method", ["add_record", "add_state", "add_checkpoint_report"])
def test_a_check_that_crashes_is_reported_as_advisory(
    monkeypatch: pytest.MonkeyPatch, method: str
) -> None:
    def _crash(*_args: Any, **_kwargs: Any) -> None:
        raise ZeroDivisionError

    if method == "add_checkpoint_report":
        monkeypatch.setattr(f"{CHECKS_MODULE}._report_checkpoints", _crash)
    else:
        monkeypatch.setattr(_StreamCheck, method, _crash)
    result = _Read(_manifest(_stream("events", "/events", _cursor())), _Api())
    problems = result.problems()
    assert list(problems) == [CHECK_ERRORS]
    assert "`ZeroDivisionError`" in problems[CHECK_ERRORS][0]


@pytest.mark.parametrize("crashing", ["_resume_start", "_add_checkpoint"])
def test_a_crash_in_the_checkpoint_walk_keeps_the_other_checks(
    monkeypatch: pytest.MonkeyPatch, crashing: str
) -> None:
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api())
    messages = result.with_final_state(
        "events", lambda state: state.update(updated_at="2024-01-02T00:00:00Z")
    )

    def _crash(*_args: Any, **_kwargs: Any) -> None:
        raise ZeroDivisionError

    if crashing == "_resume_start":
        monkeypatch.setattr(f"{CHECKS_MODULE}.{crashing}", _crash)
    else:
        monkeypatch.setattr(_StreamCheck, crashing, _crash)
    problems = result.problems(messages)
    assert set(problems) == {CHECK_ERRORS, FINAL_STATE}
    # Reported once: the stream's checkpoint check stops at the first crash.
    assert len(problems[CHECK_ERRORS]) == 1
    assert "`ZeroDivisionError`" in problems[CHECK_ERRORS][0]


# --- `test_basic_read` ------------------------------------------------------------------------


def _suite(tmp_path: Path, manifest: Mapping[str, Any]) -> DeclarativeSourceTestSuite:
    (tmp_path / "manifest.yaml").write_text(yaml.safe_dump(dict(manifest)))
    (tmp_path / ACCEPTANCE_TEST_CONFIG).write_text(
        yaml.safe_dump(
            {
                "acceptance_tests": {
                    "basic_read": {"tests": [{"config_path": "secrets/config.json"}]}
                }
            }
        )
    )

    class _Suite(DeclarativeSourceTestSuite):
        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return tmp_path

    return _Suite()


def _scenario(**fields: Any) -> ConnectorTestScenario:
    return ConnectorTestScenario.model_validate(
        {"config_dict": CONFIG, "sections": ("basic_read",), **fields}
    )


def test_basic_read_fails_on_an_enforced_cursor_state_problem(tmp_path: Path) -> None:
    suite = _suite(tmp_path, _manifest(_stream("events", "/events", _cursor(step="P1D"))))
    with requests_mock.Mocker() as mocker:
        _Api(late_days=5).install(mocker)
        with pytest.raises(AssertionError, match="checkpoints do not skip records"):
            suite.test_basic_read(_scenario())
        suite.test_basic_read(_scenario(validate_state_messages=False))
        _Api().install(mocker)
        suite.test_basic_read(_scenario())


def test_basic_read_warns_on_an_advisory_cursor_state_problem(tmp_path: Path) -> None:
    suite = _suite(tmp_path, _manifest(_stream("events", "/events", _cursor())))
    with requests_mock.Mocker() as mocker, warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _Api(bad_values=True).install(mocker)
        suite.test_basic_read(_scenario())
    assert [warning.category for warning in caught if warning.category is CursorStateWarning] == [
        CursorStateWarning
    ]


def test_basic_read_leaves_no_hooks_on_the_source(tmp_path: Path) -> None:
    manifest = _manifest(_stream("events", "/events", _cursor(step="P1D")))
    suite = _suite(tmp_path, manifest)
    source = _source(manifest)

    class _SameSourceSuite(type(suite)):  # type: ignore[misc]
        @classmethod
        def create_connector(cls, scenario: ConnectorTestScenario | None) -> Any:
            return source

    with requests_mock.Mocker() as mocker:
        _Api().install(mocker)
        _SameSourceSuite().test_basic_read(_scenario())
    assert "streams" not in source.__dict__
    assert "_handle_item" not in source._concurrent_source.__dict__  # noqa: SLF001


def _same_source_suite(suite: DeclarativeSourceTestSuite, source: object) -> Any:
    class _SameSourceSuite(type(suite)):  # type: ignore[misc]
        @classmethod
        def create_connector(cls, scenario: ConnectorTestScenario | None) -> Any:
            return source

    return _SameSourceSuite()


def test_basic_read_removes_its_hooks_when_the_read_fails(tmp_path: Path) -> None:
    manifest = _manifest(_stream("events", "/events", _cursor(step="P1D")))
    source = _source(manifest)
    with requests_mock.Mocker() as mocker:
        _Api().install(mocker)
        mocker.get(re.compile(r"https://api\.test/events.*"), status_code=403, json={})
        with pytest.raises(AirbyteEntrypointException):
            _same_source_suite(_suite(tmp_path, manifest), source).test_basic_read(_scenario())
    assert "streams" not in source.__dict__
    assert "_handle_item" not in source._concurrent_source.__dict__  # noqa: SLF001


@pytest.mark.parametrize(
    "fields",
    [
        pytest.param({"validate_state_messages": False}, id="opted_out"),
        pytest.param({"sections": ("full_refresh",)}, id="not_a_basic_read_config"),
    ],
)
def test_basic_read_installs_no_hooks_when_it_does_not_check_the_cursor_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fields: dict[str, Any]
) -> None:
    installed: list[object] = []
    install = CursorStateRecorder.install.__func__  # type: ignore[attr-defined]

    def _install(cls: type[CursorStateRecorder], connector: object) -> Any:
        installed.append(connector)
        return install(cls, connector)

    monkeypatch.setattr(CursorStateRecorder, "install", classmethod(_install))
    suite = _suite(tmp_path, _manifest(_stream("events", "/events", _cursor(step="P1D"))))
    with requests_mock.Mocker() as mocker:
        _Api(late_days=5).install(mocker)
        suite.test_basic_read(_scenario(**fields))
        assert installed == []
        _Api().install(mocker)
        suite.test_basic_read(_scenario())
    assert len(installed) == 1


def test_basic_read_bounds_the_lookback_window_by_the_read_duration(tmp_path: Path) -> None:
    # The final state of a per-partition stream holds the read's duration in whole seconds,
    # rounded up, so it is at least 1 even for a fast read.
    manifest = _manifest(_children(_cursor(step="P3D")))
    source = _source(manifest)
    with requests_mock.Mocker() as mocker:
        _Api().install(mocker)
        _same_source_suite(_suite(tmp_path, manifest), source).test_basic_read(
            _scenario(empty_streams=[{"name": "parents"}])
        )


class _ReadsAnotherSource(ConcurrentDeclarativeSource):
    """A source whose `discover` and `read` run on a fresh source, as some Python suites do."""

    edit_state: Callable[[dict[str, Any]], None] | None = None
    build_fails = False

    def _fresh(self) -> ConcurrentDeclarativeSource:
        return _source(self._source_config)

    def streams(self, config: Mapping[str, Any]) -> Any:
        if self.build_fails:
            raise ZeroDivisionError
        return super().streams(config)

    def discover(self, logger: Any, config: Mapping[str, Any]) -> Any:
        return self._fresh().discover(logger, config)

    def read(self, logger: Any, config: Any, catalog: Any, state: Any = None) -> Any:
        for message in self._fresh().read(logger, config, catalog, state):
            if self.edit_state is not None and message.type == Type.STATE:
                assert message.state is not None and message.state.stream is not None
                state_dict = copy.deepcopy(message.state.stream.stream_state.__dict__)
                self.edit_state(state_dict)
                message.state.stream.stream_state = AirbyteStateBlob(**state_dict)
            yield message


def _another_source_suite(
    tmp_path: Path,
    manifest: Mapping[str, Any],
    *,
    edit_state: Callable[[dict[str, Any]], None] | None = None,
    build_fails: bool = False,
) -> Any:
    suite = _suite(tmp_path, manifest)

    class _AnotherSourceSuite(type(suite)):  # type: ignore[misc]
        @classmethod
        def create_connector(cls, scenario: ConnectorTestScenario | None) -> Any:
            source = _ReadsAnotherSource(
                source_config=dict(manifest), config=CONFIG, catalog=None, state=None
            )
            source.edit_state = edit_state
            source.build_fails = build_fails
            return source

    return _AnotherSourceSuite()


def test_basic_read_checks_a_suite_whose_read_builds_another_source(tmp_path: Path) -> None:
    manifest = _manifest(_children(_cursor(global_cursor=True)))
    scenario = _scenario(empty_streams=[{"name": "parents"}])
    with requests_mock.Mocker() as mocker:
        _Api().install(mocker)
        _another_source_suite(tmp_path, manifest).test_basic_read(scenario)
        suite = _another_source_suite(
            tmp_path,
            manifest,
            edit_state=lambda state: state.update(use_global_cursor=False)
            if "use_global_cursor" in state
            else None,
        )
        with pytest.raises(AssertionError, match="`global_substream_cursor`"):
            suite.test_basic_read(scenario)


def test_basic_read_warns_when_the_reference_source_cannot_build(tmp_path: Path) -> None:
    manifest = _manifest(_stream("events", "/events", _cursor(step="P1D")))
    suite = _another_source_suite(tmp_path, manifest, build_fails=True)
    with requests_mock.Mocker() as mocker, warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _Api().install(mocker)
        suite.test_basic_read(_scenario())
    messages = [
        str(warning.message) for warning in caught if warning.category is CursorStateWarning
    ]
    assert len(messages) == 1
    assert "building the source's streams failed with `ZeroDivisionError`" in messages[0]
