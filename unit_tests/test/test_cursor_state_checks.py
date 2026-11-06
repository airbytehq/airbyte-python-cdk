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
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor
from airbyte_cdk.test.entrypoint_wrapper import read
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
    find_cursor_state_problems,
    format_cursor_state_problems,
)
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG

BASE = "https://api.test"
START = "2024-01-01T00:00:00Z"
END = dt.datetime(2024, 1, 31, tzinfo=dt.timezone.utc)
CONFIG = {"start_date": START}
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
    - `ignore_until`: the API ignores `until`, so every slice returns everything up to the end.
    - `bad_values`: the first record has no `updated_at` and the second one is not a date.
    - `no_values`: no record has an `updated_at`.
    """

    def __init__(
        self,
        *,
        late_days: int = 0,
        ignore_until: bool = False,
        bad_values: bool = False,
        no_values: bool = False,
    ) -> None:
        self.late_days = late_days
        self.ignore_until = ignore_until
        self.bad_values = bad_values
        self.no_values = no_values

    def _records(self, request: Any, id_offset: int = 0) -> dict[str, Any]:
        query = parse_qs(urlparse(request.url).query)
        since = _parse(query["since"][0]) if "since" in query else None
        until = _parse(query["until"][0]) if "until" in query else None
        upper = END if self.ignore_until or until is None else until
        records = []
        created = dt.datetime(2024, 1, 1, 1, tzinfo=dt.timezone.utc)
        while created <= END:
            if (since is None or created >= since) and created <= upper:
                late = self.late_days and created.day % 3 == 0
                updated = created + dt.timedelta(days=self.late_days) if late else created
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
        return find_cursor_state_problems(
            messages=lambda: iter(found),
            configured_catalog=self.catalog,
            recorder=recorder or self.recorder,
            read_seconds=read_seconds,
            reference_source=reference_source,
        )

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
        messages[messages.index(final)] = edited
        return messages


def _text(problems: Mapping[CursorStateCheck, list[str]]) -> str:
    return format_cursor_state_problems(problems)


def _assert_no_values(problems: Mapping[CursorStateCheck, list[str]]) -> None:
    """Problems name streams, fields and counts, never record, cursor or partition values."""
    text = _text(problems)
    assert "2024" not in text
    assert not re.search(r"\d{9,}", text), "a record id leaked into the problems"


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


def test_suites_whose_read_builds_another_source_get_the_concurrent_cursor_part() -> None:
    manifest = _manifest(
        _stream("events", "/events", _cursor(step="P1D")), _children(_cursor(step="P3D"))
    )
    result = _Read(manifest, _Api(late_days=5))
    # The hooks sit on a source that never reads, as in a suite that replaces `read`.
    idle = CursorStateRecorder.install(_source(manifest))
    assert idle is not None
    problems = result.problems(recorder=idle, reference_source=lambda: _source(manifest))
    assert list(problems) == [CHECKPOINTS]
    assert len(problems[CHECKPOINTS]) == 1
    assert "stream 'events'" in problems[CHECKPOINTS][0]


def test_unmatched_partitions_skip_the_per_partition_checkpoint_check() -> None:
    result = _Read(_manifest(_children(_cursor(step="P3D"))), _Api(late_days=5), names=["children"])
    result.recorder.record_partitions["children"].pop()
    problems = result.problems()
    assert list(problems) == [CHECK_ERRORS]
    assert problems[CHECK_ERRORS] == [
        "- stream 'children': its records could not be matched to their partitions, so its "
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
    problems = result.problems(messages)
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
    assert "'events': 2 record(s)" in result.problems(messages)[CHECKPOINTS][0]


def test_a_record_at_the_resume_start_is_not_flagged() -> None:
    # A resume starts at the state value itself, so it reads a record at that value again.
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api())
    state = result.states("events")[0]
    record = next(message for message in result.messages if message.type == Type.RECORD)
    messages = [_state(state, "2024-01-20T00:00:00Z"), _record(record, "2024-01-20T00:00:00Z")]
    assert result.problems(messages) == {}


def test_bounds_past_the_cap_use_the_bound_of_an_earlier_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import airbyte_cdk.test.standard_tests._cursor_state_checks as checks

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
    monkeypatch.setattr(checks, "_MAX_BOUNDS_PER_STREAM", 1)
    # The second state gets no bound of its own; the bound of the first one still flags the
    # second record.
    assert "'events': 2 record(s)" in result.problems(messages)[CHECKPOINTS][0]


def test_bounds_past_the_cap_never_flag_more(monkeypatch: pytest.MonkeyPatch) -> None:
    import airbyte_cdk.test.standard_tests._cursor_state_checks as checks

    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api(late_days=5))
    monkeypatch.setattr(checks, "_MAX_BOUNDS_PER_STREAM", 0)
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


# --- Partition coverage -----------------------------------------------------------------------


def test_listed_partition_without_state_fails() -> None:
    result = _Read(_manifest(_regions()), _Api())
    assert result.problems() == {}
    messages = result.with_final_state("regions", lambda state: state["states"].pop())
    problems = result.problems(messages)
    assert list(problems) == [PARTITION_COVERAGE]
    assert "1 of 2 partitions" in problems[PARTITION_COVERAGE][0]
    _assert_no_values(problems)


def test_partition_coverage_is_only_checked_for_a_list_router() -> None:
    result = _Read(_manifest(_children()), _Api(), names=["children"])
    messages = result.with_final_state("children", lambda state: state["states"].pop())
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


def test_a_rebuilt_stream_without_a_concurrent_cursor_keeps_the_read_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import airbyte_cdk.test.standard_tests._cursor_state_checks as checks

    class _Stream:
        name = "events"
        cursor = object()

    monkeypatch.setattr(checks, "_rebuild", lambda source, states: ({"events": _Stream()}, {}))
    result = _Read(_manifest(_stream("events", "/events", _cursor(step="P1D"))), _Api(late_days=5))
    assert list(result.problems()) == [CHECKPOINTS]


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
    import airbyte_cdk.test.standard_tests._cursor_state_checks as checks

    def _crash(*_args: Any, **_kwargs: Any) -> None:
        raise ZeroDivisionError

    if method == "add_checkpoint_report":
        monkeypatch.setattr(checks, "_report_checkpoints", _crash)
    else:
        monkeypatch.setattr(checks._StreamCheck, method, _crash)  # noqa: SLF001
    result = _Read(_manifest(_stream("events", "/events", _cursor())), _Api())
    problems = result.problems()
    assert list(problems) == [CHECK_ERRORS]
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
