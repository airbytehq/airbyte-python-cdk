# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""The future-state read: an `incremental` entry reads from a state far in the future.

- An `incremental` entry with a `future_state.future_state_path` is a scenario of its own: it is
  not merged with the config's other entries, and does not take their `empty_streams`.
- The state file must be a non-empty list of `STREAM` or `GLOBAL` state messages, with a state for
  every incremental stream the entry reads, except its `future_state.missing_streams`.
- The read must return no records and emit state for every stream; a declarative stream's state
  must hold its cursor in the cursor's declared format.
- Failure messages name the file, streams and fields, never record or state values.
"""

from __future__ import annotations

import copy
import json
import logging
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pytest
import requests_mock
import yaml

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateMessageSerializer,
    AirbyteStateType,
    AirbyteStream,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
    StreamDescriptor,
    SyncMode,
    Type,
)
from airbyte_cdk.sources import Source
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models.scenario import ConnectorTestScenario
from airbyte_cdk.test.standard_tests import DeclarativeSourceTestSuite, SourceTestSuiteBase
from airbyte_cdk.test.standard_tests._acceptance_test_config import (
    ScenarioBypass,
    collect_scenarios,
)
from airbyte_cdk.test.standard_tests._future_state import (
    assert_future_state_covers_streams,
    assert_future_state_read,
    build_future_state_catalog,
    find_cursor_format_problems,
    load_future_state,
)
from airbyte_cdk.test.standard_tests._job_runner import run_test_job
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.test.standard_tests.pytest_hooks import _scenario_test_ids
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG
from unit_tests.test.standard_test_stubs import (
    catalog_message,
    entry,
    record_message,
    run_without_skip,
    stream_json,
    suite_class,
)

STATE_PATH = "integration_tests/abnormal_state.json"
# A value that must never reach a failure message: CI logs are public.
SECRET_VALUE = "2999-12-31-secret"

USERS = AirbyteStream(
    name="users",
    json_schema={"type": "object"},
    supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
    source_defined_cursor=True,
    default_cursor_field=["updated_at"],
)
EVENTS = AirbyteStream(
    name="events",
    json_schema={"type": "object"},
    supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
    source_defined_cursor=True,
    default_cursor_field=["created_at"],
)
STATIC = AirbyteStream(
    name="static",
    json_schema={"type": "object"},
    supported_sync_modes=[SyncMode.full_refresh],
)


def _stream_state(name: str, value: str = SECRET_VALUE) -> dict[str, Any]:
    return {
        "type": "STREAM",
        "stream": {"stream_descriptor": {"name": name}, "stream_state": {"updated_at": value}},
    }


def _incremental_entry(**extra: Any) -> dict[str, Any]:
    return entry(
        "secrets/config.json", **{"future_state": {"future_state_path": STATE_PATH}, **extra}
    )


def _collect(acceptance_tests: dict[str, Any], tmp_path: Path) -> Any:
    return collect_scenarios(
        {"acceptance_tests": acceptance_tests}, tmp_path / ACCEPTANCE_TEST_CONFIG
    )


def _future_scenario(**future_state: Any) -> ConnectorTestScenario:
    return ConnectorTestScenario(
        config_path=Path("secrets/config.json"),
        config_dict={"api_key": "x"},
        sections=("incremental",),
        future_state=ConnectorTestScenario.FutureState(
            future_state_path=Path(STATE_PATH), **future_state
        ),
    )


# --- Collection ---------------------------------------------------------------------------------


def test_incremental_entry_is_its_own_scenario_without_the_read_empty_streams(
    tmp_path: Path,
) -> None:
    suite = suite_class(
        tmp_path,
        {
            "basic_read": {
                "tests": [
                    entry(
                        "secrets/config.json",
                        empty_streams=[{"name": "static", "bypass_reason": "no data"}],
                    )
                ]
            },
            "incremental": {
                "tests": [
                    _incremental_entry(
                        configured_catalog_path="integration_tests/incremental_catalog.json"
                    ),
                    entry(
                        "secrets/config.json",
                        future_state={"future_state_path": "integration_tests/state_2.json"},
                    ),
                ]
            },
        },
        base=DockerConnectorTestSuite,
    )

    read, first, second = suite.get_scenarios()

    assert read.sections == ("basic_read",)
    assert read.configured_catalog_path is None, "the incremental catalog is not merged"
    for future, state_path in [(first, STATE_PATH), (second, "integration_tests/state_2.json")]:
        assert future.sections == ("incremental",)
        assert future.commands == {"read_from_future_state"}
        assert future.future_state is not None
        assert future.future_state.future_state_path == Path(state_path)
        assert future.empty_streams is None, "an empty stream still parses and emits state"
    assert first.configured_catalog_path == Path("integration_tests/incremental_catalog.json")
    assert _scenario_test_ids([read, first, second]) == [
        str(read),
        f"{read} future state abnormal_state",
        f"{read} future state state_2",
    ]


def test_same_state_file_stem_gets_a_unique_test_id() -> None:
    scenarios = [
        _future_scenario(),
        _future_scenario().model_copy(
            update={
                "future_state": ConnectorTestScenario.FutureState(
                    future_state_path=Path("other/abnormal_state.json")
                )
            }
        ),
    ]
    ids = _scenario_test_ids(scenarios)
    assert len(set(ids)) == 2, ids


def test_future_state_fields_are_read(tmp_path: Path) -> None:
    collected = _collect(
        {
            "incremental": {
                "tests": [
                    _incremental_entry(
                        future_state={
                            "future_state_path": STATE_PATH,
                            "missing_streams": [{"name": "events", "bypass_reason": "no cursor"}],
                        },
                        # Read by CAT's other incremental tests and the Java harness, not here.
                        skip_comprehensive_incremental_tests=True,
                        client_container_config={"secrets_path": "/secrets"},
                    )
                ]
            }
        },
        tmp_path,
    )

    [scenario] = collected.scenarios
    assert scenario.future_state is not None
    assert [stream.name for stream in scenario.future_state.missing_streams] == ["events"]


def test_future_state_bypass_reason_is_a_bypass_of_the_incremental_section(
    tmp_path: Path,
) -> None:
    collected = _collect(
        {
            "incremental": {
                "tests": [
                    entry(
                        "secrets/config.json",
                        future_state={
                            "future_state_path": STATE_PATH,
                            "bypass_reason": "The API ignores the start date",
                        },
                    )
                ]
            }
        },
        tmp_path,
    )

    assert collected.scenarios == []
    assert collected.bypasses == [
        ScenarioBypass(
            reason="The API ignores the start date",
            location="acceptance_tests.incremental.tests[0].future_state.bypass_reason",
            sections=("incremental",),
        )
    ]


def test_incremental_entry_without_future_state_has_nothing_to_run(tmp_path: Path) -> None:
    collected = _collect({"incremental": {"tests": [entry("secrets/config.json")]}}, tmp_path)

    assert collected.scenarios == []
    assert collected.bypasses == []
    assert collected.notes == [
        "`acceptance_tests.incremental.tests[0]`: no `future_state.future_state_path`, so there is no state to read from."
    ]


@pytest.mark.parametrize(
    "acceptance_tests, expected",
    [
        pytest.param(
            {
                "incremental": {
                    "tests": [entry("secrets/config.json", future_state_path=SECRET_VALUE)]
                }
            },
            "must be nested under `future_state`",
            id="flat_future_state_path",
        ),
        pytest.param(
            {
                "basic_read": {
                    "tests": [entry("secrets/config.json", future_state={"future_state_path": "x"})]
                }
            },
            "is read only from an `incremental` entry",
            id="future_state_outside_incremental",
        ),
        pytest.param(
            {
                "incremental": {
                    "tests": [
                        _incremental_entry(
                            future_state={
                                "future_state_path": STATE_PATH,
                                "cursor_format": {"format": SECRET_VALUE},
                            }
                        )
                    ]
                }
            },
            "`future_state.cursor_format`",
            id="unknown_future_state_key",
        ),
    ],
)
def test_misplaced_future_state_keys_are_rejected(
    tmp_path: Path, acceptance_tests: dict[str, Any], expected: str
) -> None:
    suite = suite_class(tmp_path, acceptance_tests, base=DockerConnectorTestSuite)

    with pytest.raises(ValueError) as error:
        suite.get_scenarios()

    assert expected in str(error.value)
    assert SECRET_VALUE not in str(error.value)


# --- Input state in `run_test_job` --------------------------------------------------------------


class _StateSource(Source):
    """A source over `streams` whose `read` behaves as instructed.

    Behaviours:
    - `honours_state`: no records, and the input state of each configured stream back.
    - `ignores_state`: one record and a state per configured stream.
    - `no_state`: nothing at all.

    The input state of every `read` is appended to `read_states`, and its catalog to
    `read_catalogs`.
    """

    def __init__(self, behavior: str, streams: list[AirbyteStream]) -> None:
        self._behavior = behavior
        self._streams = streams
        self.read_states: list[list[AirbyteStateMessage] | None] = []
        self.read_catalogs: list[ConfiguredAirbyteCatalog] = []

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(connectionSpecification={"type": "object", "properties": {}})

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        return AirbyteCatalog(streams=list(self._streams))

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterable[AirbyteMessage]:
        self.read_states.append(state)
        self.read_catalogs.append(catalog)
        if self._behavior == "no_state":
            return
        for configured in catalog.streams:
            name = configured.stream.name
            if self._behavior == "ignores_state":
                yield AirbyteMessage(
                    type=Type.RECORD,
                    record=AirbyteRecordMessage(
                        stream=name, data={"updated_at": SECRET_VALUE}, emitted_at=0
                    ),
                )
            yield AirbyteMessage(
                type=Type.STATE,
                state=AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(name=name),
                        stream_state=AirbyteStateBlob(updated_at=SECRET_VALUE),
                    ),
                ),
            )


def test_run_test_job_passes_the_input_state_to_read(tmp_path: Path) -> None:
    source = _StateSource("honours_state", [USERS])
    state = [_stream_state("users")]

    run_test_job(
        source,
        "read",
        connector_root=tmp_path,
        test_scenario=ConnectorTestScenario(config_dict={"api_key": "x"}),
        catalog=build_future_state_catalog(_future_scenario(), [USERS], tmp_path),
        state=state,
    )

    [read_state] = source.read_states
    assert read_state is not None
    assert [AirbyteStateMessageSerializer.dump(message) for message in read_state] == state


def test_run_test_job_rejects_state_outside_read(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="only passed to `read`, not to `discover`"):
        run_test_job(
            _StateSource("honours_state", [USERS]),
            "discover",
            connector_root=tmp_path,
            test_scenario=ConnectorTestScenario(config_dict={"api_key": "x"}),
            state=[_stream_state("users")],
        )


# --- Loading the state file ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "content, expected",
    [
        pytest.param(None, "does not exist", id="missing"),
        pytest.param('[{"type": ', "is not valid JSON (line 1, column", id="invalid_json"),
        pytest.param(
            json.dumps({"users": {"updated_at": SECRET_VALUE}}),
            "uses the legacy layout",
            id="legacy_mapping",
        ),
        pytest.param("[]", "holds an empty list", id="empty_list"),
        pytest.param(json.dumps(SECRET_VALUE), "holds a JSON str", id="not_a_list"),
        pytest.param(
            json.dumps([{"stream_descriptor": {"name": "users"}, "stream_state": {}}]),
            "message 0 has `type` None and no matching `stream` or `global` object",
            id="untyped_message",
        ),
        pytest.param(
            json.dumps([_stream_state("users"), {"type": "STREAM"}]),
            "message 1 has `type` 'STREAM' and no matching `stream` or `global` object",
            id="stream_message_without_stream",
        ),
        pytest.param(
            json.dumps([{"type": "LEGACY", "data": {"users": SECRET_VALUE}}]),
            "message 0 has `type` 'LEGACY'",
            id="legacy_message",
        ),
        pytest.param(json.dumps([SECRET_VALUE]), "message 0 is a JSON str", id="not_an_object"),
        pytest.param(
            json.dumps([{"type": "STREAM", "stream": SECRET_VALUE}]),
            "message 0 is not a valid state message",
            id="invalid_message",
        ),
    ],
)
def test_load_future_state_fails_naming_the_file_not_the_values(
    tmp_path: Path, content: str | None, expected: str
) -> None:
    state_path = tmp_path / "abnormal_state.json"
    if content is not None:
        state_path.write_text(content)

    with pytest.raises(AssertionError) as error:
        load_future_state(state_path, display_path=Path(STATE_PATH))

    message = str(error.value)
    assert f"`{STATE_PATH}`" in message
    assert expected in message
    assert SECRET_VALUE not in message


def test_load_future_state_reads_stream_and_global_messages(tmp_path: Path) -> None:
    state = [
        _stream_state("users"),
        {
            "type": "GLOBAL",
            "global": {
                "shared_state": {"cdc": SECRET_VALUE},
                "stream_states": [
                    {"stream_descriptor": {"name": "events"}, "stream_state": {"lsn": 1}}
                ],
            },
        },
    ]
    state_path = tmp_path / "abnormal_state.json"
    state_path.write_text(json.dumps(state))

    assert load_future_state(state_path, display_path=Path(STATE_PATH)) == state


# --- The streams read and their coverage --------------------------------------------------------


def test_catalog_reads_every_incremental_stream_but_the_missing_ones(tmp_path: Path) -> None:
    scenario = _future_scenario(missing_streams=[{"name": "events", "bypass_reason": "no data"}])

    catalog = build_future_state_catalog(scenario, [USERS, EVENTS, STATIC], tmp_path)

    assert [(s.stream.name, s.sync_mode, s.cursor_field) for s in catalog.streams] == [
        ("users", SyncMode.incremental, ["updated_at"])
    ]


def test_catalog_reads_the_incremental_streams_whatever_the_declared_sync_mode(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "integration_tests" / "configured_catalog.json"
    catalog_path.parent.mkdir()
    catalog_path.write_text(
        json.dumps(
            {
                "streams": [
                    {
                        "stream": stream_json(name, ["full_refresh", "incremental"]),
                        "sync_mode": sync_mode,
                        "cursor_field": cursor_field,
                        "destination_sync_mode": "append",
                    }
                    for name, sync_mode, cursor_field in [
                        ("users", "incremental", ["modified"]),
                        ("events", "full_refresh", []),
                        ("static", "full_refresh", []),
                    ]
                ]
            }
        )
    )
    scenario = _future_scenario().model_copy(
        update={"configured_catalog_path": Path("integration_tests/configured_catalog.json")}
    )

    catalog = build_future_state_catalog(scenario, [USERS, EVENTS, STATIC], tmp_path)

    assert [(s.stream.name, s.sync_mode, s.cursor_field) for s in catalog.streams] == [
        ("users", SyncMode.incremental, ["modified"]),
        ("events", SyncMode.incremental, ["created_at"]),
    ]


def test_catalog_without_an_incremental_stream_fails(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match="has no incremental stream to read"):
        build_future_state_catalog(_future_scenario(), [STATIC], tmp_path)


def test_catalog_whose_incremental_streams_are_all_missing_fails(tmp_path: Path) -> None:
    """An empty catalog would pass every check having read nothing."""
    scenario = _future_scenario(
        missing_streams=[
            {"name": "users", "bypass_reason": "no data"},
            {"name": "events", "bypass_reason": "no data"},
        ]
    )

    with pytest.raises(AssertionError, match="has no incremental stream to read"):
        build_future_state_catalog(scenario, [USERS, EVENTS, STATIC], tmp_path)


def test_state_must_cover_every_stream_read(tmp_path: Path) -> None:
    catalog = build_future_state_catalog(_future_scenario(), [USERS, EVENTS], tmp_path)
    global_state = {
        "type": "GLOBAL",
        "global": {
            "stream_states": [{"stream_descriptor": {"name": "events"}, "stream_state": {}}]
        },
    }

    assert_future_state_covers_streams(
        [_stream_state("users"), global_state], catalog, display_path=Path(STATE_PATH)
    )
    with pytest.raises(AssertionError) as error:
        assert_future_state_covers_streams(
            [_stream_state("users")], catalog, display_path=Path(STATE_PATH)
        )
    assert "has no state for the incremental stream(s) 'events'" in str(error.value)
    assert "`future_state.missing_streams`" in str(error.value)


# --- The read's outcome -------------------------------------------------------------------------


def _read_messages(*messages: str) -> EntrypointOutput:
    return EntrypointOutput(messages=list(messages))


def test_read_fails_per_stream_on_records_and_missing_state(tmp_path: Path) -> None:
    catalog = build_future_state_catalog(_future_scenario(), [USERS, EVENTS], tmp_path)
    output = _read_messages(
        record_message("users"),
        record_message("users"),
        json.dumps({"type": "STATE", "state": _stream_state("users")}),
    )

    with pytest.raises(AssertionError) as error:
        assert_future_state_read(
            records=output.records_iterator,
            state_messages=output.state_messages,
            catalog=catalog,
            display_path=Path(STATE_PATH),
        )

    assert str(error.value).splitlines() == [
        f"The read from the future state `{STATE_PATH}` did not honour it:",
        "- stream 'users' returned 2 record(s): it ignored its state, could not parse it, or the state's cursor is not far enough in the future",
        "- stream 'events' emitted no `STREAM` or `GLOBAL` state message",
    ]


def test_read_passes_with_no_records_and_state_for_every_stream(tmp_path: Path) -> None:
    catalog = build_future_state_catalog(_future_scenario(), [USERS, EVENTS], tmp_path)
    global_state = {
        "type": "GLOBAL",
        "global": {
            "stream_states": [
                {"stream_descriptor": {"name": name}, "stream_state": {}}
                for name in ("users", "events")
            ]
        },
    }
    output = _read_messages(json.dumps({"type": "STATE", "state": global_state}))

    assert_future_state_read(
        records=output.records_iterator,
        state_messages=output.state_messages,
        catalog=catalog,
        display_path=Path(STATE_PATH),
    )


# --- The in-process test ------------------------------------------------------------------------


def _in_process_suite(tmp_path: Path, source: _StateSource, state: list[Any]) -> Any:
    suite = suite_class(tmp_path, {}, base=SourceTestSuiteBase, make_connector=lambda: source)()
    state_path = suite.get_connector_root_dir() / STATE_PATH
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state))
    return suite


def test_in_process_read_passes_when_the_source_honours_its_state(tmp_path: Path) -> None:
    source = _StateSource("honours_state", [USERS, STATIC])
    suite = _in_process_suite(tmp_path, source, [_stream_state("users")])

    run_without_skip(suite.test_read_from_future_state, _future_scenario())

    [read_state] = source.read_states
    assert read_state is not None and len(read_state) == 1
    assert [s.stream.name for s in source.read_catalogs[0].streams] == ["users"]


@pytest.mark.parametrize(
    "behavior, state, expected",
    [
        pytest.param(
            "ignores_state",
            [_stream_state("users")],
            "stream 'users' returned 1 record(s)",
            id="records",
        ),
        pytest.param(
            "no_state",
            [_stream_state("users")],
            "stream 'users' emitted no `STREAM` or `GLOBAL` state message",
            id="no_state",
        ),
        pytest.param(
            "honours_state",
            [_stream_state("other")],
            "has no state for the incremental stream(s) 'users'",
            id="uncovered_stream",
        ),
    ],
)
def test_in_process_read_fails_without_printing_values(
    tmp_path: Path, behavior: str, state: list[Any], expected: str
) -> None:
    suite = _in_process_suite(tmp_path, _StateSource(behavior, [USERS]), state)

    with pytest.raises(AssertionError) as error:
        suite.test_read_from_future_state(_future_scenario())

    assert expected in str(error.value)
    assert SECRET_VALUE not in str(error.value)


def test_in_process_read_skips_a_scenario_without_future_state(tmp_path: Path) -> None:
    suite = _in_process_suite(tmp_path, _StateSource("honours_state", [USERS]), [])
    scenario = _future_scenario().model_copy(update={"future_state": None})

    with pytest.raises(pytest.skip.Exception, match="has no future state to read from"):
        suite.test_read_from_future_state(scenario)


LEAKED_KEY = "leaked-api-key-0123456789"


class _LeakingSource(_StateSource):
    """Honours its state, and prints its secret `api_key` in the output of `leaking_command`.

    The entrypoint masks secrets in log messages, so it goes in a stream description or a record.
    """

    def __init__(self, leaking_command: str) -> None:
        super().__init__("honours_state", [USERS])
        self._leaking_command = leaking_command

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(
            connectionSpecification={
                "type": "object",
                "properties": {"api_key": {"type": "string", "airbyte_secret": True}},
            }
        )

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        if self._leaking_command != "discover":
            return super().discover(logger, config)
        leaking = copy.copy(USERS)
        leaking.json_schema = {"type": "object", "description": config["api_key"]}
        return AirbyteCatalog(streams=[leaking])

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterable[AirbyteMessage]:
        if self._leaking_command == "read":
            yield AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream="users", data={"key": config["api_key"]}, emitted_at=0
                ),
            )
        yield from super().read(logger, config, catalog, state)


@pytest.mark.parametrize("command", ["discover", "read"])
def test_in_process_read_fails_on_a_secret_in_the_output(tmp_path: Path, command: str) -> None:
    """As `test_basic_read`, a leaked secret fails before any assertion prints raw messages."""
    suite = _in_process_suite(tmp_path, _LeakingSource(command), [_stream_state("users")])
    scenario = _future_scenario().model_copy(update={"config_dict": {"api_key": LEAKED_KEY}})

    with pytest.raises(AssertionError, match=f"`{command}` .* printed secret values") as error:
        run_without_skip(suite.test_read_from_future_state, scenario)
    assert LEAKED_KEY not in str(error.value)


# --- Declarative sources ------------------------------------------------------------------------

_MANIFEST: dict[str, Any] = {
    "version": "6.0.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["items"]},
    "spec": {
        "type": "Spec",
        "connection_specification": {
            "type": "object",
            "properties": {"api_key": {"type": "string"}},
        },
    },
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "items",
            "primary_key": ["id"],
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.invalid",
                    "path": "/items",
                    "http_method": "GET",
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
            },
            "incremental_sync": {
                "type": "DatetimeBasedCursor",
                "cursor_field": "updated_at",
                "datetime_format": "%Y-%m-%d",
                "start_datetime": "2024-01-01",
                "step": "P10000D",
                "cursor_granularity": "P1D",
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "type": "object",
                    "properties": {"id": {"type": "integer"}, "updated_at": {"type": "string"}},
                },
            },
        }
    ],
}


def _declarative_suite(tmp_path: Path, state: list[Any]) -> DeclarativeSourceTestSuite:
    (tmp_path / "manifest.yaml").write_text(yaml.safe_dump(_MANIFEST))
    state_path = tmp_path / STATE_PATH
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state))

    class _Suite(DeclarativeSourceTestSuite):
        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return tmp_path

    return _Suite()


class _NoArgumentDeclarativeSource(ConcurrentDeclarativeSource):
    """A Python declarative source built with no arguments, so without the read's state."""

    def __init__(self) -> None:
        super().__init__(
            config={"api_key": "x"},
            catalog=None,
            state=None,
            source_config=copy.deepcopy(_MANIFEST),
        )


def test_generic_suite_fails_a_declarative_source_built_without_state(tmp_path: Path) -> None:
    """Without the state, the source would read from its start date and be blamed for it."""
    state_path = tmp_path / STATE_PATH
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps([_items_state("2999-01-01")]))

    class _Suite(SourceTestSuiteBase):
        connector = _NoArgumentDeclarativeSource

        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return tmp_path

    with (
        requests_mock.Mocker() as mocker,
        pytest.raises(pytest.fail.Exception, match="without the read's state"),
    ):
        _Suite().test_read_from_future_state(_future_scenario())

    assert mocker.call_count == 0


def _items_state(value: str) -> dict[str, Any]:
    return {
        "type": "STREAM",
        "stream": {"stream_descriptor": {"name": "items"}, "stream_state": {"updated_at": value}},
    }


def test_declarative_read_starts_from_the_future_state(tmp_path: Path) -> None:
    """The source reads the input state: without it, the stream reads from its start date."""
    suite = _declarative_suite(tmp_path, [_items_state("2999-01-01")])

    with requests_mock.Mocker() as mocker:
        mocker.get(
            "https://api.example.invalid/items",
            json=[{"id": 1, "updated_at": "2025-01-01"}],
        )
        run_without_skip(suite.test_read_from_future_state, _future_scenario())

    assert mocker.call_count == 0, "a stream whose state is in the future requests nothing"


_COMPONENTS_PY = """
from dataclasses import dataclass

from airbyte_cdk.sources.declarative.transformations import RecordTransformation


@dataclass
class Tag(RecordTransformation):
    def transform(self, record, config=None, stream_state=None, stream_slice=None):
        record["tag"] = "custom"
"""


def test_declarative_read_rebuilds_the_source_with_its_custom_components(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The source rebuilt with the state builds the manifest's `components.py` components."""
    monkeypatch.setenv("AIRBYTE_ENABLE_UNSAFE_CODE", "true")
    manifest = copy.deepcopy(_MANIFEST)
    manifest["streams"][0]["transformations"] = [
        {"type": "CustomTransformation", "class_name": "source_declarative_manifest.components.Tag"}
    ]
    suite = _declarative_suite(tmp_path, [_items_state("2999-01-01")])
    (tmp_path / "manifest.yaml").write_text(yaml.safe_dump(manifest))
    (tmp_path / "components.py").write_text(_COMPONENTS_PY)

    with requests_mock.Mocker() as mocker:
        mocker.get("https://api.example.invalid/items", json=[])
        run_without_skip(suite.test_read_from_future_state, _future_scenario())


def _declarative_source() -> ConcurrentDeclarativeSource:
    return ConcurrentDeclarativeSource(
        config={"api_key": "x"}, catalog=None, state=None, source_config=copy.deepcopy(_MANIFEST)
    )


def _state_output(value: str) -> list[AirbyteMessage]:
    return EntrypointOutput(
        messages=[json.dumps({"type": "STATE", "state": _items_state(value)})]
    ).state_messages


def _items_catalog(tmp_path: Path) -> ConfiguredAirbyteCatalog:
    items = _declarative_source().discover(logging.getLogger(), {}).streams
    return build_future_state_catalog(_future_scenario(), items, tmp_path)


def test_cursor_value_in_the_declared_format_passes(tmp_path: Path) -> None:
    problems = find_cursor_format_problems(
        _declarative_source(), _items_catalog(tmp_path), _state_output("2999-01-01")
    )
    assert problems == []


@pytest.mark.parametrize("value", ["2999-01-01T00:00:00Z", "29990101"])
def test_cursor_value_in_another_format_fails_without_printing_it(
    tmp_path: Path, value: str
) -> None:
    problems = find_cursor_format_problems(
        _declarative_source(), _items_catalog(tmp_path), _state_output(value)
    )

    assert problems == [
        "stream 'items': a `updated_at` value in its state is not in the cursor's format `%Y-%m-%d`"
    ]
    assert value not in problems[0]


def test_declarative_read_fails_on_cursor_format_problems(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The in-process test reports the format problems of the state the read emitted.

    The CDK's own cursors write their state back through the cursor's format, so this stubs the
    check: a value the cursor cannot parse already fails the read.
    """
    checked: list[list[AirbyteMessage]] = []

    def _problems(
        source: object, catalog: ConfiguredAirbyteCatalog, state_messages: list[AirbyteMessage]
    ) -> list[str]:
        assert isinstance(source, ConcurrentDeclarativeSource)
        checked.append(state_messages)
        return ["stream 'items': a `updated_at` value in its state is not in the cursor's format"]

    monkeypatch.setattr(
        "airbyte_cdk.test.standard_tests.source_base.find_cursor_format_problems", _problems
    )
    suite = _declarative_suite(tmp_path, [_items_state("2999-01-01")])

    with requests_mock.Mocker(), pytest.raises(AssertionError) as error:
        suite.test_read_from_future_state(_future_scenario())

    assert "- stream 'items': a `updated_at` value in its state is not in the cursor's format" in (
        str(error.value)
    )
    [state_messages] = checked
    assert [
        m.state.stream.stream_descriptor.name for m in state_messages if m.state and m.state.stream
    ] == ["items"]


class _ConfigReadingSource(ConcurrentDeclarativeSource):
    """A Python declarative source whose `streams` override reads its config, as some do."""

    def streams(self, config: Mapping[str, Any]) -> Any:
        assert config["api_key"] == "x", "the config must be the source's own"
        return super().streams(config)


def test_cursor_format_check_builds_the_streams_with_the_source_config(tmp_path: Path) -> None:
    source = _ConfigReadingSource(
        config={"api_key": "x"}, catalog=None, state=None, source_config=copy.deepcopy(_MANIFEST)
    )

    problems = find_cursor_format_problems(
        source, _items_catalog(tmp_path), _state_output("29990101")
    )

    assert problems == [
        "stream 'items': a `updated_at` value in its state is not in the cursor's format `%Y-%m-%d`"
    ]


def test_cursor_format_check_reports_a_failure_to_build_the_streams(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The failure is one more problem, so the read's own problems are still reported."""
    source = _declarative_source()

    def _fail(config: Mapping[str, Any]) -> Any:
        raise KeyError(SECRET_VALUE)

    monkeypatch.setattr(source, "streams", _fail)

    problems = find_cursor_format_problems(
        source, _items_catalog(tmp_path), _state_output("2999-01-01")
    )

    assert problems == [
        "the cursor format check could not build the source's streams to read their cursors (`KeyError`)"
    ]


def _format_problems(tmp_path: Path, state: dict[str, Any]) -> list[str]:
    output = EntrypointOutput(messages=[json.dumps({"type": "STATE", "state": state})])
    return find_cursor_format_problems(
        _declarative_source(), _items_catalog(tmp_path), output.state_messages
    )


OFF_FORMAT = "29990101"
FORMAT_PROBLEM = (
    "stream 'items': a `updated_at` value in its state is not in the cursor's format `%Y-%m-%d`"
)


def test_cursor_format_is_checked_in_a_global_state(tmp_path: Path) -> None:
    problems = _format_problems(
        tmp_path,
        {
            "type": "GLOBAL",
            "global": {
                "shared_state": {},
                "stream_states": [
                    {
                        "stream_descriptor": {"name": "items"},
                        "stream_state": {"updated_at": OFF_FORMAT},
                    }
                ],
            },
        },
    )

    assert problems == [FORMAT_PROBLEM]


def test_cursor_format_is_checked_in_each_partition(tmp_path: Path) -> None:
    """A partitioned stream's state holds its cursor values under `states[].cursor`."""
    problems = _format_problems(
        tmp_path,
        {
            "type": "STREAM",
            "stream": {
                "stream_descriptor": {"name": "items"},
                "stream_state": {
                    "states": [{"partition": {"id": "1"}, "cursor": {"updated_at": OFF_FORMAT}}],
                    "state": {"updated_at": "2999-01-01"},
                },
            },
        },
    )

    assert problems == [FORMAT_PROBLEM]


def test_cursor_format_is_not_checked_in_parent_states_or_partition_keys(tmp_path: Path) -> None:
    """A parent stream's state and a partition key are not this stream's cursor values."""
    problems = _format_problems(
        tmp_path,
        {
            "type": "STREAM",
            "stream": {
                "stream_descriptor": {"name": "items"},
                "stream_state": {
                    "states": [
                        {
                            "partition": {"updated_at": OFF_FORMAT},
                            "cursor": {"updated_at": "2999-01-01"},
                        }
                    ],
                    "parent_state": {"parents": {"updated_at": OFF_FORMAT}},
                },
            },
        },
    )

    assert problems == []


def test_cursor_format_is_not_checked_for_other_sources(tmp_path: Path) -> None:
    problems = find_cursor_format_problems(
        _StateSource("honours_state", [USERS]),
        _items_catalog(tmp_path),
        _state_output("not a date"),
    )
    assert problems == []


# --- The Docker test ----------------------------------------------------------------------------

DOCKER_ARGS: dict[str, Any] = {
    "connector_image_override": "airbyte/source-test:dev",  # skips the image build
    "connector_base_image_override": None,
    "read_from_streams": "default",
    "read_scenarios": "all",
}


def _stub_docker_read(
    monkeypatch: pytest.MonkeyPatch,
    read_output: list[str],
    *,
    with_events: bool = False,
) -> list[dict[str, Any]]:
    """Answer `discover` and `read`; record the command, catalog and state of each `read`.

    `discover` reports `users` and `static`, and `events` too when `with_events` is set.
    """
    reads: list[dict[str, Any]] = []

    def _fake(cmd: list[str], *, raise_if_errors: bool = False) -> EntrypointOutput:
        if "discover" in cmd:
            streams = [
                stream_json("users", ["full_refresh", "incremental"], ["updated_at"]),
                stream_json("static", ["full_refresh"]),
            ]
            if with_events:
                streams.append(
                    stream_json("events", ["full_refresh", "incremental"], ["created_at"])
                )
            messages = [catalog_message(*streams)]
        else:
            mounts = {
                part.rsplit(":", 1)[1]: Path(part.rsplit(":", 1)[0])
                for part in cmd
                if part.endswith((":/secrets/catalog.json", ":/secrets/state.json"))
            }
            reads.append(
                {
                    "cmd": cmd,
                    "catalog": json.loads(mounts["/secrets/catalog.json"].read_text()),
                    "state": json.loads(mounts["/secrets/state.json"].read_text()),
                }
            )
            messages = read_output
        result = EntrypointOutput(messages=messages, command=cmd)
        if raise_if_errors:
            result.raise_if_errors()
        return result

    monkeypatch.setattr(
        "airbyte_cdk.test.standard_tests.docker_base.run_docker_airbyte_command", _fake
    )
    return reads


def _docker_suite(
    tmp_path: Path, state: list[Any], acceptance_tests: dict[str, Any] | None = None
) -> DockerConnectorTestSuite:
    suite = suite_class(tmp_path, acceptance_tests or {}, base=DockerConnectorTestSuite)()
    state_path = suite.get_connector_root_dir() / STATE_PATH
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state))
    return suite


def test_docker_read_passes_the_state_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state = [_stream_state("users")]
    reads = _stub_docker_read(
        monkeypatch, [json.dumps({"type": "STATE", "state": _stream_state("users")})]
    )

    run_without_skip(
        _docker_suite(tmp_path, state).test_docker_image_build_and_read_from_future_state,
        _future_scenario(),
        **DOCKER_ARGS,
    )

    [read] = reads
    cmd = read["cmd"]
    assert cmd[cmd.index("--state") + 1] == "/secrets/state.json"
    assert read["state"] == state
    assert [
        (s["stream"]["name"], s["sync_mode"], s["cursor_field"]) for s in read["catalog"]["streams"]
    ] == [("users", "incremental", ["updated_at"])]


def test_docker_read_fails_when_the_connector_ignores_its_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_docker_read(
        monkeypatch,
        [
            record_message("users"),
            json.dumps({"type": "STATE", "state": _stream_state("users")}),
        ],
    )

    with pytest.raises(AssertionError) as error:
        _docker_suite(
            tmp_path, [_stream_state("users")]
        ).test_docker_image_build_and_read_from_future_state(_future_scenario(), **DOCKER_ARGS)

    assert "stream 'users' returned 1 record(s)" in str(error.value)
    assert SECRET_VALUE not in str(error.value)


def test_docker_read_is_off_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reads = _stub_docker_read(monkeypatch, [])

    with pytest.raises(pytest.skip.Exception, match="off unless `--read-from-streams`"):
        _docker_suite(
            tmp_path, [_stream_state("users")]
        ).test_docker_image_build_and_read_from_future_state(
            _future_scenario(), **{**DOCKER_ARGS, "read_from_streams": "none"}
        )
    assert reads == []


def test_docker_read_runs_in_image_test_like_the_docker_read() -> None:
    """`airbyte-cdk image test` selects `-m image_tests`; connector CI reads through it."""

    def _marks(test: Any) -> list[str]:
        return sorted(mark.name for mark in test.pytestmark)

    assert _marks(DockerConnectorTestSuite.test_docker_image_build_and_read_from_future_state) == (
        _marks(DockerConnectorTestSuite.test_docker_image_build_and_read)
    )
    assert "image_tests" in _marks(
        DockerConnectorTestSuite.test_docker_image_build_and_read_from_future_state
    )


# The connector's configs: `config` reads from a future state, `other` only under `basic_read`.
SCENARIO_CONFIGS: dict[str, Any] = {
    "basic_read": {"tests": [entry("secrets/other.json")]},
    "incremental": {"tests": [_incremental_entry()]},
}


@pytest.mark.parametrize("read_scenarios", ["default", "all", ["config"]])
def test_docker_read_reads_every_future_state_entry_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, read_scenarios: Any
) -> None:
    """`default` reads every entry, as the in-process test does: each one is declared for it."""
    reads = _stub_docker_read(
        monkeypatch, [json.dumps({"type": "STATE", "state": _stream_state("users")})]
    )

    run_without_skip(
        _docker_suite(
            tmp_path, [_stream_state("users")], SCENARIO_CONFIGS
        ).test_docker_image_build_and_read_from_future_state,
        _future_scenario(),
        **{**DOCKER_ARGS, "read_scenarios": read_scenarios},
    )

    assert len(reads) == 1


def test_docker_read_skips_a_config_left_out_of_read_scenarios(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A name that only the Docker read reads picks that config there, and skips this one."""
    reads = _stub_docker_read(monkeypatch, [])

    with pytest.raises(pytest.skip.Exception, match="not in `--read-scenarios=other`"):
        _docker_suite(
            tmp_path, [_stream_state("users")], SCENARIO_CONFIGS
        ).test_docker_image_build_and_read_from_future_state(
            _future_scenario(), **{**DOCKER_ARGS, "read_scenarios": ["other"]}
        )
    assert reads == []


def test_docker_read_fails_on_an_unknown_read_scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A misspelled config name must not skip every future-state read."""
    reads = _stub_docker_read(monkeypatch, [])

    with pytest.raises(pytest.fail.Exception, match="names configs that are not listed"):
        run_without_skip(
            _docker_suite(
                tmp_path, [_stream_state("users")], SCENARIO_CONFIGS
            ).test_docker_image_build_and_read_from_future_state,
            _future_scenario(),
            **{**DOCKER_ARGS, "read_scenarios": ["confg"]},
        )
    assert reads == []


def test_read_scenarios_name_a_future_state_config_for_the_docker_read(tmp_path: Path) -> None:
    """One `--read-scenarios` list picks the configs of both Docker reads."""
    suite = _docker_suite(tmp_path, [], SCENARIO_CONFIGS)

    with pytest.raises(pytest.skip.Exception, match="not in `--read-scenarios=config`"):
        suite.test_docker_image_build_and_read(
            ConnectorTestScenario(
                config_path=Path("secrets/other.json"),
                config_dict={},
                sections=("basic_read",),
            ),
            **{**DOCKER_ARGS, "read_scenarios": ["config"]},
        )


def test_docker_read_needs_state_only_for_the_streams_it_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`--read-from-streams` narrows the read before the state file's coverage is checked."""
    reads = _stub_docker_read(
        monkeypatch,
        [json.dumps({"type": "STATE", "state": _stream_state("users")})],
        with_events=True,
    )

    run_without_skip(
        _docker_suite(
            tmp_path, [_stream_state("users")]
        ).test_docker_image_build_and_read_from_future_state,
        _future_scenario(),
        **{**DOCKER_ARGS, "read_from_streams": ["users"]},
    )

    [read] = reads
    assert [s["stream"]["name"] for s in read["catalog"]["streams"]] == ["users"]


def test_docker_read_is_off_with_read_scenarios_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reads = _stub_docker_read(monkeypatch, [])

    with pytest.raises(pytest.skip.Exception, match="disabled with `--read-scenarios`"):
        _docker_suite(
            tmp_path, [_stream_state("users")]
        ).test_docker_image_build_and_read_from_future_state(
            _future_scenario(), **{**DOCKER_ARGS, "read_scenarios": "none"}
        )
    assert reads == []


def test_docker_read_skips_a_stream_list_without_an_incremental_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reads = _stub_docker_read(monkeypatch, [])

    with pytest.raises(pytest.skip.Exception, match="is an incremental stream it reads"):
        _docker_suite(
            tmp_path, [_stream_state("users")]
        ).test_docker_image_build_and_read_from_future_state(
            _future_scenario(), **{**DOCKER_ARGS, "read_from_streams": ["static"]}
        )
    assert reads == []


def test_docker_read_fails_on_a_stream_without_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reads = _stub_docker_read(monkeypatch, [], with_events=True)

    with pytest.raises(AssertionError, match="has no state for the incremental stream"):
        _docker_suite(
            tmp_path, [_stream_state("users")]
        ).test_docker_image_build_and_read_from_future_state(_future_scenario(), **DOCKER_ARGS)
    assert reads == []
