# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Each config in `acceptance-test-config.yml` runs the command of the section that lists it.

`spec` validates the config against the spec, `connection` runs `check`, `discovery` runs
`discover`, `basic_read` and `full_refresh` run a full-refresh `read`, and `incremental` runs an
incremental `read`. A config listed under several sections runs the union of their commands.
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, TypeVar, cast

import pytest
import yaml
from pydantic import ValidationError

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
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
from airbyte_cdk.test.entrypoint_wrapper import AirbyteEntrypointException, EntrypointOutput
from airbyte_cdk.test.models.scenario import (
    ALL_COMMANDS,
    SCENARIO_SECTIONS,
    SECTION_COMMANDS,
    ConnectorTestScenario,
    ScenarioCommand,
)
from airbyte_cdk.test.standard_tests import ConnectorTestSuiteBase, SourceTestSuiteBase
from airbyte_cdk.test.standard_tests._assertions import assert_config_matches_spec
from airbyte_cdk.test.standard_tests._job_runner import IConnector
from airbyte_cdk.test.standard_tests.docker_base import (
    DockerConnectorTestSuite,
    skip_unless_runs,
)
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG

POKEAPI_RESOURCE_DIR = Path(__file__).parent.parent / "resources" / "source_pokeapi_w_components_py"
IMAGE = "airbyte/source-test:dev"

SPEC_WITH_LONG_API_KEY: dict[str, Any] = {
    "type": "object",
    "required": ["api_key"],
    "properties": {"api_key": {"type": "string", "minLength": 20}},
}
VALID_CONFIG = {"api_key": "x" * 20}
# Too short for the spec: validation fails on the value itself, which must not leak.
SECRET_CONFIG = {"api_key": "hunter2-secret"}

USERS = AirbyteStream(
    name="users",
    json_schema={"type": "object"},
    supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
    source_defined_cursor=True,
    default_cursor_field=["updated_at"],
)
# Incremental with a user-defined cursor: the catalog cannot know it, so it is read full refresh.
EVENTS = AirbyteStream(
    name="events",
    json_schema={"type": "object"},
    supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
)
STATIC = AirbyteStream(
    name="static",
    json_schema={"type": "object"},
    supported_sync_modes=[SyncMode.full_refresh],
)


class _Reached(Exception):
    """Raised by a fake connector or Docker command to prove a test body ran past its skips."""


_SuiteT = TypeVar("_SuiteT", bound=DockerConnectorTestSuite)


def _suite_class(
    tmp_path: Path,
    acceptance_tests: dict[str, Any],
    *,
    base: type[_SuiteT],
    make_connector: Callable[[], IConnector] | None = None,
) -> type[_SuiteT]:
    """Build a suite class over a temporary connector dir holding `acceptance_tests`.

    `create_connector` returns `make_connector()`, or raises `_Reached` when none is given.
    """
    connector_root = tmp_path / "source-test"
    connector_root.mkdir(exist_ok=True)
    (connector_root / ACCEPTANCE_TEST_CONFIG).write_text(
        yaml.safe_dump({"acceptance_tests": acceptance_tests})
    )
    shutil.copy(POKEAPI_RESOURCE_DIR / "metadata.yaml", connector_root / "metadata.yaml")

    def get_connector_root_dir(cls: type) -> Path:
        return connector_root

    def create_connector(self: Any, scenario: ConnectorTestScenario | None) -> IConnector:
        if make_connector is None:
            raise _Reached(scenario.id if scenario else None)
        return make_connector()

    return cast(
        type[_SuiteT],
        type(
            "_Suite",
            (base,),
            {
                "get_connector_root_dir": classmethod(get_connector_root_dir),
                "create_connector": create_connector,
            },
        ),
    )


def _scenario(
    *sections: str,
    config: dict[str, Any] | None = None,
    name: str = "config",
    **extra: Any,
) -> ConnectorTestScenario:
    """A scenario over an inline config, listed under `sections` (none: built by hand).

    The config defaults to `VALID_CONFIG`: an empty config dict counts as no config at all.
    """
    return ConnectorTestScenario(
        config_path=Path(f"secrets/{name}.json"),
        config_dict=VALID_CONFIG if config is None else config,
        sections=tuple(sections),
        **extra,
    )


def _entry(config_path: str, **extra: Any) -> dict[str, Any]:
    return {"config_path": config_path, **extra}


ALL_SECTIONS_CONFIG: dict[str, Any] = {
    "spec": {"tests": [_entry("secrets/config.json")]},
    "connection": {
        "tests": [
            _entry("secrets/config.json", status="succeed"),
            _entry("integration_tests/invalid_config.json", status="failed"),
            _entry("secrets/config_iam_role.json", status="succeed"),
            {"status": "succeed"},  # no config_path: ignored
        ]
    },
    "discovery": {
        "tests": [
            _entry(
                "secrets/config.json",
                backward_compatibility_tests_config={"disable_for_version": "1.0.0"},
            ),
            _entry("secrets/config_old.json"),
        ]
    },
    "basic_read": {
        "tests": [
            _entry(
                "secrets/config.json",
                empty_streams=[{"name": "empty_stream", "bypass_reason": "no data"}],
            ),
            _entry("secrets/config_read_only.json"),
        ]
    },
    "full_refresh": {"tests": [_entry("secrets/config_full_refresh.json")]},
    "incremental": {
        "tests": [
            _entry(
                "secrets/config_incremental.json",
                configured_catalog_path="integration_tests/incremental_catalog.json",
                future_state={"future_state_path": "integration_tests/abnormal_state.json"},
            ),
            _entry("secrets/config_read_only.json"),
        ]
    },
}


# --- Section to command mapping -------------------------------------------------------------


def test_every_section_maps_to_the_command_cat_ran_it_with() -> None:
    assert dict(SECTION_COMMANDS) == {
        "spec": {"spec"},
        "connection": {"check"},
        "discovery": {"discover"},
        "basic_read": {"read"},
        "full_refresh": {"read"},
        "incremental": {"incremental_read"},
    }
    assert SCENARIO_SECTIONS == tuple(SECTION_COMMANDS)
    assert ALL_COMMANDS == {"spec", "check", "discover", "read", "incremental_read"}


def test_get_scenarios_collects_every_section(tmp_path: Path) -> None:
    suite = _suite_class(tmp_path, ALL_SECTIONS_CONFIG, base=DockerConnectorTestSuite)
    scenarios = {str(s.config_path): s for s in suite.get_scenarios()}

    assert sorted(scenarios) == [
        "integration_tests/invalid_config.json",
        "secrets/config.json",
        "secrets/config_full_refresh.json",
        "secrets/config_incremental.json",
        "secrets/config_old.json",
        "secrets/config_read_only.json",
    ], "iam_role configs and entries without config_path are skipped; the rest are deduplicated"

    full = scenarios["secrets/config.json"]
    # Sections are recorded in collection order (`SCENARIO_SECTIONS`), not file order.
    assert full.sections == ("spec", "connection", "discovery", "basic_read")
    assert full.commands == {"spec", "check", "discover", "read"}
    assert full.status == "succeed"
    assert [s.name for s in full.empty_streams or []] == ["empty_stream"]

    invalid = scenarios["integration_tests/invalid_config.json"]
    assert invalid.sections == ("connection",)
    assert invalid.commands == {"check"}
    assert invalid.status == "failed"

    read_only = scenarios["secrets/config_read_only.json"]
    assert read_only.sections == ("basic_read", "incremental")
    assert read_only.commands == {"read", "incremental_read"}

    for config_path, section, command in [
        ("secrets/config_old.json", "discovery", "discover"),
        ("secrets/config_full_refresh.json", "full_refresh", "read"),
        ("secrets/config_incremental.json", "incremental", "incremental_read"),
    ]:
        scenario = scenarios[config_path]
        assert scenario.sections == (section,)
        assert scenario.commands == {command}
        assert scenario.status is None, "no section but `connection` declares a status"


def test_dedup_scenarios_unions_sections() -> None:
    deduped = DockerConnectorTestSuite._dedup_scenarios(
        [
            ConnectorTestScenario(config_path=Path("secrets/config.json"), sections=("discovery",)),
            ConnectorTestScenario(
                config_path=Path("secrets/config.json"), sections=("connection",)
            ),
            ConnectorTestScenario(config_path=Path("secrets/config.json"), sections=("discovery",)),
        ]
    )
    assert len(deduped) == 1
    assert deduped[0].sections == ("discovery", "connection")
    assert deduped[0].commands == {"discover", "check"}


@pytest.mark.parametrize(
    "sections, expected",
    [
        pytest.param((), ALL_COMMANDS, id="hand_built_scenario_runs_everything"),
        pytest.param(("spec",), {"spec"}, id="spec"),
        pytest.param(("connection",), {"check"}, id="connection"),
        pytest.param(("discovery",), {"discover"}, id="discovery"),
        pytest.param(("basic_read",), {"read"}, id="basic_read"),
        pytest.param(("full_refresh",), {"read"}, id="full_refresh"),
        pytest.param(("incremental",), {"incremental_read"}, id="incremental"),
        pytest.param(("incremental", "basic_read"), {"read", "incremental_read"}, id="mixed"),
        pytest.param(SCENARIO_SECTIONS, ALL_COMMANDS, id="every_section"),
    ],
)
def test_commands_property(sections: tuple[str, ...], expected: set[ScenarioCommand]) -> None:
    scenario = _scenario(*sections)
    assert scenario.commands == expected
    for command in ALL_COMMANDS:
        assert scenario.runs(command) is (command in expected)
    assert scenario.runs(*ALL_COMMANDS)
    # Derived scenarios keep the sections, so the expectation helpers cannot change the commands.
    for derived in (
        scenario.without_expected_outcome(),
        scenario.with_expecting_failure(),
        scenario.with_expecting_success(),
        scenario.with_default_success(),
    ):
        assert derived.sections == scenario.sections
        assert derived.commands == expected


def test_unknown_section_is_rejected() -> None:
    with pytest.raises(ValidationError, match="Unknown `acceptance-test-config.yml` section"):
        _scenario("sequential_reads")


@pytest.mark.parametrize(
    "status, expected",
    [
        pytest.param(None, "succeed", id="no_status_defaults_to_succeed"),
        pytest.param("succeed", "succeed", id="succeed"),
        pytest.param("failed", "failed", id="failed"),
        pytest.param("exception", "exception", id="exception"),
    ],
)
def test_with_default_success(status: str | None, expected: str) -> None:
    scenario = _scenario("discovery", status=status)
    assert scenario.with_default_success().status == expected
    assert scenario.with_default_success().expected_outcome.expect_success() is (
        expected == "succeed"
    )


def test_skip_unless_runs_names_the_commands_and_sections() -> None:
    incremental_only = _scenario("incremental", name="config_incremental")

    with pytest.raises(pytest.skip.Exception) as exc_info:
        skip_unless_runs(incremental_only, "read")
    assert str(exc_info.value) == (
        "`read` does not run for scenario 'config_incremental': its config is listed under "
        "`incremental` in `acceptance-test-config.yml`, which runs `incremental_read`."
    )

    skip_unless_runs(incremental_only, "read", "incremental_read")  # no skip
    for command in ALL_COMMANDS:
        skip_unless_runs(_scenario(), command)  # hand-built scenarios never skip


# --- Which test runs for which section --------------------------------------------------------

SUITE_TEST_COMMANDS: dict[str, frozenset[ScenarioCommand]] = {
    "test_check": frozenset({"check"}),
    "test_config_matches_spec": frozenset({"spec"}),
    "test_discover": frozenset({"discover"}),
    "test_basic_read": frozenset({"read"}),
    "test_incremental_read": frozenset({"incremental_read"}),
    "test_fail_read_with_bad_catalog": frozenset({"read", "incremental_read"}),
}


@pytest.mark.parametrize("section", SCENARIO_SECTIONS)
@pytest.mark.parametrize(
    "test_name, commands", SUITE_TEST_COMMANDS.items(), ids=SUITE_TEST_COMMANDS
)
def test_source_suite_runs_each_test_for_its_sections(
    tmp_path: Path, test_name: str, commands: frozenset[ScenarioCommand], section: str
) -> None:
    suite = _suite_class(tmp_path, ALL_SECTIONS_CONFIG, base=SourceTestSuiteBase)()
    scenario = _scenario(section)

    if SECTION_COMMANDS[section] & commands:
        with pytest.raises(_Reached):
            getattr(suite, test_name)(scenario)
    else:
        with pytest.raises(pytest.skip.Exception, match=r"do(es)? not run for scenario"):
            getattr(suite, test_name)(scenario)


@pytest.mark.parametrize("test_name", SUITE_TEST_COMMANDS)
def test_source_suite_runs_every_test_for_hand_built_scenarios(
    tmp_path: Path, test_name: str
) -> None:
    suite = _suite_class(tmp_path, ALL_SECTIONS_CONFIG, base=SourceTestSuiteBase)()
    with pytest.raises(_Reached):
        getattr(suite, test_name)(_scenario())


@pytest.mark.parametrize("test_name", ["test_check", "test_config_matches_spec"])
def test_connector_suite_gates_its_tests(tmp_path: Path, test_name: str) -> None:
    suite = _suite_class(tmp_path, ALL_SECTIONS_CONFIG, base=ConnectorTestSuiteBase)()
    with pytest.raises(_Reached):
        getattr(suite, test_name)(_scenario("connection", "spec"))
    with pytest.raises(pytest.skip.Exception, match=r"do(es)? not run for scenario"):
        getattr(suite, test_name)(_scenario("discovery"))


DOCKER_TEST_COMMANDS: dict[str, frozenset[ScenarioCommand]] = {
    "test_docker_image_build_and_check": frozenset({"check"}),
    "test_docker_image_build_and_config_matches_spec": frozenset({"spec"}),
    "test_docker_image_build_and_discover": frozenset({"discover"}),
    "test_docker_image_build_and_read": frozenset({"read", "incremental_read"}),
}


def _docker_kwargs(test_name: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "connector_image_override": IMAGE,  # skips the image build
        "connector_base_image_override": None,
    }
    if test_name == "test_docker_image_build_and_read":
        kwargs |= {"read_from_streams": "all", "read_scenarios": "all"}
    return kwargs


@pytest.mark.parametrize("section", SCENARIO_SECTIONS)
@pytest.mark.parametrize(
    "test_name, commands", DOCKER_TEST_COMMANDS.items(), ids=DOCKER_TEST_COMMANDS
)
def test_docker_suite_runs_each_test_for_its_sections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    test_name: str,
    commands: frozenset[ScenarioCommand],
    section: str,
) -> None:
    def _reached(cmd: list[str], *, raise_if_errors: bool = False) -> EntrypointOutput:
        raise _Reached(cmd)

    monkeypatch.setattr(
        "airbyte_cdk.test.standard_tests.docker_base.run_docker_airbyte_command", _reached
    )
    suite = _suite_class(tmp_path, ALL_SECTIONS_CONFIG, base=DockerConnectorTestSuite)()
    scenario = _scenario(section)

    if SECTION_COMMANDS[section] & commands:
        with pytest.raises(_Reached):
            getattr(suite, test_name)(scenario, **_docker_kwargs(test_name))
    else:
        with pytest.raises(pytest.skip.Exception, match=r"do(es)? not run for scenario"):
            getattr(suite, test_name)(scenario, **_docker_kwargs(test_name))


# --- `spec` sections: the config must validate against the spec -------------------------------


class _SpecSource(Source):
    """A source whose spec requires a 20-character `api_key`; the other commands are inert."""

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(connectionSpecification=SPEC_WITH_LONG_API_KEY)

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        return AirbyteCatalog(streams=[])

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterable[AirbyteMessage]:
        yield from []


@pytest.mark.parametrize("base", [SourceTestSuiteBase, ConnectorTestSuiteBase])
def test_config_matches_spec_validates_the_config_without_leaking_it(
    tmp_path: Path, base: type[ConnectorTestSuiteBase]
) -> None:
    suite = _suite_class(tmp_path, {}, base=base, make_connector=_SpecSource)()

    suite.test_config_matches_spec(_scenario("spec", config=VALID_CONFIG))

    with pytest.raises(
        AssertionError,
        match=r"Config 'config' does not match the spec of connector 'source-test' at `\$\.api_key`",
    ) as exc_info:
        suite.test_config_matches_spec(_scenario("spec", config=SECRET_CONFIG))
    assert "hunter2" not in str(exc_info.value), "the offending config value must be redacted"
    assert "<config value> is too short" in str(exc_info.value)

    with pytest.raises(AssertionError, match="'api_key' is a required property"):
        suite.test_config_matches_spec(_scenario("spec", config={}))


def test_assert_config_matches_spec_requires_a_spec_message() -> None:
    with pytest.raises(AssertionError, match="(?i)spec"):
        assert_config_matches_spec(
            config=VALID_CONFIG,
            spec_result=EntrypointOutput(messages=[], command=["spec"]),
            connector_name="source-test",
            scenario_id="config",
        )


# --- `discovery`, `basic_read` and `incremental` sections ----------------------------------------


class _ReadSource(Source):
    """A source over `streams` whose `read` behaves as instructed.

    Behaviours:
    - `records_and_state`: one record per configured stream, one STATE per incremental stream.
    - `records_only`: records without state.
    - `no_records`: nothing at all.
    - `raise`: an uncaught error during `read`.
    - `discover_raises`: an uncaught error during `discover`.

    Every configured catalog passed to `read` is appended to `read_catalogs`.
    """

    def __init__(
        self,
        behavior: str,
        streams: list[AirbyteStream],
        read_catalogs: list[ConfiguredAirbyteCatalog],
    ) -> None:
        self._behavior = behavior
        self._streams = streams
        self._read_catalogs = read_catalogs

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(
            connectionSpecification={"type": "object", "properties": {}},
        )

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        if self._behavior == "discover_raises":
            raise RuntimeError("Discover exploded.")
        return AirbyteCatalog(streams=list(self._streams))

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterable[AirbyteMessage]:
        self._read_catalogs.append(catalog)
        if self._behavior == "raise":
            raise RuntimeError("Read exploded.")
        if self._behavior == "no_records":
            return
        for configured_stream in catalog.streams:
            name = configured_stream.stream.name
            yield AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream=name, data={"id": 1, "updated_at": "2026-01-01"}, emitted_at=0
                ),
            )
            if (
                configured_stream.sync_mode == SyncMode.incremental
                and self._behavior == "records_and_state"
            ):
                yield AirbyteMessage(
                    type=Type.STATE,
                    state=AirbyteStateMessage(
                        type=AirbyteStateType.STREAM,
                        stream=AirbyteStreamState(
                            stream_descriptor=StreamDescriptor(name=name),
                            stream_state=AirbyteStateBlob(updated_at="2026-01-01"),
                        ),
                    ),
                )


def _read_suite(
    tmp_path: Path,
    behavior: str,
    streams: list[AirbyteStream],
    read_catalogs: list[ConfiguredAirbyteCatalog] | None = None,
) -> SourceTestSuiteBase:
    catalogs = [] if read_catalogs is None else read_catalogs
    return _suite_class(
        tmp_path,
        {},
        base=SourceTestSuiteBase,
        make_connector=lambda: _ReadSource(behavior, streams, catalogs),
    )()


def _configured(catalog: ConfiguredAirbyteCatalog) -> list[tuple[str, SyncMode, list[str] | None]]:
    return [(s.stream.name, s.sync_mode, s.cursor_field) for s in catalog.streams]


def test_discover_fails_on_errors_without_a_declared_status(tmp_path: Path) -> None:
    suite = _read_suite(tmp_path, "discover_raises", [USERS])
    with pytest.raises(AirbyteEntrypointException, match="Discover exploded"):
        suite.test_discover(_scenario("discovery"))


def test_discover_fails_on_an_empty_catalog(tmp_path: Path) -> None:
    suite = _read_suite(tmp_path, "records_and_state", [])
    with pytest.raises(ValueError, match="Discovered catalog for connector 'source-test' is empty"):
        suite.test_discover(_scenario("discovery"))


def test_basic_read_reads_every_stream_full_refresh(tmp_path: Path) -> None:
    catalogs: list[ConfiguredAirbyteCatalog] = []
    suite = _read_suite(tmp_path, "records_and_state", [USERS, EVENTS, STATIC], catalogs)
    suite.test_basic_read(
        _scenario("basic_read", empty_streams=[{"name": "static", "bypass_reason": "no data"}])
    )
    assert _configured(catalogs[-1]) == [
        ("users", SyncMode.full_refresh, None),
        ("events", SyncMode.full_refresh, None),
    ]


@pytest.mark.parametrize(
    "behavior, expected_exception, match",
    [
        pytest.param(
            "no_records", AssertionError, "Expected records but got none", id="no_records"
        ),
        pytest.param("raise", AirbyteEntrypointException, "Read exploded", id="read_raises"),
        pytest.param(
            "discover_raises", AirbyteEntrypointException, "Discover exploded", id="discover_raises"
        ),
    ],
)
def test_basic_read_fails_without_a_declared_status(
    tmp_path: Path, behavior: str, expected_exception: type[Exception], match: str
) -> None:
    """No `status` means `succeed`: a read that errors or returns nothing fails."""
    suite = _read_suite(tmp_path, behavior, [USERS])
    with pytest.raises(expected_exception, match=match):
        suite.test_basic_read(_scenario("basic_read"))


def test_incremental_read_reads_incremental_streams_and_requires_state(tmp_path: Path) -> None:
    catalogs: list[ConfiguredAirbyteCatalog] = []
    suite = _read_suite(tmp_path, "records_and_state", [USERS, EVENTS, STATIC], catalogs)

    suite.test_incremental_read(_scenario("incremental"))

    # Only streams with a cursor the catalog can name are read, and in incremental mode.
    assert _configured(catalogs[-1]) == [("users", SyncMode.incremental, ["updated_at"])]


def test_incremental_read_fails_without_incremental_streams(tmp_path: Path) -> None:
    suite = _read_suite(tmp_path, "records_and_state", [STATIC])
    with pytest.raises(AssertionError, match="no discovered stream supports incremental sync"):
        suite.test_incremental_read(_scenario("incremental"))


def test_incremental_read_fails_without_state(tmp_path: Path) -> None:
    suite = _read_suite(tmp_path, "records_only", [USERS])
    with pytest.raises(AssertionError, match="emitted no STATE message"):
        suite.test_incremental_read(_scenario("incremental"))


def test_incremental_read_skips_when_every_incremental_stream_is_empty(tmp_path: Path) -> None:
    suite = _read_suite(tmp_path, "records_and_state", [USERS, STATIC])
    with pytest.raises(pytest.skip.Exception, match="listed in `empty_streams`"):
        suite.test_incremental_read(
            _scenario("incremental", empty_streams=[{"name": "users", "bypass_reason": "none"}])
        )


# --- Docker path -------------------------------------------------------------------------------


def _stream_json(
    name: str, sync_modes: list[str], cursor: list[str] | None = None
) -> dict[str, Any]:
    stream: dict[str, Any] = {
        "name": name,
        "json_schema": {"type": "object"},
        "supported_sync_modes": sync_modes,
    }
    if cursor:
        stream |= {"source_defined_cursor": True, "default_cursor_field": cursor}
    return stream


SPEC_MESSAGE = json.dumps(
    {"type": "SPEC", "spec": {"connectionSpecification": SPEC_WITH_LONG_API_KEY}}
)
TRACE_ERROR_MESSAGE = json.dumps(
    {
        "type": "TRACE",
        "trace": {"type": "ERROR", "emitted_at": 0, "error": {"message": "Command exploded."}},
    }
)


def _catalog_message(*streams: dict[str, Any]) -> str:
    return json.dumps({"type": "CATALOG", "catalog": {"streams": list(streams)}})


def _record_message(stream: str) -> str:
    return json.dumps(
        {"type": "RECORD", "record": {"stream": stream, "data": {"id": 1}, "emitted_at": 0}}
    )


def _stub_docker(
    monkeypatch: pytest.MonkeyPatch,
    responses: dict[str, list[str]],
    read_catalogs: list[dict[str, Any]] | None = None,
) -> list[list[str]]:
    """Answer each connector command run "in Docker" with canned messages; record the commands.

    The configured catalog mounted into each `read` is appended to `read_catalogs`.
    """
    calls: list[list[str]] = []

    def _fake_run_docker_airbyte_command(
        cmd: list[str], *, raise_if_errors: bool = False
    ) -> EntrypointOutput:
        calls.append(cmd)
        command = next(part for part in cmd if part in ("spec", "check", "discover", "read"))
        if command == "read" and read_catalogs is not None:
            mount = next(part for part in cmd if part.endswith(":/secrets/catalog.json"))
            read_catalogs.append(json.loads(Path(mount.rsplit(":", 1)[0]).read_text()))
        result = EntrypointOutput(messages=responses[command], command=cmd)
        if raise_if_errors:
            result.raise_if_errors()
        return result

    monkeypatch.setattr(
        "airbyte_cdk.test.standard_tests.docker_base.run_docker_airbyte_command",
        _fake_run_docker_airbyte_command,
    )
    return calls


def test_docker_config_matches_spec(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _stub_docker(monkeypatch, {"spec": [SPEC_MESSAGE]})
    suite = _suite_class(tmp_path, {}, base=DockerConnectorTestSuite)()
    kwargs = _docker_kwargs("test_docker_image_build_and_config_matches_spec")

    suite.test_docker_image_build_and_config_matches_spec(
        _scenario("spec", config=VALID_CONFIG), **kwargs
    )
    assert calls[-1][-2:] == [IMAGE, "spec"]

    with pytest.raises(AssertionError, match="does not match the spec") as exc_info:
        suite.test_docker_image_build_and_config_matches_spec(
            _scenario("spec", config=SECRET_CONFIG), **kwargs
        )
    assert "hunter2" not in str(exc_info.value)


def test_docker_discover(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _stub_docker(
        monkeypatch, {"discover": [_catalog_message(_stream_json("users", ["full_refresh"]))]}
    )
    suite = _suite_class(tmp_path, {}, base=DockerConnectorTestSuite)()

    suite.test_docker_image_build_and_discover(
        _scenario("discovery"), **_docker_kwargs("test_docker_image_build_and_discover")
    )
    assert calls[-1][-4:-1] == [IMAGE, "discover", "--config"]


@pytest.mark.parametrize(
    "messages, expected_exception, match",
    [
        pytest.param([_catalog_message()], ValueError, "is empty", id="empty_catalog"),
        pytest.param([TRACE_ERROR_MESSAGE], AirbyteEntrypointException, "exploded", id="error"),
    ],
)
def test_docker_discover_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    messages: list[str],
    expected_exception: type[Exception],
    match: str,
) -> None:
    _stub_docker(monkeypatch, {"discover": messages})
    suite = _suite_class(tmp_path, {}, base=DockerConnectorTestSuite)()
    with pytest.raises(expected_exception, match=match):
        suite.test_docker_image_build_and_discover(
            _scenario("discovery"), **_docker_kwargs("test_docker_image_build_and_discover")
        )


FULL_REFRESH_CATALOG = [("users", "full_refresh", None), ("events", "full_refresh", None)]


@pytest.mark.parametrize(
    "sections, expected",
    [
        pytest.param(("basic_read",), FULL_REFRESH_CATALOG, id="basic_read"),
        pytest.param(("full_refresh",), FULL_REFRESH_CATALOG, id="full_refresh"),
        pytest.param(("basic_read", "incremental"), FULL_REFRESH_CATALOG, id="mixed"),
        # Listed only under `incremental`: streams with a known cursor are read incrementally.
        pytest.param(
            ("incremental",),
            [("users", "incremental", ["updated_at"]), ("events", "full_refresh", None)],
            id="incremental",
        ),
    ],
)
def test_docker_read_configures_the_catalog_for_the_section(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sections: tuple[str, ...],
    expected: list[tuple[str, str, list[str] | None]],
) -> None:
    catalogs: list[dict[str, Any]] = []
    _stub_docker(
        monkeypatch,
        {
            "discover": [
                _catalog_message(
                    _stream_json("users", ["full_refresh", "incremental"], ["updated_at"]),
                    _stream_json("events", ["full_refresh", "incremental"]),
                    _stream_json("static", ["full_refresh"]),
                )
            ],
            "read": [_record_message("users"), _record_message("events")],
        },
        read_catalogs=catalogs,
    )
    suite = _suite_class(tmp_path, {}, base=DockerConnectorTestSuite)()

    suite.test_docker_image_build_and_read(
        # `empty_streams` used to crash the Docker read (names compared as streams).
        _scenario(*sections, empty_streams=[{"name": "static", "bypass_reason": "no data"}]),
        **_docker_kwargs("test_docker_image_build_and_read"),
    )

    assert [
        (s["stream"]["name"], s["sync_mode"], s.get("cursor_field"))
        for s in catalogs[-1]["streams"]
    ] == expected
