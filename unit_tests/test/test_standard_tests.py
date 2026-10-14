# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for FAST Airbyte Standard Tests."""

from pathlib import Path
from typing import Any

import pytest

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.parsers.custom_code_compiler import validate_python_code
from airbyte_cdk.sources.source import Source
from airbyte_cdk.test.models.scenario import ConnectorTestScenario
from airbyte_cdk.test.standard_tests._job_runner import IConnector
from airbyte_cdk.test.standard_tests.connector_base import ConnectorTestSuiteBase
from airbyte_cdk.test.standard_tests.declarative_sources import md5_checksum
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.test.standard_tests.pytest_hooks import _scenario_test_ids


@pytest.mark.parametrize(
    "input, expected",
    [
        (ConcurrentDeclarativeSource, True),
        (Source, True),
        (None, False),
        ("", False),
        ([], False),
        ({}, False),
        (object(), False),
    ],
)
def test_is_iconnector_check(input: Any, expected: bool) -> None:
    """Assert whether inputs are valid as an IConnector object or class."""
    if isinstance(input, type):
        assert issubclass(input, IConnector) == expected
        return

    assert isinstance(input, IConnector) == expected


@pytest.mark.parametrize(
    "scenarios, expected_statuses",
    [
        pytest.param(
            [
                ConnectorTestScenario(config_path=Path("integration_tests/config.json")),
                ConnectorTestScenario(
                    config_path=Path("integration_tests/config.json"), status="succeed"
                ),
            ],
            ["succeed"],
            id="statusless_spec_entry_inherits_connection_status",
        ),
        pytest.param(
            [
                ConnectorTestScenario(
                    config_path=Path("integration_tests/config.json"), status="failed"
                ),
                ConnectorTestScenario(config_path=Path("integration_tests/config.json")),
            ],
            ["failed"],
            id="declared_status_survives_statusless_duplicate",
        ),
        pytest.param(
            [
                ConnectorTestScenario(config_path=Path("integration_tests/config.json")),
                ConnectorTestScenario(config_path=Path("secrets/config.json"), status="succeed"),
            ],
            [None, "succeed"],
            id="different_configs_not_merged",
        ),
    ],
)
def test_dedup_scenarios_merges_status(
    scenarios: list[ConnectorTestScenario],
    expected_statuses: list[str | None],
) -> None:
    deduped = DockerConnectorTestSuite._dedup_scenarios(scenarios)
    assert [scenario.status for scenario in deduped] == expected_statuses


def test_dedup_scenarios_conflicting_statuses_raise() -> None:
    scenarios = [
        ConnectorTestScenario(config_path=Path("integration_tests/config.json"), status="succeed"),
        ConnectorTestScenario(config_path=Path("integration_tests/config.json"), status="failed"),
    ]
    with pytest.raises(ValueError, match="Conflicting expected statuses"):
        DockerConnectorTestSuite._dedup_scenarios(scenarios)


@pytest.mark.parametrize(
    "config_paths, expected_ids",
    [
        pytest.param(
            [Path("integration_tests/config.json"), Path("secrets/config.json")],
            [
                "integration_tests/'config' Test Scenario",
                "secrets/'config' Test Scenario",
            ],
            id="colliding_stems_qualified_with_parent_dir",
        ),
        pytest.param(
            [Path("secrets/config.json"), Path("secrets/valid_config.json")],
            ["'config' Test Scenario", "'valid_config' Test Scenario"],
            id="unique_stems_unchanged",
        ),
    ],
)
def test_scenario_test_ids(config_paths: list[Path], expected_ids: list[str]) -> None:
    scenarios = [ConnectorTestScenario(config_path=path) for path in config_paths]
    assert _scenario_test_ids(scenarios) == expected_ids


def test_md5_checksum_matches_injected_text_with_crlf(tmp_path: Path) -> None:
    """Assert the checksum covers `read_text()`, which `create_connector` injects, not raw bytes."""
    components_py_path = tmp_path / "components.py"
    components_py_path.write_bytes(b"def sample_function():\r\n    return 1\r\n")

    validate_python_code(
        code_text=components_py_path.read_text(),
        checksums={"md5": md5_checksum(components_py_path)},
    )


class _NoArgSource:
    def __init__(self) -> None:
        self.created_with: dict[str, Any] = {}


class _CatalogConfigStateSource:
    def __init__(self, catalog: Any, config: Any, state: Any, **kwargs: Any) -> None:
        self.created_with = {"catalog": catalog, "config": config, "state": state, **kwargs}


class _OptionalArgsSource:
    def __init__(self, catalog: Any = None, config: Any = None, state: Any = None) -> None:
        self.created_with = {"catalog": catalog, "config": config, "state": state}


class _StreamReaderSource:
    def __init__(self, stream_reader: Any, spec_class: Any, catalog: Any, config: Any, state: Any):
        self.created_with = {}


def _suite_for(connector: Any, root: Path) -> type[ConnectorTestSuiteBase]:
    return type(
        "Suite",
        (ConnectorTestSuiteBase,),
        {
            "connector": connector,
            "get_connector_root_dir": classmethod(lambda cls: root),
        },
    )


@pytest.mark.parametrize(
    "connector, scenario_config, expected",
    [
        pytest.param(_NoArgSource, {"api_key": "x"}, {}, id="no_args"),
        pytest.param(
            _CatalogConfigStateSource,
            {"api_key": "x"},
            {"catalog": None, "config": {"api_key": "x"}, "state": None},
            id="required_catalog_config_state_get_scenario_config",
        ),
        pytest.param(
            _OptionalArgsSource,
            {"api_key": "x"},
            {"catalog": None, "config": {"api_key": "x"}, "state": None},
            id="optional_config_gets_scenario_config_catalog_and_state_keep_defaults",
        ),
    ],
)
def test_create_connector_supplies_required_source_args(
    tmp_path: Path,
    connector: Any,
    scenario_config: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    suite = _suite_for(connector, tmp_path)
    created = suite.create_connector(ConnectorTestScenario(config_dict=scenario_config))
    assert created.created_with == expected  # type: ignore[attr-defined]


def test_create_connector_without_scenario_passes_empty_config(tmp_path: Path) -> None:
    suite = _suite_for(_CatalogConfigStateSource, tmp_path)
    created = suite.create_connector(None)
    assert created.created_with == {"catalog": None, "config": {}, "state": None}  # type: ignore[attr-defined]


def test_create_connector_with_missing_config_file_passes_empty_config(tmp_path: Path) -> None:
    suite = _suite_for(_CatalogConfigStateSource, tmp_path)
    scenario = ConnectorTestScenario(config_path=Path("secrets/config.json"))
    created = suite.create_connector(scenario)
    assert created.created_with["config"] == {}  # type: ignore[attr-defined]


def test_create_connector_reads_config_file_relative_to_connector_root(tmp_path: Path) -> None:
    (tmp_path / "secrets").mkdir()
    (tmp_path / "secrets" / "config.json").write_text('{"start_date": "2024-01-01"}')
    suite = _suite_for(_CatalogConfigStateSource, tmp_path)
    created = suite.create_connector(ConnectorTestScenario(config_path=Path("secrets/config.json")))
    assert created.created_with["config"] == {"start_date": "2024-01-01"}  # type: ignore[attr-defined]


def test_create_connector_names_arguments_it_cannot_supply(tmp_path: Path) -> None:
    suite = _suite_for(_StreamReaderSource, tmp_path)
    with pytest.raises(TypeError) as error:
        suite.create_connector(ConnectorTestScenario(config_dict={}))
    message = str(error.value)
    assert "`_StreamReaderSource`" in message
    assert "`stream_reader`, `spec_class`" in message
    assert "override `create_connector()`" in message


class _CatalogAtInitSource:
    """Like a file-based source: `read` only knows the streams of the catalog it was built with."""

    def __init__(self, catalog: Any, config: Any, state: Any) -> None:
        self.created_with = {"catalog": catalog, "config": config, "state": state}

    def read(self, logger: Any, config: Any, catalog: Any, state: Any = None) -> Any:
        yield {"built_with": self.created_with, "read_with": (config, catalog, state)}


def test_create_connector_reads_with_a_source_built_for_the_read_command(tmp_path: Path) -> None:
    suite = _suite_for(_CatalogAtInitSource, tmp_path)
    created = suite.create_connector(ConnectorTestScenario(config_dict={"api_key": "x"}))
    assert created.created_with == {"catalog": None, "config": {"api_key": "x"}, "state": None}  # type: ignore[attr-defined]

    messages = list(created.read(None, {"api_key": "y"}, "the catalog", ["the state"]))  # type: ignore[attr-defined]

    assert messages == [
        {
            "built_with": {
                "catalog": "the catalog",
                "config": {"api_key": "y"},
                "state": ["the state"],
            },
            "read_with": ({"api_key": "y"}, "the catalog", ["the state"]),
        }
    ]


class _OptionalConfigSource:
    """Like a declarative Python source: `config` is optional and evaluated when it is built."""

    def __init__(self, catalog: Any = None, config: Any = None, state: Any = None) -> None:
        self.created_with = {"catalog": catalog, "config": config, "state": state}

    def read(self, logger: Any, config: Any, catalog: Any, state: Any = None) -> Any:
        yield {"built_with": self.created_with}


def test_create_connector_reads_sources_with_optional_arguments_with_the_command_arguments(
    tmp_path: Path,
) -> None:
    suite = _suite_for(_OptionalConfigSource, tmp_path)
    created = suite.create_connector(ConnectorTestScenario(config_dict={"page_id": "1"}))
    # `check` and `discover` use this source, so it is built with the scenario's config.
    assert created.created_with == {"catalog": None, "config": {"page_id": "1"}, "state": None}  # type: ignore[attr-defined]

    messages = list(created.read(None, {"page_id": "1"}, "the catalog", None))  # type: ignore[attr-defined]

    assert messages == [
        {"built_with": {"catalog": "the catalog", "config": {"page_id": "1"}, "state": None}}
    ]


def test_create_connector_leaves_an_optional_config_out_when_the_scenario_has_none(
    tmp_path: Path,
) -> None:
    suite = _suite_for(_OptionalConfigSource, tmp_path)
    created = suite.create_connector(None)
    assert created.created_with == {"catalog": None, "config": None, "state": None}  # type: ignore[attr-defined]


def test_create_connector_keeps_read_of_sources_built_without_arguments(tmp_path: Path) -> None:
    class _NoArgReadSource:
        def read(self, logger: Any, config: Any, catalog: Any, state: Any = None) -> Any:
            yield "own read"

    suite = _suite_for(_NoArgReadSource, tmp_path)
    created = suite.create_connector(ConnectorTestScenario(config_dict={}))
    assert "read" not in vars(created)
    assert list(created.read(None, {}, None)) == ["own read"]  # type: ignore[attr-defined]
