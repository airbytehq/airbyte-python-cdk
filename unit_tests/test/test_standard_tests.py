# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for FAST Airbyte Standard Tests."""

from pathlib import Path
from typing import Any

import pytest
import yaml

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.source import Source
from airbyte_cdk.test.models import ExpectedOutcome
from airbyte_cdk.test.models.scenario import ConnectorTestScenario
from airbyte_cdk.test.standard_tests._job_runner import IConnector
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


def _suite_for_acceptance_test_config(
    tmp_path: Path,
    acceptance_test_config: dict[str, Any],
) -> type[DockerConnectorTestSuite]:
    """Build a suite class whose connector root holds the given acceptance-test-config.yml."""
    (tmp_path / "acceptance-test-config.yml").write_text(yaml.safe_dump(acceptance_test_config))

    class _Suite(DockerConnectorTestSuite):
        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return tmp_path

    return _Suite


@pytest.mark.parametrize(
    "acceptance_tests, expected_statuses, expected_check_statuses",
    [
        pytest.param(
            {
                "spec": {"tests": [{"spec_path": "manifest.yaml"}]},
                "connection": {"tests": [{"config_path": "secrets/config.json"}]},
            },
            {"secrets/config.json": None},
            {"secrets/config.json": "succeed"},
            id="statusless_connection_entry_defaults_to_succeed_for_check",
        ),
        pytest.param(
            {
                "basic_read": {
                    "tests": [
                        {
                            "config_path": "secrets/config.json",
                            "configured_catalog_path": "integration_tests/catalog.json",
                        }
                    ]
                },
            },
            {"secrets/config.json": None},
            {"secrets/config.json": None},
            id="basic_read_only_config_keeps_open_expectation",
        ),
        pytest.param(
            {
                "spec": {
                    "tests": [
                        {"spec_path": "manifest.yaml", "config_path": "secrets/config.json"},
                    ]
                },
            },
            {"secrets/config.json": None},
            {"secrets/config.json": None},
            id="spec_only_config_keeps_open_expectation",
        ),
        pytest.param(
            {
                "spec": {
                    "tests": [
                        {"spec_path": "manifest.yaml", "config_path": "secrets/config.json"},
                    ]
                },
                "connection": {
                    "tests": [
                        {"config_path": "secrets/config.json"},
                        {
                            "config_path": "integration_tests/invalid_config.json",
                            "status": "failed",
                        },
                        {
                            "config_path": "integration_tests/broken_config.json",
                            "status": "exception",
                        },
                    ]
                },
                "basic_read": {
                    "tests": [
                        {
                            "config_path": "secrets/config.json",
                            "configured_catalog_path": "integration_tests/catalog.json",
                        }
                    ]
                },
            },
            {
                "secrets/config.json": None,
                "integration_tests/invalid_config.json": "failed",
                "integration_tests/broken_config.json": "exception",
            },
            {
                "secrets/config.json": "succeed",
                "integration_tests/invalid_config.json": "failed",
                "integration_tests/broken_config.json": "exception",
            },
            id="declared_statuses_are_kept",
        ),
    ],
)
def test_check_scenario_applies_cat_default_only_to_connection_entries(
    tmp_path: Path,
    acceptance_tests: dict[str, Any],
    expected_statuses: dict[str, str | None],
    expected_check_statuses: dict[str, str | None],
) -> None:
    """Status-less `connection` configs default to `succeed` for `check` only.

    `get_scenarios` leaves the declared statuses untouched (so `discover`/`read` keep their
    expectations); `_check_scenario` applies the CAT default on top of them.
    """
    suite = _suite_for_acceptance_test_config(tmp_path, {"acceptance_tests": acceptance_tests})
    scenarios = suite.get_scenarios()
    assert {str(s.config_path): s.status for s in scenarios} == expected_statuses

    check_scenarios = [suite._check_scenario(scenario) for scenario in scenarios]
    assert {str(s.config_path): s.status for s in check_scenarios} == expected_check_statuses
    for scenario in check_scenarios:
        assert scenario.expected_outcome == ExpectedOutcome.from_status_str(scenario.status)


def test_check_scenario_without_acceptance_test_config(tmp_path: Path) -> None:
    """Scenarios built in code (no config_path, or no config file) are returned unchanged."""

    class _Suite(DockerConnectorTestSuite):
        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return tmp_path

    in_code = ConnectorTestScenario(config_dict={"key": "value"})
    assert _Suite._check_scenario(in_code) is in_code

    from_file = ConnectorTestScenario(config_path=Path("secrets/config.json"))
    assert _Suite._check_scenario(from_file) is from_file


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
