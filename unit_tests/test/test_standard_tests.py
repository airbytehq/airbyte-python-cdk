# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for FAST Airbyte Standard Tests."""

from pathlib import Path
from typing import Any

import pytest

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.source import Source
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
