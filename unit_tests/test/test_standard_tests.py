# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for FAST Airbyte Standard Tests."""

from pathlib import Path
from typing import Any

import pytest
from _pytest.outcomes import Skipped

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.source import Source
from airbyte_cdk.test.models.scenario import ConnectorTestScenario
from airbyte_cdk.test.standard_tests._job_runner import IConnector
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.test.standard_tests.pytest_hooks import _scenario_test_ids
from airbyte_cdk.test.standard_tests.source_base import SourceTestSuiteBase
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG


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


BYPASSED_BASIC_READ_CONFIG = """
acceptance_tests:
  connection:
    tests:
      - config_path: "secrets/config.json"
        status: "succeed"
  basic_read:
    bypass_reason: "Test account doesn't have records."
"""
ENABLED_BASIC_READ_CONFIG = """
acceptance_tests:
  basic_read:
    tests:
      - config_path: "secrets/config.json"
"""


def _test_suite_for_config(
    tmp_path: Path,
    acceptance_test_config: str,
) -> type[SourceTestSuiteBase]:
    """Build a source test suite rooted in a temp dir with the given acceptance test config."""
    (tmp_path / ACCEPTANCE_TEST_CONFIG).write_text(acceptance_test_config)
    return type(
        "TestSuiteTemp",
        (SourceTestSuiteBase,),
        {"get_connector_root_dir": classmethod(lambda cls: tmp_path)},
    )


@pytest.mark.parametrize(
    "acceptance_test_config, expected_reason",
    [
        pytest.param(
            BYPASSED_BASIC_READ_CONFIG,
            "Test account doesn't have records.",
            id="bypass_reason_without_tests_is_honored",
        ),
        pytest.param(
            ENABLED_BASIC_READ_CONFIG,
            None,
            id="declared_tests_are_not_bypassed",
        ),
        pytest.param(
            """
acceptance_tests:
  basic_read:
    bypass_reason: "  "
    """,
            None,
            id="blank_bypass_reason_is_not_a_bypass",
        ),
        pytest.param(
            """
acceptance_tests:
  basic_read:
    bypass_reason: "Documented, but tests are declared too."
    tests:
      - config_path: "secrets/config.json"
    """,
            None,
            id="tests_win_over_bypass_reason",
        ),
        pytest.param(
            """
acceptance_tests:
  connection:
    tests:
      - config_path: "secrets/config.json"
    """,
            None,
            id="missing_category_is_not_a_bypass",
        ),
    ],
)
def test_get_bypass_reason(
    tmp_path: Path,
    acceptance_test_config: str,
    expected_reason: str | None,
) -> None:
    test_suite = _test_suite_for_config(tmp_path, acceptance_test_config)
    assert test_suite.get_bypass_reason("basic_read") == expected_reason


def test_basic_read_is_skipped_when_bypassed(tmp_path: Path) -> None:
    """`test_basic_read` should skip (not fail) when `basic_read` declares a bypass reason."""
    test_suite = _test_suite_for_config(tmp_path, BYPASSED_BASIC_READ_CONFIG)
    with pytest.raises(Skipped, match="Test account doesn't have records."):
        test_suite().test_basic_read(
            scenario=ConnectorTestScenario(config_path=Path("secrets/config.json")),
        )
