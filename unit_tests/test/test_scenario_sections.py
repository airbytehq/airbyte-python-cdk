# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Every config in `acceptance-test-config.yml` gets a `check` scenario.

Configs listed only under `discovery`, `full_refresh` or `incremental` run `check` alone;
the others also run `discover` and `read`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, TypeVar, cast

import pytest
import yaml

from airbyte_cdk.test.models.scenario import (
    CHECK_ONLY_SECTIONS,
    FULL_SUITE_SECTIONS,
    SCENARIO_SECTIONS,
    ConnectorTestScenario,
)
from airbyte_cdk.test.standard_tests.docker_base import (
    DockerConnectorTestSuite,
    skip_if_check_only,
)
from airbyte_cdk.test.standard_tests.source_base import SourceTestSuiteBase
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG


class _Reached(Exception):
    """Raised by the fake `create_connector` to prove a test body ran past its skips."""


_SuiteT = TypeVar("_SuiteT", bound=DockerConnectorTestSuite)


def _suite_class(
    tmp_path: Path,
    acceptance_tests: dict[str, Any],
    *,
    base: type[_SuiteT],
) -> type[_SuiteT]:
    """Build a suite class over a temporary connector dir holding `acceptance_tests`."""
    connector_root = tmp_path / "source-test"
    connector_root.mkdir(exist_ok=True)
    (connector_root / ACCEPTANCE_TEST_CONFIG).write_text(
        yaml.safe_dump({"acceptance_tests": acceptance_tests})
    )

    def get_connector_root_dir(cls: type) -> Path:
        return connector_root

    def create_connector(self: Any, scenario: ConnectorTestScenario) -> Any:
        raise _Reached(scenario.id)

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


def test_section_constants_partition_the_scenario_sections() -> None:
    assert SCENARIO_SECTIONS == FULL_SUITE_SECTIONS + CHECK_ONLY_SECTIONS
    assert not set(FULL_SUITE_SECTIONS) & set(CHECK_ONLY_SECTIONS)


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
    assert full.sections == ("spec", "connection", "basic_read", "discovery")
    assert full.status == "succeed"
    assert [s.name for s in full.empty_streams or []] == ["empty_stream"]
    assert not full.check_only

    assert scenarios["integration_tests/invalid_config.json"].sections == ("connection",)
    assert scenarios["integration_tests/invalid_config.json"].status == "failed"

    # Listed under basic_read and incremental: the full-suite section wins.
    assert scenarios["secrets/config_read_only.json"].sections == ("basic_read", "incremental")
    assert not scenarios["secrets/config_read_only.json"].check_only

    for config_path, section in [
        ("secrets/config_old.json", "discovery"),
        ("secrets/config_full_refresh.json", "full_refresh"),
        ("secrets/config_incremental.json", "incremental"),
    ]:
        scenario = scenarios[config_path]
        assert scenario.sections == (section,)
        assert scenario.check_only, f"{config_path} is listed only under {section}"
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
    assert not deduped[0].check_only


@pytest.mark.parametrize(
    "sections, expected",
    [
        pytest.param((), False, id="hand_built_scenario_runs_everything"),
        pytest.param(("spec",), False, id="spec"),
        pytest.param(("connection",), False, id="connection"),
        pytest.param(("basic_read",), False, id="basic_read"),
        pytest.param(("discovery",), True, id="discovery_only"),
        pytest.param(("full_refresh",), True, id="full_refresh_only"),
        pytest.param(("incremental",), True, id="incremental_only"),
        pytest.param(
            ("discovery", "full_refresh", "incremental"), True, id="all_check_only_sections"
        ),
        pytest.param(("incremental", "basic_read"), False, id="mixed_sections"),
    ],
)
def test_check_only_property(sections: tuple[str, ...], expected: bool) -> None:
    scenario = ConnectorTestScenario(config_path=Path("secrets/config.json"), sections=sections)
    assert scenario.check_only is expected
    # Derived scenarios keep the sections, so the expectation helpers cannot un-mark a config.
    assert scenario.without_expected_outcome().check_only is expected
    assert scenario.with_expecting_failure().check_only is expected
    assert scenario.with_expecting_success().check_only is expected


CHECK_ONLY_SCENARIO = ConnectorTestScenario(
    config_path=Path("secrets/config_incremental.json"), sections=("incremental",)
)
FULL_SCENARIO = ConnectorTestScenario(
    config_path=Path("secrets/config.json"), sections=("connection",)
)


def test_skip_if_check_only_names_the_sections() -> None:
    with pytest.raises(pytest.skip.Exception, match="listed only under `incremental`"):
        skip_if_check_only(CHECK_ONLY_SCENARIO)
    skip_if_check_only(FULL_SCENARIO)  # no skip


def _run_not_skipped(call: Callable[[], Any]) -> None:
    """Run a suite test body and fail if it skips; any other outcome is fine."""
    try:
        call()
    except pytest.skip.Exception as exc:
        pytest.fail(f"unexpected skip: {exc}")
    except Exception:
        pass


@pytest.mark.parametrize(
    "test_name", ["test_discover", "test_basic_read", "test_fail_read_with_bad_catalog"]
)
def test_source_suite_runs_only_check_for_check_only_scenarios(
    tmp_path: Path, test_name: str
) -> None:
    suite = _suite_class(tmp_path, ALL_SECTIONS_CONFIG, base=SourceTestSuiteBase)()

    with pytest.raises(pytest.skip.Exception, match="Only `check` runs"):
        getattr(suite, test_name)(CHECK_ONLY_SCENARIO)

    # A config from a full-suite section reaches the connector.
    with pytest.raises(_Reached):
        getattr(suite, test_name)(FULL_SCENARIO)

    # `check` itself runs for both.
    with pytest.raises(_Reached):
        suite.test_check(CHECK_ONLY_SCENARIO)
    with pytest.raises(_Reached):
        suite.test_check(FULL_SCENARIO)


def test_docker_read_runs_only_check_for_check_only_scenarios(tmp_path: Path) -> None:
    suite = _suite_class(tmp_path, ALL_SECTIONS_CONFIG, base=DockerConnectorTestSuite)()

    with pytest.raises(pytest.skip.Exception, match="Only `check` runs"):
        suite.test_docker_image_build_and_read(
            CHECK_ONLY_SCENARIO,
            connector_image_override=None,
            connector_base_image_override=None,
            read_from_streams="all",
            read_scenarios="all",
        )

    # A full-suite config gets past the skips (and then fails on the missing metadata.yaml).
    _run_not_skipped(
        lambda: suite.test_docker_image_build_and_read(
            FULL_SCENARIO,
            connector_image_override=None,
            connector_base_image_override=None,
            read_from_streams="all",
            read_scenarios="all",
        )
    )
