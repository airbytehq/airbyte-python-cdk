# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""An `acceptance-test-config.yml` with no runnable scenario must fail, unless it says why.

Collection fails when the config yields no runnable scenario and declares no `bypass_reason`;
declared bypasses are reported as skipped tests with their reason. A missing file fails except
for Java connectors, a legacy top-level `tests:` key fails with the migration, and a section in
the legacy list layout is read as its `tests` list.
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

import pytest
import yaml

from airbyte_cdk.test.standard_tests._acceptance_test_config import (
    JAVA_BYPASS_REASON,
    AcceptanceTestConfigError,
    MissingAcceptanceTestConfigError,
    ScenarioBypass,
    collect_scenarios,
    load_acceptance_test_config,
    section_entries,
)
from airbyte_cdk.test.standard_tests._spec_compatibility import disabled_for_version
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG

pytest_plugins = ["pytester"]

BUILDER_REASON = "This is a builder contribution, and we do not have secrets at this time"

# The shape of the ~290 builder contributions without secrets: a `spec_path` entry, and a
# `bypass_reason` on every other section.
BYPASSED_SECTIONS_CONFIG: dict[str, Any] = {
    "connector_image": "airbyte/source-test:dev",
    "acceptance_tests": {
        "spec": {"tests": [{"spec_path": "manifest.yaml"}]},
        "connection": {"bypass_reason": BUILDER_REASON},
        "discovery": {"bypass_reason": BUILDER_REASON},
        "basic_read": {"bypass_reason": BUILDER_REASON},
        "incremental": {"bypass_reason": BUILDER_REASON},
        "full_refresh": {"bypass_reason": BUILDER_REASON},
    },
}

# No runnable config and no bypass of a collected section: only `incremental` declares one.
ZERO_SCENARIOS_CONFIG: dict[str, Any] = {
    "acceptance_tests": {
        "spec": {"tests": [{"spec_path": "manifest.yaml"}]},
        "connection": {"tests": [{"config_path": "secrets/config_iam_role.json"}]},
        "basic_read": {"tests": []},
        "incremental": {"bypass_reason": "This connector does not implement incremental sync"},
    }
}

RUNNABLE_CONFIG: dict[str, Any] = {
    "acceptance_tests": {
        "connection": {"tests": [{"config_path": "secrets/config.json", "status": "succeed"}]},
        "basic_read": {"bypass_reason": "The sandbox has no data"},
    }
}


def _write_connector(
    root: Path,
    config: dict[str, Any] | str | None,
    *,
    language: str = "manifest-only",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "metadata.yaml").write_text(
        yaml.safe_dump({"data": {"tags": [f"language:{language}"]}})
    )
    if config is not None:
        text = config if isinstance(config, str) else yaml.safe_dump(config)
        (root / ACCEPTANCE_TEST_CONFIG).write_text(text)
    return root


def _suite(root: Path) -> type[DockerConnectorTestSuite]:
    class _Suite(DockerConnectorTestSuite):
        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return root

    return _Suite


# --- Reading the config -----------------------------------------------------------------------


def test_bypassed_sections_yield_no_scenario_and_one_bypass_per_collected_section(
    tmp_path: Path,
) -> None:
    suite = _suite(_write_connector(tmp_path / "source-test", BYPASSED_SECTIONS_CONFIG))

    assert suite.get_scenarios() == []
    assert suite.get_scenario_bypasses() == [
        ScenarioBypass(
            reason=BUILDER_REASON,
            location=f"acceptance_tests.{section}.bypass_reason",
            sections=(section,),
        )
        for section in ("connection", "discovery", "basic_read", "full_refresh")
    ], "the `incremental` bypass is not a bypass of anything the standard tests run"


def test_zero_scenarios_without_bypass_lists_why_each_part_produced_nothing(
    tmp_path: Path,
) -> None:
    suite = _suite(_write_connector(tmp_path / "source-test", ZERO_SCENARIOS_CONFIG))

    assert suite.get_scenarios() == []
    assert suite.get_scenario_bypasses() == []
    message = suite.describe_empty_scenarios()
    assert message.startswith("No test scenarios for connector 'source-test': ")
    assert str(tmp_path / "source-test" / ACCEPTANCE_TEST_CONFIG) in message
    for expected in [
        "`acceptance_tests.spec.tests[0]`: no `config_path` and no `bypass_reason` "
        "(keys: `spec_path`).",
        "`acceptance_tests.connection.tests[0]`: an IAM-role config",
        "`acceptance_tests.basic_read.tests`: empty list.",
        "`acceptance_tests.incremental`: not a section the standard tests collect.",
        "a top-level `bypass_reason` for the whole connector",
    ]:
        assert expected in message


def test_entry_without_config_path_declares_a_bypass(tmp_path: Path) -> None:
    config = {
        "acceptance_tests": {
            "basic_read": {"tests": [{"bypass_reason": "Read needs a paid plan", "timeout": 1}]}
        }
    }
    suite = _suite(_write_connector(tmp_path / "source-test", config))

    assert suite.get_scenario_bypasses() == [
        ScenarioBypass(
            reason="Read needs a paid plan",
            location="acceptance_tests.basic_read.tests[0].bypass_reason",
            sections=("basic_read",),
        )
    ]


def test_section_bypass_is_ignored_next_to_runnable_configs(tmp_path: Path) -> None:
    config = {
        "acceptance_tests": {
            "connection": {
                "bypass_reason": "stale",
                "tests": [{"config_path": "secrets/config.json"}],
            }
        }
    }
    collected = collect_scenarios(config, tmp_path / ACCEPTANCE_TEST_CONFIG)

    assert [str(s.config_path) for s in collected.scenarios] == ["secrets/config.json"]
    assert collected.bypasses == []
    assert collected.notes == [
        "`acceptance_tests.connection.bypass_reason`: ignored, as the section lists 1 runnable "
        "config entries."
    ]


def test_runnable_entry_does_not_keep_its_bypass_reason(tmp_path: Path) -> None:
    config = {
        "acceptance_tests": {
            "connection": {"tests": [{"config_path": "secrets/config.json", "bypass_reason": "x"}]}
        }
    }
    [scenario] = collect_scenarios(config, tmp_path / ACCEPTANCE_TEST_CONFIG).scenarios
    assert scenario.bypass_reason is None, "a config with a `config_path` is run, not bypassed"


@pytest.mark.parametrize("reason", ["", "  ", 1, True])
def test_blank_or_non_string_section_bypass_does_not_count(tmp_path: Path, reason: Any) -> None:
    config = {"acceptance_tests": {"connection": {"bypass_reason": reason}}}
    collected = collect_scenarios(config, tmp_path / ACCEPTANCE_TEST_CONFIG)

    assert collected.bypasses == []
    assert collected.notes == [
        "`acceptance_tests.connection.bypass_reason`: not a non-empty string, so not counted."
    ]


def test_top_level_bypass_only_file_bypasses_the_connector(tmp_path: Path) -> None:
    root = _write_connector(
        tmp_path / "destination-test", 'bypass_reason: "No test config in GSM for this connector"\n'
    )
    suite = _suite(root)

    assert suite.get_scenarios() == []
    assert suite.get_scenario_bypasses() == [
        ScenarioBypass(reason="No test config in GSM for this connector", location="bypass_reason")
    ]
    assert disabled_for_version(suite.acceptance_test_config) == []


def test_top_level_bypass_next_to_runnable_configs_fails(tmp_path: Path) -> None:
    root = _write_connector(tmp_path / "source-test", {**RUNNABLE_CONFIG, "bypass_reason": "x"})

    with pytest.raises(AcceptanceTestConfigError, match="top-level `bypass_reason`.*1 runnable"):
        _suite(root).get_scenarios()


def test_top_level_bypass_with_bypassed_sections_is_accepted(tmp_path: Path) -> None:
    """A spec waiver file can carry the connector-wide bypass next to its `spec` entry."""
    config = {
        "bypass_reason": "No test config in GSM for this connector",
        "acceptance_tests": {
            "spec": {
                "tests": [{"backward_compatibility_tests_config": {"disable_for_version": "1.0.0"}}]
            }
        },
    }
    suite = _suite(_write_connector(tmp_path / "source-test", config))

    assert [bypass.location for bypass in suite.get_scenario_bypasses()] == ["bypass_reason"]
    assert disabled_for_version(suite.acceptance_test_config) == ["1.0.0"]


def test_invalid_entry_fails_with_its_location_and_without_values(tmp_path: Path) -> None:
    """A legacy list section listing `empty_streams` as plain strings, as source-netsuite did."""
    config = {
        "acceptance_tests": {
            "basic_read": [
                {"config_path": "secrets/config.json", "empty_streams": ["secret_stream_name"]}
            ]
        }
    }
    with pytest.raises(AcceptanceTestConfigError) as error:
        collect_scenarios(config, tmp_path / ACCEPTANCE_TEST_CONFIG)

    message = str(error.value)
    assert message.startswith("`acceptance_tests.basic_read[0]` in ")
    assert "`empty_streams.0`: Input should be a valid dictionary" in message
    assert "secret_stream_name" not in message, "input values are not printed"


# --- Legacy layouts ---------------------------------------------------------------------------


def test_legacy_list_sections_are_read_as_tests_lists(tmp_path: Path) -> None:
    config = {
        "acceptance_tests": {
            "spec": [{"spec_path": "manifest.yaml"}],
            "connection": [
                {"config_path": "secrets/config.json", "status": "succeed"},
                {"config_path": "integration_tests/invalid_config.json", "status": "failed"},
            ],
            "basic_read": [
                {"config_path": "secrets/config.json", "empty_streams": [{"name": "a"}]}
            ],
            "full_refresh": [{"bypass_reason": "Covered by basic_read"}],
        }
    }
    suite = _suite(_write_connector(tmp_path / "source-test", config))

    scenarios = {str(s.config_path): s for s in suite.get_scenarios()}
    assert sorted(scenarios) == ["integration_tests/invalid_config.json", "secrets/config.json"]
    assert scenarios["secrets/config.json"].sections == ("connection", "basic_read")
    assert scenarios["integration_tests/invalid_config.json"].status == "failed"
    assert suite.get_scenario_bypasses() == [
        ScenarioBypass(
            reason="Covered by basic_read",
            location="acceptance_tests.full_refresh[0].bypass_reason",
            sections=("full_refresh",),
        )
    ]


@pytest.mark.parametrize(
    "section, expected",
    [
        ({"tests": [{"a": 1}]}, [{"a": 1}]),
        ([{"a": 1}], [{"a": 1}]),
        ({"bypass_reason": "x"}, None),
        ({"tests": None}, None),
        (None, None),
        ("tests", None),
    ],
)
def test_section_entries_reads_both_layouts(section: Any, expected: Any) -> None:
    assert section_entries(section) == expected


def test_legacy_top_level_tests_key_fails_with_the_migration(tmp_path: Path) -> None:
    config = {
        "connector_image": "airbyte/source-test:dev",
        "tests": {"spec": [{"spec_path": "manifest.yaml"}]},
    }
    root = _write_connector(tmp_path / "source-test", config)

    with pytest.raises(AcceptanceTestConfigError) as error:
        _suite(root).get_scenarios()
    message = str(error.value)
    assert str(root / ACCEPTANCE_TEST_CONFIG) in message
    assert "legacy layout with a top-level `tests:` key" in message
    assert "rename `tests:` to `acceptance_tests:`" in message
    assert "airbyte/source-test:dev" not in message, "values are not printed"


@pytest.mark.parametrize(
    "text, match",
    [
        ("", "is empty"),
        ("# only comments\n", "is empty"),
        ("- a\n", "holds a list"),
        ("acceptance_tests: [a]\n", "must be a mapping of sections"),
        ("connector_image: x\n", "no `acceptance_tests` key and no top-level `bypass_reason`"),
        ("bypass_reason: ''\n", "must be a non-empty string"),
        ("acceptance_tests: [\n", "is not valid YAML"),
    ],
)
def test_unusable_config_layouts_fail(tmp_path: Path, text: str, match: str) -> None:
    path = tmp_path / ACCEPTANCE_TEST_CONFIG
    path.write_text(text)

    with pytest.raises(AcceptanceTestConfigError, match=match):
        load_acceptance_test_config(path)


# --- Missing file -----------------------------------------------------------------------------


@pytest.mark.parametrize("language", ["python", "manifest-only"])
def test_missing_config_fails_for_connectors_that_run_the_standard_tests(
    tmp_path: Path, language: str
) -> None:
    root = _write_connector(tmp_path / "destination-test", None, language=language)

    with pytest.raises(MissingAcceptanceTestConfigError, match="only a top-level `bypass_reason`"):
        _suite(root).get_scenarios()


def test_missing_config_error_is_a_file_not_found_error(tmp_path: Path) -> None:
    """Callers that tolerated a missing file by catching `FileNotFoundError` keep working."""
    root = _write_connector(tmp_path / "source-test", None)
    with pytest.raises(FileNotFoundError):
        _suite(root).acceptance_test_config


def test_missing_config_is_a_declared_bypass_for_java_connectors(tmp_path: Path) -> None:
    suite = _suite(_write_connector(tmp_path / "source-test", None, language="java"))

    assert suite.get_scenarios() == []
    assert suite.get_scenario_bypasses() == [
        ScenarioBypass(reason=JAVA_BYPASS_REASON, location="metadata.yaml")
    ]


# --- Collection (pytest_generate_tests) -------------------------------------------------------


def _run_collection(pytester: pytest.Pytester, root: Path, *args: str) -> pytest.RunResult:
    pytester.makepyfile(
        test_standard=textwrap.dedent(
            f"""
            from pathlib import Path

            from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite


            class TestSuite(DockerConnectorTestSuite):
                @classmethod
                def get_connector_root_dir(cls) -> Path:
                    return Path({str(root)!r})

                def test_with_scenario(self, scenario) -> None:
                    pass
            """
        )
    )
    return pytester.runpytest_inprocess(
        "-p",
        "airbyte_cdk.test.standard_tests.pytest_hooks",
        "-p",
        "no:cacheprovider",
        "-k",
        "test_with_scenario",
        "-rs",
        *args,
    )


def test_collection_fails_when_no_scenario_and_no_bypass(
    pytester: pytest.Pytester, tmp_path: Path
) -> None:
    root = _write_connector(tmp_path / "source-test", ZERO_SCENARIOS_CONFIG)

    result = _run_collection(pytester, root)

    assert result.ret == pytest.ExitCode.INTERRUPTED
    result.stdout.fnmatch_lines(
        [
            "*No test scenarios for connector 'source-test': *acceptance-test-config.yml lists no "
            "config the standard tests can run, and declares no `bypass_reason`. Found:",
            "*`acceptance_tests.spec.tests[[]0[]]`: no `config_path` and no `bypass_reason`*",
            "*1 error*",
        ]
    )
    assert "Traceback" not in result.stdout.str(), "the message is reported without a traceback"


def test_collection_fails_on_a_missing_config(pytester: pytest.Pytester, tmp_path: Path) -> None:
    root = _write_connector(tmp_path / "source-test", None)

    result = _run_collection(pytester, root)

    assert result.ret == pytest.ExitCode.INTERRUPTED
    result.stdout.fnmatch_lines(["*Acceptance test config file not found at: *", "*1 error*"])
    output = result.stdout.str()
    assert output.count("Acceptance test config file not found at:") == 1, "reported once"
    assert "During handling of the above exception" not in output


def test_collection_fails_on_the_legacy_top_level_tests_key(
    pytester: pytest.Pytester, tmp_path: Path
) -> None:
    root = _write_connector(tmp_path / "source-test", {"tests": {"spec": []}})

    result = _run_collection(pytester, root)

    assert result.ret == pytest.ExitCode.INTERRUPTED
    result.stdout.fnmatch_lines(["*legacy layout with a top-level `tests:` key*", "*1 error*"])


def test_bypasses_are_reported_as_skipped_tests_and_at_collection(
    pytester: pytest.Pytester, tmp_path: Path
) -> None:
    root = _write_connector(tmp_path / "source-test", BYPASSED_SECTIONS_CONFIG)

    result = _run_collection(pytester, root)

    assert result.ret == pytest.ExitCode.OK
    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(
        [
            "source-test: acceptance-test-config.yml declares 4 bypass(es), reported as skipped "
            "tests: connection, discovery, basic_read, full_refresh: " + BUILDER_REASON,
            "SKIPPED [[]1[]] *: acceptance-test-config.yml bypasses connection, discovery, "
            "basic_read, full_refresh: " + BUILDER_REASON,
        ]
    )


def test_bypass_params_have_readable_ids(pytester: pytest.Pytester, tmp_path: Path) -> None:
    root = _write_connector(tmp_path / "source-test", RUNNABLE_CONFIG)

    result = _run_collection(pytester, root, "--collect-only", "-q")

    result.stdout.fnmatch_lines(
        [
            "*test_with_scenario[[]'config' Test Scenario[]]",
            "*test_with_scenario[[]bypassed-basic_read[]]",
        ]
    )


def test_connector_wide_bypass_is_one_skipped_test(
    pytester: pytest.Pytester, tmp_path: Path
) -> None:
    root = _write_connector(tmp_path / "source-test", "bypass_reason: No sandbox account\n")

    result = _run_collection(pytester, root, "-v")

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(
        [
            "*test_with_scenario[[]bypassed-connector[]] SKIPPED*",
            "SKIPPED [[]1[]] *: acceptance-test-config.yml bypasses the connector: "
            "No sandbox account",
        ]
    )


def test_bypasses_are_grouped_by_reason_and_name_their_entries(
    pytester: pytest.Pytester, tmp_path: Path
) -> None:
    config = {
        "acceptance_tests": {
            "connection": {
                "tests": [
                    {"config_path": "integration_tests/invalid_config.json", "status": "failed"},
                    {"bypass_reason": "The GSM config fails check"},
                ]
            },
            "basic_read": {"bypass_reason": "The GSM config fails check"},
            "full_refresh": {"bypass_reason": "Covered by basic_read"},
        }
    }
    root = _write_connector(tmp_path / "source-test", config)

    result = _run_collection(pytester, root, "-v")

    result.assert_outcomes(passed=1, skipped=2)
    result.stdout.fnmatch_lines(
        [
            "source-test: acceptance-test-config.yml declares 3 bypass(es), reported as skipped "
            "tests: connection.tests[[]1[]], basic_read: The GSM config fails check; "
            "full_refresh: Covered by basic_read",
            "*test_with_scenario[[]bypassed-connection-basic_read[]] SKIPPED*",
            "*test_with_scenario[[]bypassed-full_refresh[]] SKIPPED*",
        ]
    )
