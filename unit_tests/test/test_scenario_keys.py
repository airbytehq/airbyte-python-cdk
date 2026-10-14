# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""`ConnectorTestScenario` declares only keys the standard tests read, and rejects the rest.

An `acceptance-test-config.yml` entry that sets a key the standard tests do not read fails
collection with a message naming the key, the section and the config, instead of being ignored.
"""

from __future__ import annotations

import ast
import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

import airbyte_cdk.test
from airbyte_cdk.test.models import scenario as scenario_module
from airbyte_cdk.test.models.scenario import (
    LEGACY_ENTRY_KEYS,
    REMOVED_ENTRY_KEYS,
    SCENARIO_SECTIONS,
    ConnectorTestScenario,
    entry_key_problems,
)
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG

POKEAPI_RESOURCE_DIR = Path(__file__).parent.parent / "resources" / "source_pokeapi_w_components_py"
SECRET = "hunter2-do-not-print"

# Fields with no reader yet, each with the change that adds the reader. The guard below fails once
# such a field is read, so the entry is removed by the change that starts reading it.
AWAITING_READER: dict[str, str] = {
    "bypass_reason": "the zero-scenario rule, airbyte-internal-issues#17131",
}


# --- Guard: every field is read outside the model --------------------------------------------


def _names_used_by_the_harness() -> set[str]:
    """Attribute names, keyword names and string constants used in `airbyte_cdk.test`, minus the model.

    Only the test harness consumes scenarios, so the rest of the CDK is not scanned: a parameter
    elsewhere that happens to share a field's name must not count as a reader.
    """
    harness_root = Path(airbyte_cdk.test.__file__).parent
    model_module = Path(scenario_module.__file__)
    names: set[str] = set()
    for path in harness_root.rglob("*.py"):
        if path == model_module:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Attribute):
                names.add(node.attr)
            elif isinstance(node, ast.keyword) and node.arg:
                names.add(node.arg)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                names.add(node.value)
    return names


def _fields_read_by_each_model_member() -> dict[str, set[str]]:
    """For each method and property of `ConnectorTestScenario`, the `self.<name>` it reads.

    A field read only through a member (`config_dict` through `get_config_dict()`) is read
    whenever that member is used outside the model.
    """
    tree = ast.parse(Path(scenario_module.__file__).read_text(encoding="utf-8"))
    model = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == ConnectorTestScenario.__name__
    )
    return {
        member.name: {
            node.attr
            for node in ast.walk(member)
            if isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "self"
        }
        for member in model.body
        if isinstance(member, ast.FunctionDef)
    }


def _fields_without_reader() -> set[str]:
    used = _names_used_by_the_harness()
    read_through_members = {
        field
        for member, fields in _fields_read_by_each_model_member().items()
        if member in used
        for field in fields
    }
    return {
        field
        for field in ConnectorTestScenario.model_fields
        if field not in used and field not in read_through_members
    }


def test_every_scenario_field_is_read_outside_the_model() -> None:
    unread = _fields_without_reader() - set(AWAITING_READER)

    assert not unread, (
        f"`ConnectorTestScenario` declares fields that nothing in `airbyte_cdk.test` reads: "
        f"{sorted(unread)}. A config that sets such a field looks like it configures a check "
        "that never runs. Read the field in a test, or remove it (and add it to "
        "`REMOVED_ENTRY_KEYS` so configs that still set it fail loudly)."
    )


@pytest.mark.parametrize("field", sorted(AWAITING_READER))
def test_fields_awaiting_a_reader_are_still_unread(field: str) -> None:
    assert field in ConnectorTestScenario.model_fields
    assert field in _fields_without_reader(), (
        f"`{field}` is read now ({AWAITING_READER[field]}): remove it from `AWAITING_READER`."
    )


def test_guard_detects_an_unread_field() -> None:
    """The guard is not vacuous: the fields this change removed would have failed it."""
    used = _names_used_by_the_harness()
    for removed in ("expect_records", "timeout_seconds", "file_types"):
        assert removed not in used


def test_removed_keys_are_not_fields_nor_legacy_keys() -> None:
    assert set(REMOVED_ENTRY_KEYS) == {
        "expect_records",
        "exact_order",
        "ignored_fields",
        "fail_on_extra_columns",
        "timeout_seconds",
        "file_types",
    }
    assert not set(REMOVED_ENTRY_KEYS) & set(ConnectorTestScenario.model_fields)
    for section, keys in LEGACY_ENTRY_KEYS.items():
        assert section in SCENARIO_SECTIONS
        assert not keys & set(REMOVED_ENTRY_KEYS), section
        assert not keys & set(ConnectorTestScenario.model_fields), section


# --- The model rejects keys it does not read -------------------------------------------------


@pytest.mark.parametrize("key", sorted(REMOVED_ENTRY_KEYS))
@pytest.mark.parametrize("section", ["basic_read", "full_refresh"])
def test_removed_key_is_rejected_as_no_longer_read(key: str, section: str) -> None:
    with pytest.raises(ValidationError) as error:
        ConnectorTestScenario.model_validate(
            {"config_path": "secrets/config.json", key: {"path": SECRET}, "sections": (section,)}
        )

    message = str(error.value)
    assert (
        f"`{key}` in the `{section}` entry for config `secrets/config.json` is no longer read: "
        f"{REMOVED_ENTRY_KEYS[key]}. Remove the key." in message
    )
    assert SECRET not in message


def test_unknown_key_is_rejected() -> None:
    with pytest.raises(ValidationError) as error:
        ConnectorTestScenario.model_validate(
            {
                "config_path": "secrets/config.json",
                "config_dict": {"api_key": SECRET},
                "empty_stream": [{"name": "users"}],
                "sections": ("basic_read",),
            }
        )

    message = str(error.value)
    assert (
        "`empty_stream` in the `basic_read` entry for config `secrets/config.json` is not a key "
        "the standard tests read. Remove it, or fix its spelling (accepted: " in message
    )
    assert "`empty_streams`" in message
    assert "`sections`" not in message, "filled in by get_scenarios(), not a key to suggest"
    assert SECRET not in message


def test_unknown_key_of_a_scenario_built_by_hand_is_rejected() -> None:
    with pytest.raises(ValidationError, match="`expect_records` in the scenario for config"):
        ConnectorTestScenario(  # type: ignore[call-arg]
            config_path=Path("secrets/config.json"), expect_records={"path": "records.jsonl"}
        )


@pytest.mark.parametrize(
    "section, key",
    [(section, key) for section, keys in LEGACY_ENTRY_KEYS.items() for key in sorted(keys)],
)
def test_legacy_key_is_accepted_and_dropped_in_its_section(section: str, key: str) -> None:
    scenario = ConnectorTestScenario.model_validate(
        {"config_path": "secrets/config.json", key: {"anything": 1}, "sections": (section,)}
    )

    assert key not in scenario.model_dump()
    assert scenario.sections == (section,)


def test_legacy_key_is_rejected_outside_its_section() -> None:
    with pytest.raises(
        ValidationError,
        match="`backward_compatibility_tests_config` in the `basic_read` entry .* is not a key",
    ):
        ConnectorTestScenario.model_validate(
            {
                "config_path": "secrets/config.json",
                "backward_compatibility_tests_config": {"disable_for_version": "1.0.0"},
                "sections": ("basic_read",),
            }
        )


def test_unknown_key_of_an_empty_stream_is_rejected() -> None:
    with pytest.raises(ValidationError, match="empty_streams.0.reason"):
        ConnectorTestScenario.model_validate(
            {
                "config_path": "secrets/config.json",
                "empty_streams": [{"name": "users", "reason": "no data"}],
                "sections": ("basic_read",),
            }
        )


def test_bypass_reason_is_accepted() -> None:
    scenario = ConnectorTestScenario.model_validate(
        {"config_path": "secrets/config.json", "bypass_reason": "no sandbox", "sections": ("spec",)}
    )
    assert scenario.bypass_reason == "no sandbox"


def test_entry_key_problems_without_config_path() -> None:
    assert entry_key_problems({"spec_path": "manifest.yaml", "timeout_seconds": 60}, ["spec"]) == [
        "`timeout_seconds` in the `spec` entry without a `config_path` is no longer read: "
        "the standard tests have no per-config timeout. Remove the key."
    ]


# --- get_scenarios() fails collection with every problem of the file --------------------------


def _suite(tmp_path: Path, acceptance_tests: dict[str, Any]) -> type[DockerConnectorTestSuite]:
    connector_root = tmp_path / "source-test"
    connector_root.mkdir(exist_ok=True)
    (connector_root / ACCEPTANCE_TEST_CONFIG).write_text(
        yaml.safe_dump({"acceptance_tests": acceptance_tests})
    )
    shutil.copy(POKEAPI_RESOURCE_DIR / "metadata.yaml", connector_root / "metadata.yaml")

    return type(
        "_Suite",
        (DockerConnectorTestSuite,),
        {"get_connector_root_dir": classmethod(lambda cls: connector_root)},
    )


def test_get_scenarios_reports_every_key_not_read(tmp_path: Path) -> None:
    suite = _suite(
        tmp_path,
        {
            "spec": {"tests": [{"spec_path": "manifest.yaml", "timeout_seconds": 60}]},
            "connection": {
                "tests": [
                    {"config_path": "secrets/config.json", "status": "succeed"},
                    {
                        "config_path": "secrets/config_iam_role.json",
                        "status": "succeed",
                        "timeout_seconds": 60,
                    },
                ]
            },
            "basic_read": {
                "tests": [
                    {
                        "config_path": "secrets/config.json",
                        "config_dict": {"api_key": SECRET},
                        "expect_records": {"path": "integration_tests/expected_records.jsonl"},
                        "fail_on_extra_columns": False,
                    }
                ]
            },
            "full_refresh": {
                "tests": [
                    {
                        "config_path": "secrets/config.json",
                        "ignored_fields": {"users": [{"name": "updated_at"}]},
                        "unknown_key": SECRET,
                    }
                ]
            },
            # Not collected: its keys are not checked.
            "incremental": {
                "tests": [{"config_path": "secrets/config.json", "timeout_seconds": 3600}]
            },
        },
    )

    with pytest.raises(ValueError) as error:
        suite.get_scenarios()

    message = str(error.value)
    lines = message.splitlines()
    assert lines[0] == (
        f"`{tmp_path / 'source-test' / ACCEPTANCE_TEST_CONFIG}` is not valid for the standard tests:"
    )
    assert [line.split(" is ")[0] for line in lines[1:]] == [
        "- `timeout_seconds` in the `spec` entry without a `config_path`",
        "- `timeout_seconds` in the `connection` entry for config `secrets/config_iam_role.json`",
        "- `expect_records` in the `basic_read` entry for config `secrets/config.json`",
        "- `fail_on_extra_columns` in the `basic_read` entry for config `secrets/config.json`",
        "- `ignored_fields` in the `full_refresh` entry for config `secrets/config.json`",
        "- `unknown_key` in the `full_refresh` entry for config `secrets/config.json`",
    ]
    assert "no longer read" in lines[1]
    assert "is not a key the standard tests read" in lines[-1]
    assert SECRET not in message


def test_get_scenarios_reports_invalid_values_without_printing_them(tmp_path: Path) -> None:
    suite = _suite(
        tmp_path,
        {
            "connection": {
                "tests": [
                    {"config_path": "secrets/config.json", "status": SECRET},
                    {"config_path": "secrets/other.json", "config_dict": SECRET},
                ]
            }
        },
    )

    with pytest.raises(ValueError) as error:
        suite.get_scenarios()

    message = str(error.value)
    assert "- `connection` entry for config `secrets/config.json`: `status`: Input should be" in (
        message
    )
    assert "- `connection` entry for config `secrets/other.json`: `config_dict`:" in message
    assert SECRET not in message


def test_get_scenarios_accepts_the_keys_still_in_use(tmp_path: Path) -> None:
    suite = _suite(
        tmp_path,
        {
            "spec": {
                "tests": [
                    {
                        "spec_path": "manifest.yaml",
                        "deployment_mode": "cloud",
                        "backward_compatibility_tests_config": {"disable_for_version": "1.0.0"},
                    },
                    {"config_path": "secrets/config.json", "spec_path": "manifest.yaml"},
                ]
            },
            "connection": {
                "tests": [
                    {
                        "config_path": "secrets/config.json",
                        "status": "succeed",
                        "deployment_mode": "oss",
                    }
                ]
            },
            "discovery": {
                "tests": [
                    {
                        "config_path": "secrets/config.json",
                        "backward_compatibility_tests_config": {"disable_for_version": "1.0.0"},
                    }
                ]
            },
            "basic_read": {
                "tests": [
                    {
                        "config_path": "secrets/config.json",
                        "configured_catalog_path": "integration_tests/configured_catalog.json",
                        "empty_streams": [{"name": "users", "bypass_reason": "no data"}],
                        "validate_schema": False,
                        "expect_trace_message_on_failure": False,
                        "validate_state_messages": False,
                        "client_container_config": {"client_container_dockerfile_path": "x"},
                    }
                ]
            },
            "full_refresh": {
                "tests": [
                    {
                        "config_path": "secrets/config.json",
                        "configured_catalog_path": "integration_tests/configured_catalog.json",
                        "client_container_config": {"client_container_dockerfile_path": "x"},
                    }
                ]
            },
        },
    )

    [scenario] = suite.get_scenarios()

    assert scenario.sections == ("spec", "connection", "discovery", "basic_read", "full_refresh")
    assert scenario.validate_schema is False
