# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Read `acceptance-test-config.yml` into test scenarios and declared bypasses.

Every connector that runs the standard tests (Python and manifest-only sources and destinations)
needs an `acceptance-test-config.yml`, and the file must yield at least one runnable scenario (a
config listed under a collected section) or declare why it has none:

- a top-level `bypass_reason` bypasses the whole connector; a file holding only that key is the
  opt-out for a connector without test configs;
- a `bypass_reason` on a collected section (`connection: {bypass_reason: ...}`) bypasses that
  section, the layout the Connector Acceptance Tests used;
- a `bypass_reason` on an entry without `config_path` bypasses that entry.

Only Java connectors, which are tested by their own Gradle suites, may have no file at all.

Failure messages name sections, entries and keys, never values, as configs may hold secrets.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeGuard

import yaml
from pydantic import ValidationError

from airbyte_cdk.test.models.scenario import (
    SCENARIO_SECTIONS,
    ConnectorTestScenario,
    entry_key_problems,
)
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG, METADATA_YAML

JAVA_LANGUAGE_TAGS = frozenset({"language:java"})
"""The `metadata.yaml` tags of connectors that may have no `acceptance-test-config.yml`."""

JAVA_BYPASS_REASON = (
    f"Java connector without {ACCEPTANCE_TEST_CONFIG}: Java connectors are tested by their "
    "Gradle suites, not by the standard tests."
)

BYPASS_ONLY_EXAMPLE = 'bypass_reason: "No test config in GSM for this connector"'


class AcceptanceTestConfigError(ValueError):
    """`acceptance-test-config.yml` cannot be turned into test scenarios."""


class MissingAcceptanceTestConfigError(AcceptanceTestConfigError, FileNotFoundError):
    """A connector that runs the standard tests has no `acceptance-test-config.yml`."""


@dataclass(frozen=True)
class ScenarioBypass:
    """A declared reason why a part of `acceptance-test-config.yml` has no runnable config."""

    reason: str
    location: str
    """Where the reason is declared, e.g. `acceptance_tests.connection.bypass_reason`."""
    sections: tuple[str, ...] = ()
    """The collected sections it bypasses; empty for a connector-wide bypass."""


@dataclass
class CollectedScenarios:
    """What `acceptance-test-config.yml` yields: runnable scenarios, bypasses, and why."""

    config_path: Path
    scenarios: list[ConnectorTestScenario] = field(default_factory=list)
    """One scenario per runnable entry, before deduplication by config."""
    bypasses: list[ScenarioBypass] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    """Why each section or entry that is not a runnable config or a bypass produced nothing."""

    def describe_empty(self, connector_name: str) -> str:
        """The collection error for a connector with no runnable scenario and no bypass."""
        found = self.notes or ["(no sections)"]
        return "\n".join(
            [
                f"No test scenarios for connector '{connector_name}': {self.config_path} lists "
                "no config the standard tests can run, and declares no `bypass_reason`. "
                "Found:",
                *(f"  - {note}" for note in found),
                "Add a `config_path` entry under one of "
                f"{', '.join(f'`{section}`' for section in SCENARIO_SECTIONS)}, or declare why "
                "the connector has none: a `bypass_reason` on each section "
                "(`connection: {bypass_reason: ...}`), or a top-level `bypass_reason` for the "
                "whole connector.",
            ]
        )


def section_entries(section: Any) -> list[Any] | None:
    """Return the entries of an `acceptance_tests` section, in either layout.

    The current layout lists them under `tests` (`spec: {tests: [...]}`); the legacy one makes the
    section the list itself (`spec: [...]`). A section of any other shape has no entries.
    """
    if isinstance(section, Mapping):
        tests = section.get("tests")
        return tests if isinstance(tests, list) else None
    if isinstance(section, list):
        return section
    return None


def is_java_connector(connector_root: Path) -> bool:
    """Whether `metadata.yaml` tags the connector as Java (`language:java`)."""
    metadata_path = connector_root / METADATA_YAML
    try:
        metadata = yaml.safe_load(metadata_path.read_text())
    except (OSError, yaml.YAMLError):
        return False
    data = metadata.get("data") if isinstance(metadata, Mapping) else None
    tags = data.get("tags") if isinstance(data, Mapping) else None
    return isinstance(tags, list) and any(tag in JAVA_LANGUAGE_TAGS for tag in tags)


def load_acceptance_test_config(config_path: Path) -> dict[str, Any]:
    """Load and validate the layout of `acceptance-test-config.yml`.

    Raises:
        MissingAcceptanceTestConfigError: The file does not exist.
        AcceptanceTestConfigError: The file is not valid YAML, is empty, uses the legacy
            top-level `tests:` layout, or has neither `acceptance_tests` nor a top-level
            `bypass_reason`.
    """
    config_path = config_path.absolute()
    if not config_path.exists():
        raise MissingAcceptanceTestConfigError(
            f"Acceptance test config file not found at: {config_path}. Every Python and "
            "manifest-only connector needs one for the standard tests (only Java connectors may "
            "omit it). List the connector's test configs in it, or, if it has none, create the "
            f"file with only a top-level `bypass_reason` that says why, e.g. "
            f"`{BYPASS_ONLY_EXAMPLE}`."
        )

    try:
        config = yaml.safe_load(config_path.read_text())
    except yaml.YAMLError as error:
        raise AcceptanceTestConfigError(f"{config_path} is not valid YAML: {error}") from error

    if not isinstance(config, Mapping):
        found = "is empty" if config is None else f"holds a {type(config).__name__}"
        raise AcceptanceTestConfigError(
            f"{config_path} {found}; expected a mapping with an `acceptance_tests` key, or only a "
            f"top-level `bypass_reason` (`{BYPASS_ONLY_EXAMPLE}`)."
        )

    top_level_reason = config.get("bypass_reason")
    if "bypass_reason" in config and not _is_reason(top_level_reason):
        raise AcceptanceTestConfigError(
            f"The top-level `bypass_reason` of {config_path} must be a non-empty string."
        )

    if "acceptance_tests" not in config:
        if "tests" in config:
            raise AcceptanceTestConfigError(
                f"{config_path} uses the legacy layout with a top-level `tests:` key, which the "
                "standard tests do not read. Migrate it: rename `tests:` to `acceptance_tests:` "
                "and nest each section's entries under `tests` "
                "(`connection: {tests: [{config_path: ...}]}`), keeping the other top-level keys."
            )
        if top_level_reason is None:
            raise AcceptanceTestConfigError(
                f"{config_path} has no `acceptance_tests` key and no top-level `bypass_reason`. "
                f"Found top-level keys: {', '.join(f'`{key}`' for key in config) or 'none'}."
            )
    elif not isinstance(config["acceptance_tests"], Mapping):
        raise AcceptanceTestConfigError(
            f"`acceptance_tests` in {config_path} must be a mapping of sections "
            f"(`connection: {{tests: [...]}}`), not a {type(config['acceptance_tests']).__name__}."
        )

    return dict(config)


def collect_scenarios(config: Mapping[str, Any], config_path: Path) -> CollectedScenarios:
    """Collect the runnable scenarios and the declared bypasses of a loaded config.

    Every section of `SCENARIO_SECTIONS` is read, in that order, in the current layout
    (`section: {tests: [...]}`) and the legacy list layout (`section: [...]`). An entry with a
    `config_path` is a runnable scenario, except an IAM-role config, which the standard tests do
    not support. An entry without `config_path` contributes its `bypass_reason`, if it has one.
    A section `bypass_reason` counts only when the section lists no runnable config.

    Raises:
        AcceptanceTestConfigError: An entry sets a key the standard tests do not read (see
            `entry_key_problems`; every entry is checked, including those that are not run), an
            entry is not a valid scenario (e.g. `empty_streams` as plain strings), or a top-level
            `bypass_reason` is declared next to runnable configs, which would leave it unclear
            whether the connector is tested. The first two list every problem of the file.
    """
    collected = CollectedScenarios(config_path=config_path)
    top_level_reason = config.get("bypass_reason")
    if _is_reason(top_level_reason):
        collected.bypasses.append(ScenarioBypass(reason=top_level_reason, location="bypass_reason"))

    acceptance_tests = config.get("acceptance_tests") or {}
    _raise_if_invalid(
        config_path,
        [
            problem
            for section_name in SCENARIO_SECTIONS
            for entry in section_entries(acceptance_tests.get(section_name)) or []
            if isinstance(entry, Mapping)
            for problem in entry_key_problems(entry, (section_name,))
        ],
    )

    strictness_level = config.get("test_strictness_level")
    problems: list[str] = []
    for section_name in SCENARIO_SECTIONS:
        if section_name in acceptance_tests:
            _collect_section(
                collected,
                problems,
                section_name,
                acceptance_tests[section_name],
                strictness_level=strictness_level,
            )
    _raise_if_invalid(config_path, problems)
    for section_name in acceptance_tests:
        if section_name not in SCENARIO_SECTIONS:
            collected.notes.append(
                f"`acceptance_tests.{section_name}`: not a section the standard tests collect."
            )

    if collected.scenarios and _is_reason(top_level_reason):
        raise AcceptanceTestConfigError(
            f"{config_path} declares a top-level `bypass_reason`, which bypasses the whole "
            f"connector, but also lists {len(collected.scenarios)} runnable config entries. Remove "
            "the top-level `bypass_reason` to run them, or remove the entries."
        )
    return collected


def _collect_section(
    collected: CollectedScenarios,
    problems: list[str],
    section_name: str,
    section: Any,
    *,
    strictness_level: Any,
) -> None:
    where = f"acceptance_tests.{section_name}"
    entries = section_entries(section)
    section_reason = section.get("bypass_reason") if isinstance(section, Mapping) else None

    runnable = 0
    if entries is None:
        if section is None:
            collected.notes.append(f"`{where}`: empty.")
        elif not isinstance(section, Mapping):
            collected.notes.append(
                f"`{where}`: a {type(section).__name__}, not a mapping with a `tests` list."
            )
        elif section_reason is None:
            keys = ", ".join(f"`{key}`" for key in section) or "none"
            collected.notes.append(
                f"`{where}`: no `tests` list and no `bypass_reason` (keys: {keys})."
            )
    else:
        prefix = f"{where}.tests" if isinstance(section, Mapping) else where
        if not entries:
            collected.notes.append(f"`{prefix}`: empty list.")
        for index, entry in enumerate(entries):
            location = f"{prefix}[{index}]"
            if _collect_entry(collected, problems, section_name, location, entry, strictness_level):
                runnable += 1

    if section_reason is None:
        return
    if not _is_reason(section_reason):
        collected.notes.append(f"`{where}.bypass_reason`: not a non-empty string, so not counted.")
    elif runnable:
        collected.notes.append(
            f"`{where}.bypass_reason`: ignored, as the section lists {runnable} runnable config "
            "entries."
        )
    else:
        collected.bypasses.append(
            ScenarioBypass(
                reason=section_reason,
                location=f"{where}.bypass_reason",
                sections=(section_name,),
            )
        )


def _collect_entry(
    collected: CollectedScenarios,
    problems: list[str],
    section_name: str,
    location: str,
    entry: Any,
    strictness_level: Any,
) -> bool:
    """Collect one section entry; return whether it is a runnable scenario.

    An entry that is not a valid scenario adds its problems to `problems` instead.
    """
    if not isinstance(entry, Mapping):
        collected.notes.append(f"`{location}`: a {type(entry).__name__}, not a mapping.")
        return False

    config_path = entry.get("config_path")
    if not config_path:
        reason = entry.get("bypass_reason")
        if _is_reason(reason):
            collected.bypasses.append(
                ScenarioBypass(
                    reason=reason,
                    location=f"{location}.bypass_reason",
                    sections=(section_name,),
                )
            )
        else:
            keys = ", ".join(f"`{key}`" for key in entry) or "none"
            collected.notes.append(
                f"`{location}`: no `config_path` and no `bypass_reason` (keys: {keys})."
            )
        return False

    if "iam_role" in str(config_path):
        collected.notes.append(
            f"`{location}`: an IAM-role config, which the standard tests do not support."
        )
        return False

    fields = {key: value for key, value in entry.items() if key != "bypass_reason"}
    try:
        scenario = ConnectorTestScenario.model_validate(
            {
                **fields,
                "sections": (section_name,),
                "test_strictness_level": strictness_level,
            }
        )
    except ValidationError as error:
        # Pydantic's own message repeats the offending input; name the field and rule only.
        problems.extend(
            f"`{section_name}` entry for config `{config_path}`: "
            f"`{'.'.join(str(part) for part in detail['loc']) or '(entry)'}`: {detail['msg']}"
            for detail in error.errors(include_input=False, include_url=False)
        )
        return False
    collected.scenarios.append(scenario)
    return True


def _raise_if_invalid(config_path: Path, problems: list[str]) -> None:
    """Fail with every problem found in `acceptance-test-config.yml`, one per line, if any."""
    if problems:
        raise AcceptanceTestConfigError(
            f"`{config_path}` is not valid for the standard tests:\n"
            + "\n".join(f"- {problem}" for problem in problems)
        )


def _is_reason(value: Any) -> TypeGuard[str]:
    return isinstance(value, str) and bool(value.strip())
