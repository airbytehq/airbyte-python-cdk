# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Run acceptance tests in PyTest.

These tests leverage the same `acceptance-test-config.yml` configuration files as the
acceptance tests in CAT, but they run in PyTest instead of CAT. This allows us to run
the acceptance tests in the same local environment as we are developing in, speeding
up iteration cycles.
"""

from __future__ import annotations

import json
import tempfile
from contextlib import contextmanager, suppress
from pathlib import Path  # noqa: TC003  # Pydantic needs this (don't move to 'if typing' block)
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, cast

import yaml
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from airbyte_cdk.test.models.outcome import ExpectedOutcome

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Mapping


ScenarioCommand = Literal["spec", "check", "discover", "read"]
"""A connector command the standard tests run for a scenario. See `SECTION_COMMANDS`."""

SECTION_COMMANDS: Mapping[str, frozenset[ScenarioCommand]] = MappingProxyType(
    {
        "spec": frozenset({"spec"}),
        "connection": frozenset({"check"}),
        "discovery": frozenset({"discover"}),
        "basic_read": frozenset({"read"}),
        "full_refresh": frozenset({"read"}),
    }
)
"""The command that runs for a config listed under each section of `acceptance-test-config.yml`.

Each section stands for the connector command the Connector Acceptance Tests ran its configs
with, and the standard tests run that command for the config:

- `spec`: the config must validate against the connector's spec (`test_config_matches_spec`).
- `connection`: `check` must report the declared `status` (`test_check`).
- `discovery`: `discover` must succeed and return streams (`test_discover`).
- `basic_read`, `full_refresh`: a full-refresh `read` must succeed and return records
  (`test_basic_read`, `test_fail_read_with_bad_catalog`). It reads the streams of the config's
  `configured_catalog_path`, or every discovered stream when it declares none, minus its
  `empty_streams`.

A config listed under several sections runs the union of their commands. Configs listed only
under `incremental` are not collected: without input state, an incremental `read` checks the same
thing as a full-refresh one.
"""

SCENARIO_SECTIONS: tuple[str, ...] = tuple(SECTION_COMMANDS)
"""Every section that `get_scenarios()` reads, in the order its entries are collected."""

ALL_COMMANDS: frozenset[ScenarioCommand] = frozenset(
    command for commands in SECTION_COMMANDS.values() for command in commands
)
"""Every command; what a scenario built by hand (no `sections`) runs."""

_NO_RECORD_COMPARISON = (
    "it configured the Connector Acceptance Tests' comparison of the records read against "
    "recorded ones, which the standard tests do not run"
)

REMOVED_ENTRY_KEYS: Mapping[str, str] = MappingProxyType(
    {
        "expect_records": _NO_RECORD_COMPARISON,
        "exact_order": _NO_RECORD_COMPARISON,
        "ignored_fields": (
            "it listed the fields the Connector Acceptance Tests skipped when they compared "
            "records, against `expect_records` or between two consecutive full-refresh reads; "
            "the standard tests run neither comparison"
        ),
        "fail_on_extra_columns": (
            "the Connector Acceptance Tests accepted it but never enforced it, and the standard "
            "tests have no such check"
        ),
        "timeout_seconds": "the standard tests have no per-config timeout",
        "file_types": "the standard tests have no check that every supported file type is read",
    }
)
"""Keys an `acceptance-test-config.yml` entry can no longer set, with why they are not read.

They configured checks of the retired Connector Acceptance Tests (CAT) that the standard tests do
not run. `ConnectorTestScenario` declared some of them and ignored them; an entry that still sets
one is now rejected, so a config cannot look like it asks for a check that never runs.
"""

LEGACY_ENTRY_KEYS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "spec": frozenset({"spec_path", "backward_compatibility_tests_config", "deployment_mode"}),
        "connection": frozenset({"deployment_mode"}),
        "discovery": frozenset({"backward_compatibility_tests_config"}),
        "basic_read": frozenset({"client_container_config"}),
        "full_refresh": frozenset({"client_container_config"}),
    }
)
"""CAT keys that an entry of each section may still set, although the scenario does not read them.

`backward_compatibility_tests_config` of a `spec` entry is read from the file itself by the spec
backward-compatibility test (`disable_for_version`). The others configure CAT checks that have no
standard-test counterpart yet, and connectors still set them. They are accepted, and dropped, only
in the section CAT read them from. Every other key that is not a field of `ConnectorTestScenario`
is rejected (see `entry_key_problems`).
"""


BASIC_READ_ONLY_FIELDS: frozenset[str] = frozenset(
    {
        "validate_schema",
        "validate_primary_keys_data_type",
        "validate_state_messages",
        "expect_trace_message_on_failure",
    }
)
"""Fields that only a `basic_read` entry may set: CAT's opt-outs from its basic read checks.

`get_scenarios()` merges the entries for one config, and an opt-out declared by any of them is
kept, so the same key in another section would silently turn the config's `basic_read` check off.
CAT read these keys only from `basic_read` entries; its `discovery` entries had their own
`validate_primary_keys_data_type`, which skipped a different check (the schema type of the key
fields).
"""


POPULATED_FIELDS: Mapping[str, str] = MappingProxyType(
    {
        "sections": "the standard tests set it from the section that lists the entry",
        "test_strictness_level": (
            "it is read only at the top level of `acceptance-test-config.yml`"
        ),
    }
)
"""Fields of `ConnectorTestScenario` that `get_scenarios()` fills in, so an entry cannot set them.

An entry that set one would be overwritten without a word, so it is rejected, with the reason.
"""


def entry_key_problems(
    entry: Mapping[str, Any],
    sections: Iterable[str],
    *,
    from_file: bool = True,
) -> list[str]:
    """Return one message per key of an `acceptance-test-config.yml` entry that is not accepted.

    An entry may set the fields of `ConnectorTestScenario`, except the `POPULATED_FIELDS`, and the
    `LEGACY_ENTRY_KEYS` of the section that lists it; the `BASIC_READ_ONLY_FIELDS` only from a
    `basic_read` entry. A key of `REMOVED_ENTRY_KEYS` is reported as no longer read, a key of
    `POPULATED_FIELDS` as not an entry key, and a key of `BASIC_READ_ONLY_FIELDS` outside
    `basic_read` as read only there, with the reason; any other key as unknown. Each message names the key, the section and the entry's
    `config_path`, never a value.

    `from_file=False` checks the data a scenario is built from (by `get_scenarios()` or by hand),
    where the `POPULATED_FIELDS` are accepted, and so are the `BASIC_READ_ONLY_FIELDS` in every
    section: a scenario merged from several entries, or copied with every field written out,
    carries them whatever its sections.
    """
    sections = tuple(sections)
    accepted = set(ConnectorTestScenario.model_fields)
    if from_file:
        accepted -= set(POPULATED_FIELDS)
    for section in sections:
        accepted |= LEGACY_ENTRY_KEYS.get(section, frozenset())
    if from_file and "basic_read" not in sections:
        accepted -= BASIC_READ_ONLY_FIELDS
    where = (f"the `{'`/`'.join(sections)}` entry" if sections else "the scenario") + (
        f" for config `{entry['config_path']}`"
        if entry.get("config_path")
        else " without a `config_path`"
    )
    listed = sorted(accepted - set(POPULATED_FIELDS))

    problems: list[str] = []
    for key in entry:
        if key in accepted:
            continue
        if key in REMOVED_ENTRY_KEYS:
            problems.append(
                f"`{key}` in {where} is no longer read: {REMOVED_ENTRY_KEYS[key]}. Remove the key."
            )
        elif key in BASIC_READ_ONLY_FIELDS:
            problems.append(
                f"`{key}` in {where} is read only from a `basic_read` entry: it opts the config "
                "out of a check of the basic read test. Move it to the config's `basic_read` "
                "entry, or remove it."
            )
        elif key in POPULATED_FIELDS:
            problems.append(
                f"`{key}` in {where} is not an entry key: {POPULATED_FIELDS[key]}. "
                "Remove it from the entry."
            )
        else:
            problems.append(
                f"`{key}` in {where} is not a key the standard tests read. Remove it, or fix "
                f"its spelling (accepted: {', '.join(f'`{k}`' for k in listed)})."
            )
    return problems


class ConnectorTestScenario(BaseModel):
    """Acceptance test scenario, as a Pydantic model.

    This class represents an acceptance test scenario, which is a single test case
    that can be run against a connector. It is used to deserialize and validate the
    acceptance test configuration file.

    Every field is read by the standard tests (a unit test enforces it). An entry that sets a key
    that is not a field is rejected, except for the `LEGACY_ENTRY_KEYS` of its section; a key the
    standard tests used to accept and ignore is reported as no longer read (`REMOVED_ENTRY_KEYS`).
    `get_scenarios()` also rejects an entry outside `basic_read` that sets one of the
    `BASIC_READ_ONLY_FIELDS`, and an entry that sets one of the `POPULATED_FIELDS`, which it fills
    in itself.
    """

    # Allows the class to be hashable, which PyTest will require
    # when we use to parameterize tests.
    # `hide_input_in_errors`: a validation error must not print config values.
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    class AcceptanceTestEmptyStream(BaseModel):
        model_config = ConfigDict(extra="forbid")

        name: str
        bypass_reason: str | None = None

        # bypass reason does not affect equality
        def __hash__(self) -> int:
            return hash(self.name)

    config_path: Path | None = None
    config_dict: dict[str, Any] | None = None

    _id: str | None = None  # Used to override the default ID generation

    configured_catalog_path: Path | None = None
    empty_streams: list[AcceptanceTestEmptyStream] | None = None
    status: Literal["succeed", "failed", "exception"] | None = None
    # Why this scenario is not run: set on the placeholder scenarios that report a declared
    # `bypass_reason` of `acceptance-test-config.yml` as skipped tests.
    bypass_reason: str | None = None
    # CAT's `basic_read` opt-out from validating records against the stream schemas.
    validate_schema: bool = True
    # CAT's `basic_read` opt-out from checking the primary key of every record.
    validate_primary_keys_data_type: bool = True
    # CAT's `basic_read` opt-out from checking that every stream emits per-stream state.
    validate_state_messages: bool = True
    # CAT's `basic_read` opt-out from requiring a well-formed error trace from a failing read.
    expect_trace_message_on_failure: bool = True
    # The connector's top-level `test_strictness_level` in `acceptance-test-config.yml`. Populated
    # by `get_scenarios()`; at `high`, `validate_schema: false` is rejected, as in CAT.
    test_strictness_level: Literal["low", "high"] | None = None
    # The sections of `acceptance-test-config.yml` that list this config. Populated by
    # `get_scenarios()`; empty for scenarios built by hand, which run every command.
    sections: tuple[str, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def _reject_keys_not_read(cls, data: Any) -> Any:
        """Reject keys that are not read, and drop the `LEGACY_ENTRY_KEYS` of the entry's section."""
        if not isinstance(data, dict):
            return data
        sections = data.get("sections") or ()
        if isinstance(sections, str):
            sections = (sections,)
        problems = entry_key_problems(data, sections, from_file=False)
        if problems:
            raise ValueError("\n".join(problems))
        legacy = {key for section in sections for key in LEGACY_ENTRY_KEYS.get(section, ())}
        return {key: value for key, value in data.items() if key not in legacy}

    @field_validator("sections")
    @classmethod
    def _sections_must_be_known(cls, sections: tuple[str, ...]) -> tuple[str, ...]:
        unknown = [section for section in sections if section not in SECTION_COMMANDS]
        if unknown:
            raise ValueError(
                f"Unknown `acceptance-test-config.yml` section(s) {unknown}; "
                f"expected any of {list(SCENARIO_SECTIONS)}."
            )
        return sections

    def get_config_dict(
        self,
        *,
        connector_root: Path,
        empty_if_missing: bool,
    ) -> dict[str, Any]:
        """Return the config dictionary.

        If a config dictionary has already been loaded, return it. Otherwise, load
        the config file and return the dictionary.

        If `self.config_dict` and `self.config_path` are both `None`:
        - return an empty dictionary if `empty_if_missing` is True
        - raise a ValueError if `empty_if_missing` is False
        """
        if self.config_dict is not None:
            return self.config_dict

        if self.config_path is not None:
            config_path = self.config_path
            if not config_path.is_absolute():
                # We usually receive a relative path here. Let's resolve it.
                config_path = (connector_root / self.config_path).resolve().absolute()

            return cast(
                dict[str, Any],
                yaml.safe_load(config_path.read_text()),
            )

        if empty_if_missing:
            return {}

        raise ValueError("No config dictionary or path provided.")

    @property
    def expected_outcome(self) -> ExpectedOutcome:
        """Whether the test scenario expects an exception to be raised.

        Returns True if the scenario expects an exception, False if it does not,
        and None if there is no set expectation.
        """
        return ExpectedOutcome.from_status_str(self.status)

    @property
    def commands(self) -> frozenset[ScenarioCommand]:
        """The connector commands the standard tests run for this scenario.

        A config runs the command of every `acceptance-test-config.yml` section that lists it
        (`SECTION_COMMANDS`). A scenario built by hand, with no `sections`, runs every command.
        """
        if not self.sections:
            return ALL_COMMANDS

        return frozenset(
            command for section in self.sections for command in SECTION_COMMANDS[section]
        )

    def runs(self, *commands: ScenarioCommand) -> bool:
        """Whether any of `commands` runs for this scenario. See `commands`."""
        return bool(self.commands & frozenset(commands))

    @property
    def is_basic_read_config(self) -> bool:
        """Whether this config is listed under `basic_read` (or the scenario was built by hand).

        Only the records of these configs are checked as in CAT's basic read test: every stream
        not declared in `empty_streams` returns records, every record matches its stream's
        schema unless `validate_schema: false` is set, every record has a usable primary key
        unless `validate_primary_keys_data_type: false` is set, and every stream emits state
        unless `validate_state_messages: false` is set. A config read only because it is listed
        under `full_refresh` never had CAT's basic read checks, and has no `basic_read` entry to
        declare those exceptions in, so its `read` only has to return some records.
        """
        return not self.sections or "basic_read" in self.sections

    @property
    def id(self) -> str:
        """Return a unique identifier for the test scenario.

        This is used by PyTest to identify the test scenario.
        """
        if self._id:
            return self._id

        if self.config_path:
            return self.config_path.stem

        return str(hash(self))

    def __str__(self) -> str:
        return f"'{self.id}' Test Scenario"

    @contextmanager
    def with_temp_config_file(
        self,
        connector_root: Path,
    ) -> Generator[Path, None, None]:
        """Yield a temporary JSON file path containing the config dict and delete it on exit."""
        config = self.get_config_dict(
            empty_if_missing=True,
            connector_root=connector_root,
        )
        with tempfile.NamedTemporaryFile(
            prefix="config-",
            suffix=".json",
            mode="w",
            delete=False,  # Don't fail if cannot delete the file on exit
            encoding="utf-8",
        ) as temp_file:
            temp_file.write(json.dumps(config))
            temp_file.flush()
            # Allow the file to be read and written by other processes
            # Write access is needed for connectors with config migration (e.g., OAuth token refresh)
            temp_path = Path(temp_file.name)
            temp_path.chmod(temp_path.stat().st_mode | 0o666)
            yield temp_path

        # attempt cleanup, ignore errors
        with suppress(OSError):
            temp_path.unlink()

    def without_expected_outcome(self) -> ConnectorTestScenario:
        """Return a copy of the scenario that does not expect failure or success.

        This is useful when running multiple steps, to defer the expectations to a later step.
        """
        return ConnectorTestScenario(
            **self.model_dump(exclude={"status"}),
        )

    def with_expecting_failure(self) -> ConnectorTestScenario:
        """Return a copy of the scenario that expects failure.

        This is useful when deriving new scenarios from existing ones.
        """
        if self.status == "failed":
            return self

        return ConnectorTestScenario(
            **self.model_dump(exclude={"status"}),
            status="failed",
        )

    def with_expecting_success(self) -> ConnectorTestScenario:
        """Return a copy of the scenario that expects success.

        This is useful when deriving new scenarios from existing ones.
        """
        if self.status == "succeed":
            return self

        return ConnectorTestScenario(
            **self.model_dump(exclude={"status"}),
            status="succeed",
        )

    def with_default_success(self) -> ConnectorTestScenario:
        """Return the scenario, expecting success when it declares no `status`.

        No `status` in `acceptance-test-config.yml` means `succeed`, the CAT default, for every
        command a config runs. `ALLOW_ANY` is kept only for intermediate steps that defer their
        expectation (see `without_expected_outcome`).
        """
        if self.status is not None:
            return self

        return self.with_expecting_success()

    @property
    def requires_creds(self) -> bool:
        """Return True if the scenario requires credentials to run."""
        return bool(self.config_path and "secrets" in self.config_path.parts)
