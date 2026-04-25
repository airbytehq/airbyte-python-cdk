# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Load smoke-test scenarios from a connector's `metadata.yaml`.

Scenarios declared under
`data.connectorTestSuitesOptions[].suite == 'smokeTests'` are converted into
`ConnectorTestScenario` instances so they can be parametrized by the standard
test suite alongside scenarios discovered from `acceptance-test-config.yml`.

`configSettings` values support two interpolation sigils:

- `${secret-ref:<item>/<section>/<field>}` resolves a credential previously
  written to `secrets/.env.<section>` by an out-of-band fetcher (today the
  airbyte-ops-mcp `secrets fetch` command). All three segments are required.
- `${relative-date:-P<duration>}` resolves an ISO-8601 duration anchored at
  today's UTC midnight and renders as an RFC 3339 timestamp
  (e.g. `2026-03-22T00:00:00Z`).

`AIRBYTE_TEST_CREDS_PROVIDER=1pass` and `AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME`
are required *only when* a `${secret-ref:...}` sigil is actually present in
the resolved scenario. Pure `${relative-date:...}` or literal scenarios resolve
without any provider env vars.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import isodate
import yaml

from airbyte_cdk.test.models import ConnectorTestScenario

SMOKE_TEST_SUITE_NAME = "smokeTests"
SECRETS_DIRNAME = "secrets"
ENV_FILE_PREFIX = ".env."

_SECRET_REF_PATTERN = re.compile(
    r"^\$\{secret-ref:(?P<item>[^/}]+)/(?P<section>[^/}]+)/(?P<field>[^/}]+)\}$"
)
_RELATIVE_DATE_PATTERN = re.compile(r"^\$\{relative-date:-(?P<duration>P[^}]+)\}$")


class SigilResolutionError(ValueError):
    """Raised when a sigil cannot be resolved into a concrete value."""


def load_metadata_smoke_test_scenarios(
    metadata_path: Path,
    connector_root: Path,
) -> list[ConnectorTestScenario]:
    """Return smoke-test scenarios declared in a connector's `metadata.yaml`.

    Returns an empty list when the file does not exist or contains no
    `smokeTests` suite. Sigil resolution is eager: each scenario's
    `configSettings` is fully materialized before the `ConnectorTestScenario`
    is constructed, so downstream test code never sees an unresolved sigil.
    """
    if not metadata_path.exists():
        return []

    metadata = yaml.safe_load(metadata_path.read_text()) or {}
    suites = metadata.get("data", {}).get("connectorTestSuitesOptions", [])

    scenarios: list[ConnectorTestScenario] = []
    for suite in suites:
        if not isinstance(suite, dict) or suite.get("suite") != SMOKE_TEST_SUITE_NAME:
            continue

        for raw_scenario in suite.get("scenarios", []) or []:
            scenarios.append(
                _build_scenario(
                    raw_scenario=raw_scenario,
                    connector_root=connector_root,
                )
            )

    return scenarios


def _build_scenario(
    *,
    raw_scenario: dict[str, Any],
    connector_root: Path,
) -> ConnectorTestScenario:
    name = raw_scenario.get("name")
    if not name or not isinstance(name, str):
        raise SigilResolutionError("Smoke-test scenario is missing a string `name` field.")

    raw_settings = raw_scenario.get("configSettings", {}) or {}
    if not isinstance(raw_settings, dict):
        raise SigilResolutionError(
            f"Smoke-test scenario `{name}` has non-mapping `configSettings`."
        )

    env_cache: dict[str, dict[str, str]] = {}
    has_secret_ref = _contains_secret_ref(raw_settings)
    if has_secret_ref:
        _validate_provider_env()

    resolved_settings = _resolve_sigils(
        value=raw_settings,
        connector_root=connector_root,
        env_cache=env_cache,
    )

    scenario = ConnectorTestScenario.model_validate({"config_dict": resolved_settings})
    # `_id` is a Pydantic v2 private attribute, so it is not populated by
    # `model_validate` and the model is frozen. Bypass `__setattr__` once at
    # construction time so the scenario reports a stable, human-readable id.
    object.__setattr__(scenario, "_id", f"smoke-{name}")
    return scenario


def _resolve_sigils(
    *,
    value: Any,
    connector_root: Path,
    env_cache: dict[str, dict[str, str]],
) -> Any:
    """Recursively resolve `${...}` sigils inside `value`."""
    if isinstance(value, dict):
        return {
            k: _resolve_sigils(
                value=v,
                connector_root=connector_root,
                env_cache=env_cache,
            )
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [
            _resolve_sigils(
                value=item,
                connector_root=connector_root,
                env_cache=env_cache,
            )
            for item in value
        ]
    if isinstance(value, str):
        return _resolve_string(
            raw=value,
            connector_root=connector_root,
            env_cache=env_cache,
        )
    return value


def _resolve_string(
    *,
    raw: str,
    connector_root: Path,
    env_cache: dict[str, dict[str, str]],
) -> str:
    secret_match = _SECRET_REF_PATTERN.match(raw)
    if secret_match is not None:
        return _resolve_secret_ref(
            item=secret_match.group("item"),
            section=secret_match.group("section"),
            field=secret_match.group("field"),
            connector_root=connector_root,
            env_cache=env_cache,
        )

    date_match = _RELATIVE_DATE_PATTERN.match(raw)
    if date_match is not None:
        return _resolve_relative_date(duration_str=date_match.group("duration"))

    if raw.startswith("${") and raw.endswith("}"):
        raise SigilResolutionError(
            f"Unrecognized smoke-test sigil: `{raw}`. Supported forms are "
            "`${secret-ref:<item>/<section>/<field>}` and "
            "`${relative-date:-P<duration>}`."
        )

    return raw


def _resolve_secret_ref(
    *,
    item: str,
    section: str,
    field: str,
    connector_root: Path,
    env_cache: dict[str, dict[str, str]],
) -> str:
    """Resolve `${secret-ref:item/section/field}` against `secrets/.env.<section>`.

    The section segment is the literal 1Password section name (no prefix
    stripping). Env var names inside each `.env.<section>` file are flat:
    `<ITEM_UPPER>_<FIELD_UPPER>`.
    """
    env_path = connector_root / SECRETS_DIRNAME / f"{ENV_FILE_PREFIX}{section}"
    env_vars = _load_env_file(path=env_path, cache=env_cache)

    var_name = f"{item.upper()}_{field.upper()}"
    if var_name not in env_vars:
        raise SigilResolutionError(
            f"Env var `{var_name}` not found in `{env_path}` for sigil "
            f"`${{secret-ref:{item}/{section}/{field}}}`."
        )
    return env_vars[var_name]


def _resolve_relative_date(*, duration_str: str) -> str:
    """Render `${relative-date:-P<duration>}` as RFC 3339 at today midnight UTC."""
    duration = isodate.parse_duration(duration_str)
    now = datetime.now(tz=timezone.utc)
    today_midnight = datetime(
        year=now.year,
        month=now.month,
        day=now.day,
        tzinfo=timezone.utc,
    )
    rendered: datetime = today_midnight - duration
    if rendered.tzinfo is None:
        rendered = rendered.replace(tzinfo=timezone.utc)
    return rendered.strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_env_file(
    *,
    path: Path,
    cache: dict[str, dict[str, str]],
) -> dict[str, str]:
    """Parse a `.env.<section>` file into a name->value mapping.

    Supports `KEY=value` and `KEY="value"` lines. Comments (`#`) and blank
    lines are ignored. The result is cached per `path` so that scenarios with
    multiple sigils against the same section pay the parse cost once.
    """
    cache_key = str(path)
    if cache_key in cache:
        return cache[cache_key]

    if not path.exists():
        raise SigilResolutionError(
            f"Env file `{path}` not found. Run `airbyte-ops secrets fetch "
            "<connector>` to populate it before invoking the test suite."
        )

    parsed: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        parsed[key] = value

    cache[cache_key] = parsed
    return parsed


def _contains_secret_ref(value: Any) -> bool:
    """Walk `value` and return True if any string contains a `secret-ref` sigil."""
    if isinstance(value, dict):
        return any(_contains_secret_ref(v) for v in value.values())
    if isinstance(value, list):
        return any(_contains_secret_ref(item) for item in value)
    if isinstance(value, str):
        return _SECRET_REF_PATTERN.match(value) is not None
    return False


def _validate_provider_env() -> None:
    """Fail fast if the secret-provider env var contract is unmet."""
    provider = os.environ.get("AIRBYTE_TEST_CREDS_PROVIDER", "")
    vault = os.environ.get("AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME", "")
    if provider != "1pass":
        raise SigilResolutionError(
            "Smoke-test scenario uses `${secret-ref:...}` sigils but "
            "`AIRBYTE_TEST_CREDS_PROVIDER` is not set to `1pass` "
            "(only supported provider today)."
        )
    if not vault:
        raise SigilResolutionError(
            "Smoke-test scenario uses `${secret-ref:...}` sigils but "
            "`AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME` is not set."
        )
