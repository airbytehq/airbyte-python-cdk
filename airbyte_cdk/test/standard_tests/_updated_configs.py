# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Persist connector configs that were rotated while a standard test was running.

Connectors that rotate credentials during a run (for example, single-use OAuth
refresh tokens) report the new config to the platform as a
`CONTROL`/`CONNECTOR_CONFIG` message. In production the platform stores that
config; in tests nothing would, and the next run would start from a stale,
already-consumed credential.

This module captures those messages from an `EntrypointOutput` and writes each
new config to `<config dir>/updated_configurations/<stem>|<emitted_at_ms>.json`
next to the scenario's config file. Two consumers rely on that location:

- `ConnectorTestScenario.get_config_dict` prefers the newest update, so later
  tests in the same session use the rotated credential.
- CI runs `airbyte-ops secrets push` after the tests to upload the files back
  to Google Secret Manager as new secret versions.

Persistence never fails a test: errors are logged and swallowed.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airbyte_cdk.models import OrchestratorType, Type
from airbyte_cdk.test.models.scenario import (
    UPDATED_CONFIGURATIONS_DIRNAME,
    find_latest_updated_config_file,
)

if TYPE_CHECKING:
    from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
    from airbyte_cdk.test.models import ConnectorTestScenario

logger = logging.getLogger("airbyte")


def extract_config_updates(output: EntrypointOutput) -> list[tuple[dict[str, Any], int]]:
    """Return `(config, emitted_at_ms)` for each CONNECTOR_CONFIG control message, in order."""
    updates: list[tuple[dict[str, Any], int]] = []
    for message in output.get_message_by_types([Type.CONTROL], safe_iterator=True):
        control = message.control
        if (
            control is None
            or control.type != OrchestratorType.CONNECTOR_CONFIG
            or control.connectorConfig is None
        ):
            continue
        updates.append((dict(control.connectorConfig.config), int(control.emitted_at)))
    return updates


def persist_updated_config(
    new_config: dict[str, Any],
    *,
    config_path: Path,
    emitted_at: int,
) -> Path | None:
    """Write `new_config` as an update to `config_path` if it differs from the current one.

    "Current" is the newest already-persisted update, or the original file when
    there is none. Returns the written path, or `None` when nothing changed.
    """
    current_path = find_latest_updated_config_file(config_path) or config_path
    current_config: Any = None
    if current_path.is_file():
        try:
            current_config = json.loads(current_path.read_text())
        except json.JSONDecodeError:
            current_config = None

    if new_config == current_config:
        return None

    updated_dir = config_path.parent / UPDATED_CONFIGURATIONS_DIRNAME
    updated_dir.mkdir(parents=True, exist_ok=True)
    target = updated_dir / f"{config_path.stem}|{emitted_at}{config_path.suffix}"
    target.write_text(json.dumps(new_config))
    target.chmod(0o600)
    logger.info("Persisted updated connector config to %s", target)
    return target


def persist_config_updates(
    output: EntrypointOutput,
    *,
    scenario: ConnectorTestScenario | None,
    connector_root: Path,
    in_place_updates: list[dict[str, Any]] | None = None,
) -> list[Path]:
    """Persist every config rotation observed during a test job.

    Collects CONNECTOR_CONFIG control messages from `output`, plus any
    `in_place_updates` (configs a containerized connector wrote back into its
    mounted config file instead of, or in addition to, emitting a message), and
    writes each one that differs from the current config via
    `persist_updated_config`.

    Does nothing when the scenario has no `config_path` (inline `config_dict`
    scenarios have no file to update). Never raises.
    """
    if scenario is None:
        return []
    config_path = scenario.resolve_config_path(connector_root)
    if config_path is None:
        return []

    persisted: list[Path] = []
    try:
        updates = extract_config_updates(output)
        for in_place_update in in_place_updates or []:
            updates.append((in_place_update, int(time.time() * 1000)))

        for new_config, emitted_at in updates:
            written = persist_updated_config(
                new_config,
                config_path=config_path,
                emitted_at=emitted_at,
            )
            if written is not None:
                persisted.append(written)
    except Exception:
        logger.warning(
            "Failed to persist updated connector config for %s",
            config_path,
            exc_info=True,
        )

    return persisted
