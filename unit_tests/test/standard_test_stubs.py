# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Stubs shared by the standard-test unit tests: scenarios, suite classes and a fake Docker runner."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Callable, TypeVar, cast

import pytest
import yaml

from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models.scenario import ConnectorTestScenario
from airbyte_cdk.test.standard_tests._job_runner import IConnector
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG

POKEAPI_RESOURCE_DIR = Path(__file__).parent.parent / "resources" / "source_pokeapi_w_components_py"

# Long enough for the `minLength: 20` API key of the specs the tests use.
VALID_CONFIG = {"api_key": "x" * 20}


class Reached(Exception):
    """Raised by a fake connector or Docker command to prove a test body ran past its skips."""


_SuiteT = TypeVar("_SuiteT", bound=DockerConnectorTestSuite)


def run_without_skip(test: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call a standard test method that must run, not skip.

    A unit test that calls a standard test method directly would otherwise be skipped too when the
    method skips, and a skip passes in CI.
    """
    try:
        return test(*args, **kwargs)
    except pytest.skip.Exception as skip:
        pytest.fail(f"`{test.__name__}` was skipped instead of run: {skip}")


def suite_class(
    tmp_path: Path,
    acceptance_tests: dict[str, Any],
    *,
    base: type[_SuiteT],
    make_connector: Callable[[], IConnector] | None = None,
) -> type[_SuiteT]:
    """Build a suite class over a temporary connector dir holding `acceptance_tests`.

    `create_connector` returns `make_connector()`, or raises `Reached` when none is given.
    """
    connector_root = tmp_path / "source-test"
    connector_root.mkdir(exist_ok=True)
    (connector_root / ACCEPTANCE_TEST_CONFIG).write_text(
        yaml.safe_dump({"acceptance_tests": acceptance_tests})
    )
    shutil.copy(POKEAPI_RESOURCE_DIR / "metadata.yaml", connector_root / "metadata.yaml")

    def get_connector_root_dir(cls: type) -> Path:
        return connector_root

    def create_connector(self: Any, scenario: ConnectorTestScenario | None) -> IConnector:
        if make_connector is None:
            raise Reached(scenario.id if scenario else None)
        return make_connector()

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


def scenario(
    *sections: str,
    config: dict[str, Any] | None = None,
    name: str = "config",
    **extra: Any,
) -> ConnectorTestScenario:
    """A scenario over an inline config, listed under `sections` (none: built by hand).

    The config defaults to `VALID_CONFIG`: an empty config dict counts as no config at all.
    """
    return ConnectorTestScenario(
        config_path=Path(f"secrets/{name}.json"),
        config_dict=VALID_CONFIG if config is None else config,
        sections=tuple(sections),
        **extra,
    )


def entry(config_path: str, **extra: Any) -> dict[str, Any]:
    return {"config_path": config_path, **extra}


def stream_json(
    name: str, sync_modes: list[str], cursor: list[str] | None = None
) -> dict[str, Any]:
    stream: dict[str, Any] = {
        "name": name,
        "json_schema": {"type": "object"},
        "supported_sync_modes": sync_modes,
    }
    if cursor:
        stream |= {"source_defined_cursor": True, "default_cursor_field": cursor}
    return stream


def catalog_message(*streams: dict[str, Any]) -> str:
    return json.dumps({"type": "CATALOG", "catalog": {"streams": list(streams)}})


def record_message(stream: str) -> str:
    return json.dumps(
        {"type": "RECORD", "record": {"stream": stream, "data": {"id": 1}, "emitted_at": 0}}
    )


def state_message(stream: str) -> str:
    """The final per-stream state, with the `sourceStats` the CDK entrypoint adds."""
    return json.dumps(
        {
            "type": "STATE",
            "state": {
                "type": "STREAM",
                "stream": {
                    "stream_descriptor": {"name": stream},
                    "stream_state": {"__ab_no_cursor_state_message": True},
                },
                "sourceStats": {"recordCount": 1.0},
            },
        }
    )


def stub_docker(
    monkeypatch: pytest.MonkeyPatch,
    responses: dict[str, list[str]],
    read_catalogs: list[dict[str, Any]] | None = None,
) -> list[list[str]]:
    """Answer each connector command run "in Docker" with canned messages; record the commands.

    The configured catalog mounted into each `read` is appended to `read_catalogs`.
    """
    calls: list[list[str]] = []

    def _fake_run_docker_airbyte_command(
        cmd: list[str], *, raise_if_errors: bool = False
    ) -> EntrypointOutput:
        calls.append(cmd)
        command = next(part for part in cmd if part in ("spec", "check", "discover", "read"))
        if command == "read" and read_catalogs is not None:
            mount = next(part for part in cmd if part.endswith(":/secrets/catalog.json"))
            read_catalogs.append(json.loads(Path(mount.rsplit(":", 1)[0]).read_text()))
        result = EntrypointOutput(messages=responses[command], command=cmd)
        if raise_if_errors:
            result.raise_if_errors()
        return result

    monkeypatch.setattr(
        "airbyte_cdk.test.standard_tests.docker_base.run_docker_airbyte_command",
        _fake_run_docker_airbyte_command,
    )
    return calls
