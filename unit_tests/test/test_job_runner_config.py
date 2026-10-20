from pathlib import Path
from typing import Any

import pytest

from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models.scenario import ConnectorTestScenario
from airbyte_cdk.test.standard_tests._job_runner import run_test_job


def _run_test_job_args(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    scenario: ConnectorTestScenario,
    verb: str = "discover",
) -> list[str]:
    captured_args: list[str] = []

    def fake_run_command(
        *,
        source: Any,
        args: list[str],
        expected_outcome: Any,
    ) -> EntrypointOutput:
        captured_args.extend(args)
        return EntrypointOutput(messages=[], command=args)

    monkeypatch.setattr(entrypoint_wrapper, "_run_command", fake_run_command)
    run_test_job(
        lambda: object(),
        verb,  # type: ignore[arg-type]
        connector_root=tmp_path,
        test_scenario=scenario,
    )
    return captured_args


def test_config_path_with_empty_object_passes_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}")

    args = _run_test_job_args(
        monkeypatch,
        tmp_path,
        ConnectorTestScenario(config_path=config_path),
    )

    assert "--config" in args
    config_index = args.index("--config") + 1
    assert Path(args[config_index]).read_text() == "{}"


def test_config_dict_passes_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    args = _run_test_job_args(
        monkeypatch,
        tmp_path,
        ConnectorTestScenario(config_dict={"a": 1}),
    )

    assert "--config" in args


def test_missing_config_does_not_pass_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    args = _run_test_job_args(monkeypatch, tmp_path, ConnectorTestScenario())

    assert "--config" not in args


def test_spec_does_not_pass_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}")

    args = _run_test_job_args(
        monkeypatch,
        tmp_path,
        ConnectorTestScenario(config_path=config_path),
        verb="spec",
    )

    assert "--config" not in args
