# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for persisting configs rotated during standard tests."""

import json
from pathlib import Path

import orjson

from airbyte_cdk.models import (
    AirbyteControlConnectorConfigMessage,
    AirbyteControlMessage,
    AirbyteMessage,
    AirbyteMessageSerializer,
    OrchestratorType,
    Type,
)
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.models.scenario import (
    UPDATED_CONFIGURATIONS_DIRNAME,
    find_latest_updated_config_file,
)
from airbyte_cdk.test.standard_tests._updated_configs import (
    extract_config_updates,
    persist_config_updates,
    persist_updated_config,
)


def _control_message(config: dict, emitted_at: float) -> str:
    message = AirbyteMessage(
        type=Type.CONTROL,
        control=AirbyteControlMessage(
            type=OrchestratorType.CONNECTOR_CONFIG,
            emitted_at=emitted_at,
            connectorConfig=AirbyteControlConnectorConfigMessage(config=config),
        ),
    )
    return orjson.dumps(AirbyteMessageSerializer.dump(message)).decode()


def _record_message() -> str:
    return json.dumps(
        {
            "type": "RECORD",
            "record": {"stream": "s", "data": {"a": 1}, "emitted_at": 1},
        }
    )


def _write_scenario_config(connector_root: Path, name: str = "config.json") -> Path:
    secrets_dir = connector_root / "secrets"
    secrets_dir.mkdir(parents=True, exist_ok=True)
    config_path = secrets_dir / name
    config_path.write_text(json.dumps({"refresh_token": "original"}))
    return config_path


def test_find_latest_updated_config_file_prefers_highest_emitted_at(tmp_path: Path) -> None:
    config_path = _write_scenario_config(tmp_path)
    assert find_latest_updated_config_file(config_path) is None

    updated_dir = config_path.parent / UPDATED_CONFIGURATIONS_DIRNAME
    updated_dir.mkdir()
    (updated_dir / "config|2000.json").write_text("{}")
    (updated_dir / "config|1000.json").write_text("{}")
    (updated_dir / "config_oauth|9000.json").write_text("{}")  # different stem, ignored

    assert find_latest_updated_config_file(config_path) == updated_dir / "config|2000.json"


def test_scenario_get_config_dict_prefers_latest_update(tmp_path: Path) -> None:
    config_path = _write_scenario_config(tmp_path)
    scenario = ConnectorTestScenario(config_path=Path("secrets/config.json"))

    assert scenario.get_config_dict(connector_root=tmp_path, empty_if_missing=False) == {
        "refresh_token": "original"
    }

    updated_dir = config_path.parent / UPDATED_CONFIGURATIONS_DIRNAME
    updated_dir.mkdir()
    (updated_dir / "config|1700.json").write_text(json.dumps({"refresh_token": "rotated"}))

    assert scenario.get_effective_config_path(tmp_path) == updated_dir / "config|1700.json"
    assert scenario.get_config_dict(connector_root=tmp_path, empty_if_missing=False) == {
        "refresh_token": "rotated"
    }
    # Identity and creds detection still derive from the original path.
    assert scenario.id == "config"
    assert scenario.requires_creds


def test_extract_config_updates_only_returns_connector_config_controls() -> None:
    output = EntrypointOutput(
        messages=[
            _record_message(),
            _control_message({"refresh_token": "first"}, 1000.0),
            _control_message({"refresh_token": "second"}, 2000.0),
        ]
    )

    assert extract_config_updates(output) == [
        ({"refresh_token": "first"}, 1000),
        ({"refresh_token": "second"}, 2000),
    ]


def test_persist_config_updates_writes_each_new_config(tmp_path: Path) -> None:
    config_path = _write_scenario_config(tmp_path)
    scenario = ConnectorTestScenario(config_path=Path("secrets/config.json"))
    output = EntrypointOutput(
        messages=[
            _control_message({"refresh_token": "first"}, 1000.0),
            # Same as previous update: must not be persisted twice.
            _control_message({"refresh_token": "first"}, 1500.0),
            _control_message({"refresh_token": "second"}, 2000.0),
        ]
    )

    persisted = persist_config_updates(output, scenario=scenario, connector_root=tmp_path)

    updated_dir = config_path.parent / UPDATED_CONFIGURATIONS_DIRNAME
    assert persisted == [updated_dir / "config|1000.json", updated_dir / "config|2000.json"]
    assert json.loads((updated_dir / "config|2000.json").read_text()) == {"refresh_token": "second"}
    assert (updated_dir / "config|2000.json").stat().st_mode & 0o777 == 0o600
    # The original secret file is left untouched.
    assert json.loads(config_path.read_text()) == {"refresh_token": "original"}


def test_persist_config_updates_skips_config_equal_to_original(tmp_path: Path) -> None:
    config_path = _write_scenario_config(tmp_path)
    scenario = ConnectorTestScenario(config_path=Path("secrets/config.json"))
    output = EntrypointOutput(messages=[_control_message({"refresh_token": "original"}, 1000.0)])

    assert persist_config_updates(output, scenario=scenario, connector_root=tmp_path) == []
    assert not (config_path.parent / UPDATED_CONFIGURATIONS_DIRNAME).exists()


def test_persist_config_updates_handles_in_place_updates(tmp_path: Path) -> None:
    config_path = _write_scenario_config(tmp_path)
    scenario = ConnectorTestScenario(config_path=Path("secrets/config.json"))
    output = EntrypointOutput(messages=[_record_message()])

    persisted = persist_config_updates(
        output,
        scenario=scenario,
        connector_root=tmp_path,
        in_place_updates=[{"refresh_token": "rewritten"}],
    )

    assert len(persisted) == 1
    assert persisted[0].parent == config_path.parent / UPDATED_CONFIGURATIONS_DIRNAME
    assert persisted[0].name.startswith("config|")
    assert json.loads(persisted[0].read_text()) == {"refresh_token": "rewritten"}


def test_persist_config_updates_is_noop_without_config_path(tmp_path: Path) -> None:
    output = EntrypointOutput(messages=[_control_message({"refresh_token": "x"}, 1000.0)])

    assert persist_config_updates(output, scenario=None, connector_root=tmp_path) == []
    assert (
        persist_config_updates(
            output,
            scenario=ConnectorTestScenario(config_dict={"refresh_token": "inline"}),
            connector_root=tmp_path,
        )
        == []
    )


def test_persist_updated_config_compares_against_latest_update(tmp_path: Path) -> None:
    config_path = _write_scenario_config(tmp_path)
    first = persist_updated_config({"refresh_token": "a"}, config_path=config_path, emitted_at=1000)
    assert first is not None

    # Equal to the latest persisted update, not the original: nothing to write.
    assert (
        persist_updated_config({"refresh_token": "a"}, config_path=config_path, emitted_at=2000)
        is None
    )
    # Differs from the latest update: written even though it matches nothing else.
    assert (
        persist_updated_config({"refresh_token": "b"}, config_path=config_path, emitted_at=3000)
        is not None
    )
