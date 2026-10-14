# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

import json
import subprocess
import sys
from pathlib import Path

SCRIPT = """
import json
import sys
from unittest.mock import patch

import requests_mock
import yaml

from airbyte_cdk.cli.source_declarative_manifest import _run

manifest_path, config_path = sys.argv[1:]
with open(manifest_path) as manifest_file:
    manifest = yaml.safe_load(manifest_file)

with patch(
    "airbyte_cdk.cli.source_declarative_manifest._run._is_local_manifest_command",
    return_value=True,
):
    with patch(
        "airbyte_cdk.cli.source_declarative_manifest._run.YamlDeclarativeSource._read_and_parse_yaml_file",
        return_value=manifest,
    ):
        with requests_mock.Mocker() as mocker:
            mocker.get(
                "https://pokeapi.co/api/v2/pokemon/blastoise",
                json={"name": "blastoise"},
            )
            _run.handle_command(["spec"])
            _run.handle_command(["check", "--config", config_path])
            _run.handle_command(["discover", "--config", config_path])

print("LOADED_HEAVY_MODULES=" + json.dumps(sorted(m for m in ("pandas", "numpy") if m in sys.modules)))
"""


def test_spec_check_discover_do_not_import_pandas_or_numpy() -> None:
    manifest_path = Path(__file__).parent.parent / "resources" / "valid_local_manifest.yaml"
    config_path = Path(__file__).parent.parent / "resources" / "valid_local_pokeapi_config.json"

    result = subprocess.run(
        [sys.executable, "-c", SCRIPT, str(manifest_path), str(config_path)],
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stderr
    assert '"connectionStatus":{"status":"SUCCEEDED"}' in result.stdout
    assert '"catalog"' in result.stdout
    marker = next(
        line for line in result.stdout.splitlines() if line.startswith("LOADED_HEAVY_MODULES=")
    )
    assert json.loads(marker.split("=", 1)[1]) == []
