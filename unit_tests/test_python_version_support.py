# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

import tomllib
from pathlib import Path


def test_python_version_floor() -> None:
    with (Path(__file__).resolve().parents[1] / "pyproject.toml").open("rb") as project_file:
        project = tomllib.load(project_file)

    assert project["tool"]["poetry"]["dependencies"]["python"] == ">=3.11,<3.15"


def test_pillow_has_no_python_version_split() -> None:
    with (Path(__file__).resolve().parents[1] / "pyproject.toml").open("rb") as project_file:
        project = tomllib.load(project_file)

    pillow = project["tool"]["poetry"]["dependencies"]["pillow"]
    assert isinstance(pillow, dict)
    assert pillow["version"] == ">=13.0.0"
    assert pillow["optional"] is True
    assert "python" not in pillow
    assert "markers" not in pillow
