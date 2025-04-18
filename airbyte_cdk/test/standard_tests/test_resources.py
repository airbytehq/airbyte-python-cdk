# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Resources for Airbyte CDK tests."""

from contextlib import suppress
from pathlib import Path

ACCEPTANCE_TEST_CONFIG = "acceptance-test-config.yml"
MANIFEST_YAML = "manifest.yaml"
METADATA_YAML = "metadata.yaml"


def find_connector_root(from_paths: list[Path]) -> Path:
    """Find the root directory of the connector."""
    for path in from_paths:
        # If we reach here, we didn't find the manifest file in any parent directory
        # Check if the manifest file exists in the current directory
        for parent in [path, *path.parents]:
            if (parent / METADATA_YAML).exists():
                return parent
            if (parent / MANIFEST_YAML).exists():
                return parent
            if (parent / ACCEPTANCE_TEST_CONFIG).exists():
                return parent
            if parent.name == "airbyte_cdk":
                break

    raise FileNotFoundError(
        "Could not find connector root directory relative to the provided directories: "
        f"'{str(from_paths)}'."
    )


def find_connector_root_from_name(connector_name: str) -> Path:
    """Find the root directory of the connector from its name."""
    with suppress(FileNotFoundError):
        return find_connector_root([Path(connector_name)])

    # If the connector name is not found, check if we are in the airbyte monorepo
    # and try to find the connector root from the current directory.

    cwd: Path = Path().absolute()

    if "airbyte" not in cwd.parts:
        raise FileNotFoundError(
            "Could not find connector root directory relative and we are not in the airbyte repo. "
            f"Current directory: {cwd} "
        )

    # Find the connector root from the current directory

    airbyte_repo_root: Path
    for parent in [cwd, *cwd.parents]:
        if parent.name == "airbyte":
            airbyte_repo_root = parent
            break
    else:
        raise FileNotFoundError(
            "Could not find connector root directory relative and we are not in the airbyte repo."
        )

    expected_connector_dir: Path = (
        airbyte_repo_root / "airbyte-integrations" / "connectors" / connector_name
    )
    if not expected_connector_dir.exists():
        raise FileNotFoundError(
            f"Could not find connector directory '{expected_connector_dir}' relative to the airbyte repo."
        )

    return expected_connector_dir
