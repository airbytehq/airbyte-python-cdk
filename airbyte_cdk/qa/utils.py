"""Utilities for QA checks."""

from pathlib import Path
from typing import List

from airbyte_cdk.qa.connector import Connector


def get_all_connectors_in_directory(directory: Path) -> List[Connector]:
    """Get all connectors in a directory.

    Args:
        directory: The directory to search for connectors

    Returns:
        List[Connector]: The connectors found in the directory
    """
    connectors = []
    for path in directory.iterdir():
        if path.is_dir() and (
            path.name.startswith("source-") or path.name.startswith("destination-")
        ):
            connectors.append(Connector(path.name, path))
    return connectors


def remove_strict_encrypt_suffix(connector_name: str) -> str:
    """Remove the strict-encrypt suffix from a connector name.

    Args:
        connector_name: The connector name

    Returns:
        str: The connector name without the strict-encrypt suffix
    """
    if connector_name.endswith("-strict-encrypt"):
        return connector_name[:-14]
    return connector_name
