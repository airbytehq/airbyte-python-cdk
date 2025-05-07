"""Common utilities for Airbyte CDK CLI."""

from pathlib import Path

from airbyte_cdk.test.standard_tests.test_resources import find_connector_root_from_name


def resolve_connector_name_and_directory(
    connector_name: str | None = None,
    connector_directory: Path | None = None,
) -> tuple[str, Path]:
    """Resolve the connector name and directory.

    This function will resolve the connector name and directory based on the provided
    arguments. If no connector name or directory is provided, it will look within the
    current working directory. If the current working directory is not a connector
    directory (e.g. starting with 'source-') and no connector name or path is provided,
    the process will fail.
    """
    if not connector_directory:
        if connector_name:
            connector_directory = find_connector_root_from_name(connector_name)
        else:
            cwd = Path().resolve().absolute()
            if cwd.name.startswith("source-") or cwd.name.startswith("destination-"):
                connector_directory = cwd
            else:
                raise ValueError(
                    "Either connector_name or connector_directory must be provided if not "
                    "running from a connector directory."
                )

    if not connector_name:
        connector_name = connector_directory.name

    if connector_directory:
        connector_directory = connector_directory.resolve().absolute()
    elif connector_name:
        connector_directory = find_connector_root_from_name(connector_name)
    else:
        raise ValueError("Either connector_name or connector_directory must be provided.")

    return connector_name, connector_directory


def resolve_connector_name(
    connector_directory: Path,
) -> str:
    """Resolve the connector name.

    This function will resolve the connector name based on the provided connector directory.
    If the current working directory is not a connector directory
    (e.g. starting with 'source-'), the process will fail.

    Raises:
        FileNotFoundError: If the connector directory does not exist or cannot be found.
    """
    if not connector_directory:
        raise FileNotFoundError(
            "Connector directory does not exist or cannot be found. Please provide a valid "
            "connector directory."
        )
    connector_name = connector_directory.absolute().name
    if not connector_name.startswith("source-") and not connector_name.startswith("destination-"):
        raise ValueError(
            f"Connector directory '{connector_name}' does not look like a valid connector directory. "
            f"Full path: {connector_directory.absolute()}"
        )
    return connector_name
