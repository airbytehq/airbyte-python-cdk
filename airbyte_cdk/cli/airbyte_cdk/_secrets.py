# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Secret management commands.

This module provides commands for managing secrets for Airbyte connectors.

Usage:
    airbyte-cdk secrets fetch --connector-name source-github
    airbyte-cdk secrets fetch --connector-directory /path/to/connector
    airbyte-cdk secrets fetch  # Run from within a connector directory

Usage without pre-installing (stateless):
    pipx run airbyte-cdk secrets fetch ...
    uvx airbyte-cdk secrets fetch ...

The 'fetch' command retrieves secrets from Google Secret Manager based on connector
labels and writes them to the connector's `secrets` directory.
"""

import json
import os
from pathlib import Path

import rich_click as click

from airbyte_cdk.cli.airbyte_cdk._util import resolve_connector_name_and_directory

AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
CONNECTOR_LABEL = "connector"


@click.group(
    name="secrets",
    help=__doc__.replace("\n", "\n\n"),  # Render docstring as help text (markdown)
)
def secrets_cli_group() -> None:
    """Secret management commands."""
    pass


@secrets_cli_group.command()
@click.option(
    "--connector-name",
    type=str,
    help="Name of the connector to fetch secrets for. Ignored if --connector-directory is provided.",
)
@click.option(
    "--connector-directory",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Path to the connector directory.",
)
@click.option(
    "--gcp-project-id",
    type=str,
    default=AIRBYTE_INTERNAL_GCP_PROJECT,
    help=f"GCP project ID. Defaults to '{AIRBYTE_INTERNAL_GCP_PROJECT}'.",
)
def fetch(
    connector_name: str | None = None,
    connector_directory: Path | None = None,
    gcp_project_id: str = AIRBYTE_INTERNAL_GCP_PROJECT,
) -> None:
    """Fetch secrets for a connector from Google Secret Manager.

    This command fetches secrets for a connector from Google Secret Manager and writes them
    to the connector's secrets directory.

    If no connector name or directory is provided, we will look within the current working
    directory. If the current working directory is not a connector directory (e.g. starting
    with 'source-') and no connector name or path is provided, the process will fail.
    """
    try:
        from google.cloud import secretmanager_v1 as secretmanager
    except ImportError:
        raise ImportError(
            "google-cloud-secret-manager package is required for Secret Manager integration. "
            "Install it with 'pip install airbyte-cdk[dev]' "
            "or 'pip install google-cloud-secret-manager'."
        )

    click.echo("Fetching secrets...")

    # Resolve connector name/directory
    try:
        connector_name, connector_directory = resolve_connector_name_and_directory(
            connector_name=connector_name,
            connector_directory=connector_directory,
        )
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Could not find connector directory for '{connector_name}'. "
            "Please provide the --connector-directory option with the path to the connector. "
            "Note: This command requires either running from within a connector directory, "
            "being in the airbyte monorepo, or explicitly providing the connector directory path."
        ) from e
    except ValueError as e:
        raise ValueError(str(e))

    # Create secrets directory if it doesn't exist
    secrets_dir = connector_directory / "secrets"
    secrets_dir.mkdir(parents=True, exist_ok=True)

    gitignore_path = secrets_dir / ".gitignore"
    gitignore_path.write_text("*")

    # Get GSM client
    credentials_json = os.environ.get("GCP_GSM_CREDENTIALS")
    if not credentials_json:
        raise ValueError(
            "No Google Cloud credentials found. Please set the GCP_GSM_CREDENTIALS environment variable."
        )

    client = secretmanager.SecretManagerServiceClient.from_service_account_info(
        json.loads(credentials_json)
    )

    # List all secrets with the connector label
    parent = f"projects/{gcp_project_id}"
    filter_string = f"labels.{CONNECTOR_LABEL}={connector_name}"
    secrets = client.list_secrets(
        request=secretmanager.ListSecretsRequest(
            parent=parent,
            filter=filter_string,
        )
    )

    # Fetch and write secrets
    secret_count = 0
    for secret in secrets:
        secret_name = secret.name
        version_name = f"{secret_name}/versions/latest"
        response = client.access_secret_version(name=version_name)
        payload = response.payload.data.decode("UTF-8")

        filename_base = "config"  # Default filename
        if secret.labels and "filename" in secret.labels:
            filename_base = secret.labels["filename"]

        secret_file_path = secrets_dir / f"{filename_base}.json"
        secret_file_path.write_text(payload)
        secret_file_path.chmod(0o600)  # default to owner read/write only
        click.echo(f"Secret written to: {secret_file_path.absolute()!s}")
        secret_count += 1

    if secret_count == 0:
        click.echo(f"No secrets found for connector: {connector_name}")


__all__ = [
    "secrets_cli_group",
]
