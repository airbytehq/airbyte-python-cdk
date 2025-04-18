# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Secret management commands."""

import json
import os
from pathlib import Path
from typing import Optional

import rich_click as click

from airbyte_cdk.test.standard_tests.test_resources import find_connector_root_from_name

AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
CONNECTOR_LABEL = "connector"


@click.group(name="secrets")
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
    "--project",
    type=str,
    default=AIRBYTE_INTERNAL_GCP_PROJECT,
    help=f"GCP project ID. Defaults to '{AIRBYTE_INTERNAL_GCP_PROJECT}'.",
)
def fetch(
    connector_name: Optional[str] = None,
    connector_directory: Optional[Path] = None,
    project: str = AIRBYTE_INTERNAL_GCP_PROJECT,
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
            "Install it with 'pip install google-cloud-secret-manager'."
        )

    click.echo("Fetching secrets...")

    # Resolve connector name/directory
    if not connector_name and not connector_directory:
        cwd = Path().resolve().absolute()
        if cwd.name.startswith("source-") or cwd.name.startswith("destination-"):
            connector_name = cwd.name
            connector_directory = cwd
        else:
            raise ValueError(
                "Either connector_name or connector_directory must be provided if not "
                "running from a connector directory."
            )

    if connector_directory:
        connector_directory = connector_directory.resolve().absolute()
        if not connector_name:
            connector_name = connector_directory.name
    elif connector_name:
        connector_directory = find_connector_root_from_name(connector_name)
    else:
        raise ValueError("Either connector_name or connector_directory must be provided.")

    # Create secrets directory if it doesn't exist
    secrets_dir = connector_directory / "secrets"
    secrets_dir.mkdir(parents=True, exist_ok=True)

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
    parent = f"projects/{project}"
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
        click.echo(f"Secret written to: {secret_file_path}")
        secret_count += 1

    if secret_count == 0:
        click.echo(f"No secrets found for connector: {connector_name}")


__all__ = [
    "secrets_cli_group",
]
