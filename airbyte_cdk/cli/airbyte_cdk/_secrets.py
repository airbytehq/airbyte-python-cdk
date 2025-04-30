# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""**Secret management commands.**

This module provides commands for managing secrets for Airbyte connectors.

**Usage:**

```bash
# Fetch secrets
airbyte-cdk secrets fetch --connector-name source-github
airbyte-cdk secrets fetch --connector-directory /path/to/connector
airbyte-cdk secrets fetch  # Run from within a connector directory

# List secrets (without fetching)
airbyte-cdk secrets list --connector-name source-github
airbyte-cdk secrets list --connector-directory /path/to/connector
```

**Usage without pre-installing (stateless):**

```bash
pipx run airbyte-cdk secrets fetch ...
uvx airbyte-cdk secrets fetch ...
```

The 'fetch' command retrieves secrets from Google Secret Manager based on connector
labels and writes them to the connector's `secrets` directory.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import cast

import rich_click as click
from click import style
from rich.console import Console
from rich.table import Table

from airbyte_cdk.cli.airbyte_cdk._util import (
    resolve_connector_name,
    resolve_connector_name_and_directory,
)

AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
CONNECTOR_LABEL = "connector"


try:
    from google.cloud import secretmanager_v1 as secretmanager
    from google.cloud.secretmanager_v1 import Secret
except ImportError:
    # If the package is not installed, we will raise an error in the CLI command.
    secretmanager = None  # type: ignore
    Secret = None  # type: ignore


@click.group(
    name="secrets",
    help=__doc__.replace("\n", "\n\n"),  # Render docstring as help text (markdown) # type: ignore
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
    click.echo("Fetching secrets...")

    client = _get_gsm_secrets_client()
    connector_name, connector_directory = resolve_connector_name_and_directory(
        connector_name=connector_name,
        connector_directory=connector_directory,
    )
    secrets_dir = _get_secrets_dir(
        connector_directory=connector_directory,
        connector_name=connector_name,
        ensure_exists=True,
    )
    secrets = _fetch_secret_handles(
        connector_name=connector_name,
        gcp_project_id=gcp_project_id,
    )
    # Fetch and write secrets
    secret_count = 0
    for secret in secrets:
        secret_file_path = _get_secret_filepath(
            secrets_dir=secrets_dir,
            secret=secret,
        )
        _write_secret_file(
            secret=secret,
            client=client,
            file_path=secret_file_path,
        )
        click.echo(f"Secret written to: {secret_file_path.absolute()!s}")
        secret_count += 1

    if secret_count == 0:
        click.echo(
            f"No secrets found for connector: '{connector_name}'",
            err=True,
        )


@secrets_cli_group.command("list")
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
def list_(
    connector_name: str | None = None,
    connector_directory: Path | None = None,
    gcp_project_id: str = AIRBYTE_INTERNAL_GCP_PROJECT,
) -> None:
    """List secrets for a connector from Google Secret Manager.

    This command fetches secrets for a connector from Google Secret Manager and prints
    them as a table.

    If no connector name or directory is provided, we will look within the current working
    directory. If the current working directory is not a connector directory (e.g. starting
    with 'source-') and no connector name or path is provided, the process will fail.
    """
    click.echo("Fetching secrets...")

    connector_name = connector_name or resolve_connector_name(
        connector_directory=connector_directory or Path().resolve().absolute(),
    )
    secrets: list[Secret] = _fetch_secret_handles(  # type: ignore
        connector_name=connector_name,
        gcp_project_id=gcp_project_id,
    )

    if not secrets:
        click.echo(
            f"No secrets found for connector: '{connector_name}'",
            err=True,
        )
        return
    # print a rich table with the secrets
    click.echo(
        style(
            f"Secrets for connector '{connector_name}' in project '{gcp_project_id}':",
            fg="green",
        )
    )

    console = Console()
    table = Table(title=f"'{connector_name}' Secrets")
    table.add_column("Name", justify="left", style="cyan", overflow="fold")
    table.add_column("Labels", justify="left", style="magenta", overflow="fold")
    table.add_column("Created", justify="left", style="blue", overflow="fold")
    for secret in secrets:
        full_secret_name = secret.name
        secret_name = full_secret_name.split("/secrets/")[-1]  # Removes project prefix
        # E.g. https://console.cloud.google.com/security/secret-manager/secret/SECRET_SOURCE-SHOPIFY__CREDS/versions?hl=en&project=dataline-integration-testing
        secret_url = f"https://console.cloud.google.com/security/secret-manager/secret/{secret_name}/versions?hl=en&project={gcp_project_id}"
        table.add_row(
            f"[link={secret_url}]{secret_name}[/link]",
            "\n".join([f"{k}={v}" for k, v in secret.labels.items()]),
            str(secret.create_time),
        )

    console.print(table)


def _fetch_secret_handles(
    connector_name: str,
    gcp_project_id: str = AIRBYTE_INTERNAL_GCP_PROJECT,
) -> list["Secret"]:  # type: ignore
    """Fetch secrets from Google Secret Manager."""
    if not secretmanager:
        raise ImportError(
            "google-cloud-secret-manager package is required for Secret Manager integration. "
            "Install it with 'pip install airbyte-cdk[dev]' "
            "or 'pip install google-cloud-secret-manager'."
        )

    client = _get_gsm_secrets_client()

    # List all secrets with the connector label
    parent = f"projects/{gcp_project_id}"
    filter_string = f"labels.{CONNECTOR_LABEL}={connector_name}"
    secrets = client.list_secrets(
        request=secretmanager.ListSecretsRequest(
            parent=parent,
            filter=filter_string,
        )
    )
    return [s for s in secrets]


def _write_secret_file(
    secret: "Secret",  # type: ignore
    client: "secretmanager.SecretManagerServiceClient",  # type: ignore
    file_path: Path,
) -> None:
    version_name = f"{secret.name}/versions/latest"
    response = client.access_secret_version(name=version_name)
    file_path.write_text(response.payload.data.decode("UTF-8"))
    file_path.chmod(0o600)  # default to owner read/write only


def _get_secrets_dir(
    connector_directory: Path,
    connector_name: str,
    ensure_exists: bool = True,
) -> Path:
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

    secrets_dir = connector_directory / "secrets"
    if ensure_exists:
        secrets_dir.mkdir(parents=True, exist_ok=True)

        gitignore_path = secrets_dir / ".gitignore"
        if not gitignore_path.exists():
            gitignore_path.write_text("*")

    return secrets_dir


def _get_secret_filepath(
    secrets_dir: Path,
    secret: Secret,  # type: ignore
) -> Path:
    """Get the file path for a secret based on its labels."""
    if secret.labels and "filename" in secret.labels:
        return secrets_dir / f"{secret.labels['filename']}.json"

    return secrets_dir / "config.json"  # Default filename


def _get_gsm_secrets_client() -> "secretmanager.SecretManagerServiceClient":  # type: ignore
    """Get the Google Secret Manager client."""
    if not secretmanager:
        raise ImportError(
            "google-cloud-secret-manager package is required for Secret Manager integration. "
            "Install it with 'pip install airbyte-cdk[dev]' "
            "or 'pip install google-cloud-secret-manager'."
        )

    credentials_json = os.environ.get("GCP_GSM_CREDENTIALS")
    if not credentials_json:
        raise ValueError(
            "No Google Cloud credentials found. "
            "Please set the `GCP_GSM_CREDENTIALS` environment variable."
        )

    return cast(
        "secretmanager.SecretManagerServiceClient",
        secretmanager.SecretManagerServiceClient.from_service_account_info(
            json.loads(credentials_json)
        ),
    )
