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

The command retrieves secrets from Google Secret Manager based on connector
labels and writes them to the connector's `secrets` directory.
"""

from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

import requests
import rich_click as click
import yaml
from click import style
from rich.console import Console
from rich.table import Table

from airbyte_cdk.cli.airbyte_cdk._util import (
    resolve_connector_name,
    resolve_connector_name_and_directory,
)

AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
CONNECTOR_LABEL = "connector"
GLOBAL_MASK_KEYS_URL = "https://connectors.airbyte.com/files/registries/v0/specs_secrets_mask.yaml"

logger = logging.getLogger("airbyte-cdk.cli.secrets")

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
@click.option(
    "--print-ci-secrets-masks",
    help="Print GitHub CI mask for secrets.",
    type=bool,
    is_flag=True,
    default=False,
)
def fetch(
    connector_name: str | None = None,
    connector_directory: Path | None = None,
    gcp_project_id: str = AIRBYTE_INTERNAL_GCP_PROJECT,
    print_ci_secrets_masks: bool = False,
) -> None:
    """Fetch secrets for a connector from Google Secret Manager.

    This command fetches secrets for a connector from Google Secret Manager and writes them
    to the connector's secrets directory.

    If no connector name or directory is provided, we will look within the current working
    directory. If the current working directory is not a connector directory (e.g. starting
    with 'source-') and no connector name or path is provided, the process will fail.

    The `--print-ci-secrets-masks` option will print the GitHub CI mask for the secrets.
    This is useful for masking secrets in CI logs.

    WARNING: This action causes the secrets to be printed in clear text to `STDOUT`. For security
    reasons, this function will only execute if the `CI` environment variable is set. Otherwise,
    masks will not be printed.
    """
    click.echo("Fetching secrets...", err=True)

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
        click.echo(f"Secret written to: {secret_file_path.absolute()!s}", err=True)
        secret_count += 1

    if secret_count == 0:
        click.echo(
            f"No secrets found for connector: '{connector_name}'",
            err=True,
        )

    if not print_ci_secrets_masks:
        return

    if not os.environ.get("CI", None):
        click.echo(
            "The `--print-ci-secrets-masks` option is only available in CI environments. "
            "The `CI` env var is either not set or not set to a truthy value. "
            "Skipping printing secret masks.",
            err=True,
        )
        return

    # Else print the CI mask
    _print_ci_secrets_masks(
        secrets_dir=secrets_dir,
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
    click.echo("Scanning secrets...", err=True)

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
        # E.g. https://console.cloud.google.com/security/secret-manager/secret/SECRET_SOURCE-SHOPIFY__CREDS/versions?hl=en&project=<gcp_project_id>
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


def _print_ci_secrets_masks(
    secrets_dir: Path,
) -> None:
    """Print GitHub CI mask for secrets.

    https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/workflow-commands-for-github-actions#example-masking-an-environment-variable

    The env var `CI` is set to a truthy value in GitHub Actions, so we can use it to
    determine if we are in a CI environment. If not, we don't want to print the masks,
    as it will cause the secrets to be printed in clear text to STDOUT.
    """
    if not os.environ.get("CI", None):
        click.echo(
            "The `--print-ci-secrets-masks` option is only available in CI environments. "
            "The `CI` env var is either not set or not set to a truthy value. "
            "Skipping printing secret masks.",
            err=True,
        )
        return

    for secret_file_path in secrets_dir.glob("*.json"):
        config_dict = json.loads(secret_file_path.read_text())
        _print_ci_secrets_masks_for_config(config=config_dict)


def _print_ci_secrets_masks_for_config(
    config: dict[str, str] | list[Any] | Any,
) -> None:
    """Print GitHub CI mask for secrets config, navigating child nodes recursively."""
    if isinstance(config, list):
        for item in config:
            _print_ci_secrets_masks_for_config(item)

    if isinstance(config, dict):
        for key, value in config.items():
            if _is_secret_property(key):
                logger.debug(f"Masking secret for config key: {key}")
                print(f"::add-mask::{value!s}")
                if isinstance(value, dict):
                    # For nested dicts, we also need to mask the json-stringified version
                    print(f"::add-mask::{json.dumps(value)!s}")

            if isinstance(value, (dict, list)):
                _print_ci_secrets_masks_for_config(config=value)


def _is_secret_property(property_name: str) -> bool:
    """Check if the property name is in the list of properties to mask.

    To avoid false negatives, we perform a case-insensitive check, and we include any property name
    that contains a rule entry, even if it is not an exact match.

    For example, if the rule entry is "password", we will also match "PASSWORD" and "my_password".
    """
    names_to_mask: list[str] = _get_spec_mask()
    if any([mask.lower() in property_name.lower() for mask in names_to_mask]):
        return True

    return False


@lru_cache
def _get_spec_mask() -> list[str]:
    """Get the list of properties to mask from the spec mask file."""
    response = requests.get(GLOBAL_MASK_KEYS_URL, allow_redirects=True)
    if not response.ok:
        logger.error(f"Failed to fetch spec mask: {response.content.decode('utf-8')}")
    try:
        return cast(list[str], yaml.safe_load(response.content)["properties"])
    except Exception as e:
        logger.error(f"Failed to parse spec mask: {e}")
        raise
