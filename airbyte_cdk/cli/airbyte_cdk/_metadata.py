# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""CLI commands for metadata validation."""

import json
import sys
from pathlib import Path

import rich_click as click

from airbyte_cdk.models.connector_metadata import validate_metadata_file


@click.group(name="metadata")
def metadata_cli_group() -> None:
    """Commands for working with connector metadata."""
    pass


@metadata_cli_group.command(name="validate")
@click.option(
    "--file",
    "-f",
    "file_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to the metadata.yaml file to validate",
)
@click.option(
    "--schema",
    "-s",
    "schema_source",
    type=str,
    default=None,
    help="URL or file path to JSON schema (defaults to monorepo schema)",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "text"]),
    default="text",
    help="Output format (json or text)",
)
def validate_command(file_path: Path, schema_source: str | None, output_format: str) -> None:
    """Validate a connector metadata.yaml file.

    This command validates a metadata.yaml file against the connector metadata schema
    and reports any validation errors.

    Examples:
        airbyte-cdk metadata validate --file metadata.yaml
        airbyte-cdk metadata validate --file metadata.yaml --format json
        airbyte-cdk metadata validate --file metadata.yaml --schema /path/to/schema.json
    """
    result = validate_metadata_file(file_path, schema_source)

    if output_format == "json":
        click.echo(result.model_dump_json(indent=2))
    else:
        if result.valid:
            click.secho("✓ Metadata file is valid", fg="green")
        else:
            click.secho("✗ Metadata file is invalid", fg="red")
            click.echo()
            click.echo("Errors:")
            for error in result.errors:
                error_type = error.get("type", "unknown")
                path = error.get("path", "")
                message = error.get("message", "")

                if path:
                    click.echo(f"  • {path}: {message} (type: {error_type})")
                else:
                    click.echo(f"  • {message} (type: {error_type})")

    sys.exit(0 if result.valid else 1)
