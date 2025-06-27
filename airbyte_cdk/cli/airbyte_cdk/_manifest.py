# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Manifest related commands.

This module provides a command line interface (CLI) for validating and migrating
Airbyte CDK manifests.
"""

import sys
from importlib import metadata
from pathlib import Path
from typing import Any, Dict

import rich_click as click
import yaml
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate

from airbyte_cdk.manifest_migrations.migration_handler import ManifestMigrationHandler
from airbyte_cdk.sources.declarative.manifest_declarative_source import (
    _get_declarative_component_schema,
)


@click.group(
    name="manifest",
    help=__doc__.replace("\n", "\n\n"),  # Render docstring as help text (markdown)
)
def manifest_cli_group() -> None:
    """Manifest related commands."""
    pass


@manifest_cli_group.command("validate")
@click.option(
    "--manifest-path",
    type=click.Path(exists=True, path_type=Path),
    default="manifest.yaml",
    help="Path to the manifest file to validate (default: manifest.yaml)",
)
def validate_manifest(manifest_path: Path) -> None:
    """Validate a manifest file against the declarative component schema.

    This command validates the manifest file and checks version compatibility.
    If validation fails, it will suggest running the migrate command if needed.
    """
    try:
        with open(manifest_path, "r") as f:
            manifest_dict = yaml.safe_load(f)

        if not isinstance(manifest_dict, dict):
            click.echo(
                f"❌ Error: Manifest file {manifest_path} does not contain a valid YAML dictionary",
                err=True,
            )
            sys.exit(1)

        schema = _get_declarative_component_schema()

        validate(manifest_dict, schema)

        migration_handler = ManifestMigrationHandler(manifest_dict)
        migrated_manifest = migration_handler.apply_migrations()

        if migrated_manifest != manifest_dict:
            click.echo(
                f"⚠️  Manifest {manifest_path} is valid but could benefit from migration to the latest version.",
                err=True,
            )
            click.echo(
                "Run 'airbyte-cdk manifest migrate' to apply available migrations.", err=True
            )
            sys.exit(1)

        click.echo(f"✅ Manifest {manifest_path} is valid and up to date.")

    except FileNotFoundError:
        click.echo(f"❌ Error: Manifest file {manifest_path} not found", err=True)
        sys.exit(1)
    except yaml.YAMLError as e:
        click.echo(f"❌ Error: Invalid YAML in {manifest_path}: {e}", err=True)
        sys.exit(1)
    except ValidationError as e:
        click.echo(f"❌ Validation failed for {manifest_path}:", err=True)
        click.echo(f"   {e.message}", err=True)
        click.echo(
            "Run 'airbyte-cdk manifest migrate' to apply available migrations that might fix this issue.",
            err=True,
        )
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Unexpected error validating {manifest_path}: {e}", err=True)
        sys.exit(1)


@manifest_cli_group.command("migrate")
@click.option(
    "--manifest-path",
    type=click.Path(exists=True, path_type=Path),
    default="manifest.yaml",
    help="Path to the manifest file to migrate (default: manifest.yaml)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what changes would be made without actually modifying the file",
)
def migrate_manifest(manifest_path: Path, dry_run: bool) -> None:
    """Apply migrations to make a manifest file compatible with the latest version.

    This command applies all necessary migrations to update the manifest file
    to be compatible with the latest CDK version.
    """
    try:
        with open(manifest_path, "r") as f:
            original_manifest = yaml.safe_load(f)

        if not isinstance(original_manifest, dict):
            click.echo(
                f"❌ Error: Manifest file {manifest_path} does not contain a valid YAML dictionary",
                err=True,
            )
            sys.exit(1)

        migration_handler = ManifestMigrationHandler(original_manifest)
        migrated_manifest = migration_handler.apply_migrations()

        if migrated_manifest == original_manifest:
            click.echo(f"✅ Manifest {manifest_path} is already up to date - no migrations needed.")
            return

        if dry_run:
            click.echo(f"🔍 Dry run - changes that would be made to {manifest_path}:")
            click.echo(
                "   Migrations would be applied to update the manifest to the latest version."
            )
            click.echo("   Run without --dry-run to apply the changes.")
            return

        current_cdk_version = metadata.version("airbyte_cdk")
        migrated_manifest["version"] = current_cdk_version

        with open(manifest_path, "w") as f:
            yaml.dump(migrated_manifest, f, default_flow_style=False, sort_keys=False)

        click.echo(
            f"✅ Successfully migrated {manifest_path} to the latest version ({current_cdk_version})."
        )

        try:
            schema = _get_declarative_component_schema()
            validate(migrated_manifest, schema)
            click.echo(f"✅ Migrated manifest {manifest_path} passes validation.")
        except ValidationError as e:
            click.echo(
                f"⚠️  Warning: Migrated manifest {manifest_path} still has validation issues:",
                err=True,
            )
            click.echo(f"   {e.message}", err=True)
            click.echo("   Manual fixes may be required.", err=True)

    except FileNotFoundError:
        click.echo(f"❌ Error: Manifest file {manifest_path} not found", err=True)
        sys.exit(1)
    except yaml.YAMLError as e:
        click.echo(f"❌ Error: Invalid YAML in {manifest_path}: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Unexpected error migrating {manifest_path}: {e}", err=True)
        sys.exit(1)


__all__ = [
    "manifest_cli_group",
]
