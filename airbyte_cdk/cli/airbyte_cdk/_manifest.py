# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Manifest related commands.

This module provides a command line interface (CLI) for validating and migrating
Airbyte CDK manifests.
"""

import copy
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
from airbyte_cdk.sources.declarative.parsers.manifest_normalizer import (
    ManifestNormalizer,
)

EXIT_SUCCESS = 0
EXIT_FIXABLE_VIA_MIGRATION = 1
EXIT_NON_FIXABLE_ISSUES = 2
EXIT_GENERAL_ERROR = 3


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
@click.option(
    "--strict",
    is_flag=True,
    help="Enable strict mode: fail if migration is available even for valid manifests",
)
def validate_manifest(manifest_path: Path, strict: bool) -> None:
    """Validate a manifest file against the declarative component schema.

    This command validates the manifest file and checks version compatibility.
    If validation fails, it will suggest running the migrate command if needed.

    Exit codes:

    \\b
    0: Manifest is valid and up to date
    \\b
    1: Manifest has issues that are fixable via migration
    \\b
    2: Manifest has validation errors that are NOT fixable via migration
    \\b
    3: General errors (file not found, invalid YAML, etc.)
    """
    try:
        manifest_dict = yaml.safe_load(manifest_path.read_text())

        if not isinstance(manifest_dict, dict):
            click.echo(
                f"❌ Error: Manifest file {manifest_path} does not contain a valid YAML dictionary",
                err=True,
            )
            sys.exit(EXIT_GENERAL_ERROR)

        schema = _get_declarative_component_schema()

        validation_error = None
        try:
            validate(manifest_dict, schema)
            original_is_valid = True
        except ValidationError as e:
            original_is_valid = False
            validation_error = e

        migration_handler = ManifestMigrationHandler(copy.deepcopy(manifest_dict))
        migrated_manifest = migration_handler.apply_migrations()

        migration_available = migrated_manifest != manifest_dict

        if original_is_valid and not migration_available:
            click.echo(f"✅ Manifest {manifest_path} is valid and up to date.")
            return

        if original_is_valid and migration_available:
            if not strict:
                click.echo(f"✅ Manifest {manifest_path} is valid and up to date.")
                return
            else:
                click.echo(
                    f"⚠️  Manifest {manifest_path} is valid but could benefit from migration to the latest version.",
                    err=True,
                )
                click.echo(
                    "Run 'airbyte-cdk manifest migrate' to apply available migrations.", err=True
                )
                sys.exit(EXIT_FIXABLE_VIA_MIGRATION)

        if migration_available:
            try:
                validate(migrated_manifest, schema)
                click.echo(f"❌ Validation failed for {manifest_path}:", err=True)
                if validation_error:
                    click.echo(f"   {validation_error.message}", err=True)
                click.echo(
                    "✅ Issues are fixable via migration. Run 'airbyte-cdk manifest migrate' to fix these issues.",
                    err=True,
                )
                sys.exit(EXIT_FIXABLE_VIA_MIGRATION)
            except ValidationError:
                click.echo(f"❌ Validation failed for {manifest_path}:", err=True)
                if validation_error:
                    click.echo(f"   {validation_error.message}", err=True)
                sys.exit(EXIT_NON_FIXABLE_ISSUES)
        else:
            click.echo(f"❌ Validation failed for {manifest_path}:", err=True)
            if validation_error:
                click.echo(f"   {validation_error.message}", err=True)
            sys.exit(EXIT_NON_FIXABLE_ISSUES)

    except FileNotFoundError:
        click.echo(f"❌ Error: Manifest file {manifest_path} not found", err=True)
        sys.exit(EXIT_GENERAL_ERROR)
    except yaml.YAMLError as e:
        click.echo(f"❌ Error: Invalid YAML in {manifest_path}: {e}", err=True)
        sys.exit(EXIT_GENERAL_ERROR)
    except Exception as e:
        click.echo(f"❌ Unexpected error validating {manifest_path}: {e}", err=True)
        sys.exit(EXIT_GENERAL_ERROR)


@manifest_cli_group.command("migrate")
@click.option(
    "--manifest-path",
    type=click.Path(exists=True, path_type=Path),
    default="manifest.yaml",
    help="Path to the manifest file to migrate (default: manifest.yaml)",
)
@click.option(
    "--in-place",
    is_flag=True,
    help="Modify the file in place instead of printing to stdout",
)
@click.option(
    "--exit-non-zero",
    is_flag=True,
    help="Return non-zero exit code if the file is modified",
)
@click.option(
    "--quiet",
    is_flag=True,
    help="Suppress output and return non-zero exit code if modified",
)
def migrate_manifest(manifest_path: Path, in_place: bool, exit_non_zero: bool, quiet: bool) -> None:
    """Apply migrations to make a manifest file compatible with the latest version.

    This command applies all necessary migrations to update the manifest file
    to be compatible with the latest CDK version.

    By default, the migrated manifest is printed to stdout. Use --in-place to modify the file directly.
    """
    try:
        original_manifest = yaml.safe_load(manifest_path.read_text())

        if not isinstance(original_manifest, dict):
            click.echo(
                f"❌ Error: Manifest file {manifest_path} does not contain a valid YAML dictionary",
                err=True,
            )
            sys.exit(EXIT_GENERAL_ERROR)

        migration_handler = ManifestMigrationHandler(original_manifest)
        migrated_manifest = migration_handler.apply_migrations()

        if quiet:
            exit_non_zero = True

        file_modified = migrated_manifest != original_manifest

        if not file_modified:
            if not quiet:
                click.echo(
                    f"✅ Manifest {manifest_path} is already up to date - no migrations needed."
                )
            return

        current_cdk_version = metadata.version("airbyte_cdk")
        migrated_manifest["version"] = current_cdk_version

        migrated_yaml = yaml.dump(migrated_manifest, default_flow_style=False, sort_keys=False)

        if in_place:
            manifest_path.write_text(migrated_yaml)
            if not quiet:
                click.echo(
                    f"✅ Successfully migrated {manifest_path} to the latest version ({current_cdk_version})."
                )
        else:
            click.echo(migrated_yaml, nl=False)

        if exit_non_zero and file_modified:
            sys.exit(1)

        if in_place and not quiet:
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
        sys.exit(EXIT_GENERAL_ERROR)
    except yaml.YAMLError as e:
        click.echo(f"❌ Error: Invalid YAML in {manifest_path}: {e}", err=True)
        sys.exit(EXIT_GENERAL_ERROR)
    except Exception as e:
        click.echo(f"❌ Unexpected error migrating {manifest_path}: {e}", err=True)
        sys.exit(EXIT_FIXABLE_VIA_MIGRATION)


@manifest_cli_group.command("normalize")
@click.option(
    "--manifest-path",
    type=click.Path(exists=True, path_type=Path),
    default="manifest.yaml",
    help="Path to the manifest file to normalize (default: manifest.yaml)",
)
@click.option(
    "--in-place",
    is_flag=True,
    help="Modify the file in place instead of printing to stdout",
)
@click.option(
    "--exit-non-zero",
    is_flag=True,
    help="Return non-zero exit code if the file is modified",
)
@click.option(
    "--quiet",
    is_flag=True,
    help="Suppress output and return non-zero exit code if modified",
)
def normalize_manifest(
    manifest_path: Path, in_place: bool, exit_non_zero: bool, quiet: bool
) -> None:
    """Normalize a manifest file by removing duplicated definitions and replacing them with references.

    This command normalizes the manifest file by deduplicating elements and
    creating references to shared components, making the manifest more maintainable.

    By default, the normalized manifest is printed to stdout. Use --in-place to modify the file directly.
    """
    try:
        original_manifest = yaml.safe_load(manifest_path.read_text())

        if not isinstance(original_manifest, dict):
            click.echo(
                f"❌ Error: Manifest file {manifest_path} does not contain a valid YAML dictionary",
                err=True,
            )
            sys.exit(EXIT_GENERAL_ERROR)

        schema = _get_declarative_component_schema()
        normalizer = ManifestNormalizer(original_manifest, schema)
        normalized_manifest = normalizer.normalize()

        if quiet:
            exit_non_zero = True

        file_modified = normalized_manifest != original_manifest

        if not file_modified:
            if not quiet:
                click.echo(
                    f"✅ Manifest {manifest_path} is already normalized - no changes needed."
                )
            return

        normalized_yaml = yaml.dump(normalized_manifest, default_flow_style=False, sort_keys=False)

        if in_place:
            manifest_path.write_text(normalized_yaml)
            if not quiet:
                click.echo(f"✅ Successfully normalized {manifest_path}.")
        else:
            click.echo(normalized_yaml, nl=False)

        if exit_non_zero and file_modified:
            sys.exit(1)

        if in_place and not quiet:
            try:
                validate(normalized_manifest, schema)
                click.echo(f"✅ Normalized manifest {manifest_path} passes validation.")
            except ValidationError as e:
                click.echo(
                    f"⚠️  Warning: Normalized manifest {manifest_path} has validation issues:",
                    err=True,
                )
                click.echo(f"   {e.message}", err=True)
                click.echo("   Manual fixes may be required.", err=True)

    except FileNotFoundError:
        click.echo(f"❌ Error: Manifest file {manifest_path} not found", err=True)
        sys.exit(EXIT_GENERAL_ERROR)
    except yaml.YAMLError as e:
        click.echo(f"❌ Error: Invalid YAML in {manifest_path}: {e}", err=True)
        sys.exit(EXIT_GENERAL_ERROR)
    except Exception as e:
        click.echo(f"❌ Unexpected error normalizing {manifest_path}: {e}", err=True)
        sys.exit(EXIT_GENERAL_ERROR)


__all__ = [
    "manifest_cli_group",
]
