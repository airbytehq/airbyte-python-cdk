# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Manifest related commands.

Coming soon.

This module is planned to provide a command line interface (CLI) for validating
Airbyte CDK manifests.
"""

import click


@click.group(name="manifest")
def manifest_cli_group(
    help=__doc__,
) -> None:
    """Manifest related commands."""
    pass


__all__ = [
    "manifest_cli_group",
]
