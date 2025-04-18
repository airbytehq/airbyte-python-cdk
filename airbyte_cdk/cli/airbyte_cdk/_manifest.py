# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Manifest related commands."""

import click


@click.group(name="manifest")
def manifest_cli_group() -> None:
    """Manifest related commands."""
    pass


__all__ = [
    "manifest_cli_group",
]
