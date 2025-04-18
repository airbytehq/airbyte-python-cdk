# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Docker image commands."""

import click


@click.group(name="image")
def image_cli_group() -> None:
    """Docker image commands."""
    pass


__all__ = [
    "image_cli_group",
]
