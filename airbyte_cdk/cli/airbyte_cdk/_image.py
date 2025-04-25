# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Docker image commands.

Coming soon.

This module is planned to provide a command line interface (CLI) for building
Docker images for Airbyte CDK connectors.
"""

import click


@click.group(
    name="image",
    help=__doc__.replace("\n", "\n\n"),  # Render docstring as help text (markdown)
)
def image_cli_group() -> None:
    """Docker image commands."""
    pass


__all__ = [
    "image_cli_group",
]
