"""Main CLI command group for Airbyte CDK."""

import click

from airbyte_cdk.cli.commands.image import image


@click.group()
def cli() -> None:
    """Airbyte CDK command-line interface."""
    pass


cli.add_command(image)
