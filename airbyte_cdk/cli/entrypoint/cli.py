"""Entrypoint for the Airbyte CDK CLI."""

from airbyte_cdk.cli.commands.cli import cli


def run():
    """Run the Airbyte CDK CLI."""
    cli(prog_name="airbyte-cdk")
