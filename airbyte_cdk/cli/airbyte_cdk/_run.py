# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""CLI command for `airbyte-cdk`."""

USAGE = """CLI command for `airbyte-cdk`.

This CLI interface allows you to interact with your connector, including
testing and running commands.

**Basic Usage:**

```bash
airbyte-cdk --help
airbyte-cdk connector --help
airbyte-cdk manifest --help
```

**Running Statelessly:**

You can run the latest version of this CLI, from any machine, using `pipx` or `uvx`:

```bash
# Run the latest version of the CLI:
pipx run airbyte-cdk connector --help
uvx airbyte-cdk connector --help

# Run from a specific CDK version:
pipx run airbyte-cdk==6.5.1 connector --help
uvx airbyte-cdk==6.5.1 connector --help
```

**Running within your virtualenv:**

You can also run from your connector's virtualenv:

```bash
poetry run airbyte-cdk connector --help
```

"""

import rich_click as click

click.rich_click.TEXT_MARKUP = "markdown"


@click.group()
def connector() -> None:
    """Connector related commands."""
    pass

@click.group()
def manifest() -> None:
    """Manifest related commands."""
    pass

@connector.command()
def test() -> None:
    """Run connector tests."""
    click.echo("Connector test command executed.")

@click.group(
    help=USAGE.replace("\n", "\n\n"),
)
def cli() -> None:
    """Airbyte CDK CLI.

    Help text is provided from the file-level docstring.
    """

cli.add_command(connector)
cli.add_command(manifest)

def main() -> None:
    cli()

if __name__ == "__main__":
    main()
