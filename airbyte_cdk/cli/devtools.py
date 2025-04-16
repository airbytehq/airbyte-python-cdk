# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Dev tools for Airbyte CDK development.

This module provides a command line interface (CLI) for various development tasks related to the Airbyte CDK (Connector Development Kit).

These are invoked with the `airbyte-cdk` entrypoint, which is installed alongside the CDK.

For a list of available commands, run:

```bash
airbyte-cdk --help
```

"""
import sys
from pathlib import Path

import click
import pytest


@click.command(context_settings=dict(ignore_unknown_options=True, allow_extra_args=True))
@click.option(
    "--cwd",
    required=False,
    help="Path to the directory containing the source definition.",
)
@click.option("--docker-image", type=str, help="Docker image to test.")
@click.option("-v", "--verbose", count=True, help="Increase verbosity of output.")
@click.argument("pytest_args", nargs=-1, type=click.UNPROCESSED)
def test(cwd, docker_image, source_def, spec_file, verbose, pytest_args, ):
    """
    Run standard tests for an Airbyte source definition.

    You can pass additional pytest args after '--', e.g.:

        airbyte-cdk test --source-def source-pokeapi -- -k test_incremental -m "slow"
    """
    click.echo(f"🧪 Running tests for: {source_def}")
    if spec_file:
        click.echo(f"📄 Using spec/config file: {spec_file}")

    # Build base pytest args
    args = []

    # Set verbosity
    if verbose >= 1:
        args.append("-" + "v" * verbose)

    # Point to the test suite (you can add logic to choose suite per source)
    test_dir = Path("airbyte_cdk/test/standard_tests")
    args.append(str(test_dir))

    # Append passthrough pytest args
    args.extend(pytest_args)

    click.echo(f"🚀 pytest args: {' '.join(args)}")

    # Run pytest
    sys.exit(pytest.main(args))


def run() -> None:
    args: list[str] = sys.argv[1:]
    handle_command(args)
