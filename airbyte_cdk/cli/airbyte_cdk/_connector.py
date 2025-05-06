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

import os
from pathlib import Path
from types import ModuleType

import rich_click as click

# from airbyte_cdk.test.standard_tests import pytest_hooks
from airbyte_cdk.cli.airbyte_cdk._util import resolve_connector_name_and_directory
from airbyte_cdk.test.standard_tests.test_resources import find_connector_root_from_name
from airbyte_cdk.test.standard_tests.util import create_connector_test_suite

click.rich_click.TEXT_MARKUP = "markdown"

pytest: ModuleType | None
try:
    import pytest
except ImportError:
    pytest = None
    # Handle the case where pytest is not installed.
    # This prevents import errors when running the script without pytest installed.
    # We will raise an error later if pytest is required for a given command.


TEST_FILE_TEMPLATE = '''
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""FAST Airbyte Standard Tests for the {connector_name} source."""

#from airbyte_cdk.test.standard_tests import {base_class_name}
from airbyte_cdk.test.standard_tests.util import create_connector_test_suite
from pathlib import Path

pytest_plugins = [
    "airbyte_cdk.test.standard_tests.pytest_hooks",
]

TestSuite = create_connector_test_suite(
    connector_directory=Path(),
)

# class TestSuite({base_class_name}):
#     """Test suite for the {connector_name} source.

#     This class inherits from SourceTestSuiteBase and implements all of the tests in the suite.

#     As long as the class name starts with "Test", pytest will automatically discover and run the
#     tests in this class.
#     """
'''


@click.group(
    name="connector",
    help=__doc__.replace("\n", "\n\n"),  # Render docstring as help text (markdown)
)
def connector_cli_group() -> None:
    """Connector related commands."""
    pass


@connector_cli_group.command()
@click.option(
    "--connector-name",
    type=str,
    help="Name of the connector to test. Ignored if --connector-directory is provided.",
)
@click.option(
    "--connector-directory",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Path to the connector directory.",
)
@click.option(
    "--collect-only",
    is_flag=True,
    default=False,
    help="Only collect tests, do not run them.",
)
def test(
    connector_name: str | None = None,
    connector_directory: Path | None = None,
    *,
    collect_only: bool = False,
) -> None:
    """Run connector tests.

    This command runs the standard connector tests for a specific connector.

    If no connector name or directory is provided, we will look within the current working
    directory. If the current working directory is not a connector directory (e.g. starting
    with 'source-') and no connector name or path is provided, the process will fail.
    """
    if pytest is None:
        raise ImportError(
            "pytest is not installed. Please install pytest to run the connector tests."
        )
    click.echo("Connector test command executed.")
    connector_name, connector_directory = resolve_connector_name_and_directory(
        connector_name=connector_name,
        connector_directory=connector_directory,
    )

    connector_test_suite = create_connector_test_suite(
        connector_name=connector_name if not connector_directory else None,
        connector_directory=connector_directory,
    )

    pytest_args: list[str] = []
    if connector_directory:
        pytest_args.append(f"--rootdir={connector_directory}")
        os.chdir(str(connector_directory))
    else:
        print("No connector directory provided. Running tests in the current directory.")

    file_text = TEST_FILE_TEMPLATE.format(
        base_class_name=connector_test_suite.__bases__[0].__name__,
        connector_name=connector_name,
    )
    test_file_path = Path() / ".tmp" / "integration_tests/test_airbyte_standards.py"
    test_file_path = test_file_path.resolve().absolute()
    test_file_path.parent.mkdir(parents=True, exist_ok=True)
    test_file_path.write_text(file_text)

    if collect_only:
        pytest_args.append("--collect-only")

    pytest_args.append(str(test_file_path))
    click.echo(f"Running tests from connector directory: {connector_directory}...")
    click.echo(f"Test file: {test_file_path}")
    click.echo(f"Collect only: {collect_only}")
    click.echo(f"Pytest args: {pytest_args}")
    click.echo("Invoking Pytest...")
    pytest.main(
        pytest_args,
        plugins=[],
    )


__all__ = [
    "connector_cli_group",
]
