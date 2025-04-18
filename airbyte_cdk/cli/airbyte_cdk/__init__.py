# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""CLI commands for `airbyte-cdk`.

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

from typing import cast

import click

from airbyte_cdk.cli.airbyte_cdk._connector import connector_cli_group
from airbyte_cdk.cli.airbyte_cdk._manifest import manifest_cli_group


@click.group(
    help=cast(str, __doc__).replace("\n", "\n\n"),  # Workaround to format help text correctly
)
def cli() -> None:
    """Airbyte CDK CLI.

    Help text is provided from the file-level docstring.
    """


cli.add_command(connector_cli_group)
cli.add_command(manifest_cli_group)


if __name__ == "__main__":
    cli()
