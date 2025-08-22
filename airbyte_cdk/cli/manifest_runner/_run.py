# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Standalone CLI for the Airbyte CDK Manifest Runner.

This CLI provides commands for running and managing the FastAPI-based manifest runner server.

**Installation:**

To use the manifest-runner functionality, install the CDK with the manifest-runner extra:

```bash
pip install airbyte-cdk[manifest-runner]
# or
poetry install --extras manifest-runner
```

**Usage:**

```bash
manifest-runner start --port 8000
manifest-runner info
manifest-runner --help
```
"""

import sys
from typing import Optional

import rich_click as click

# Import server dependencies with graceful fallback
try:
    import fastapi
    import uvicorn

    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    fastapi = None
    uvicorn = None


def _check_manifest_runner_dependencies() -> None:
    """Check if manifest-runner dependencies are installed."""
    if not FASTAPI_AVAILABLE:
        click.echo(
            "❌ Manifest runner dependencies not found. Please install with:\n\n"
            "  pip install airbyte-cdk[manifest-runner]\n"
            "  # or\n"
            "  poetry install --extras manifest-runner\n",
            err=True,
        )
        sys.exit(1)


@click.group(
    help=__doc__.replace("\n", "\n\n"),  # Render docstring as help text (markdown)
    invoke_without_command=True,
)
@click.option(
    "--version",
    is_flag=True,
    help="Show the version of the Airbyte CDK Manifest Runner.",
)
@click.pass_context
def cli(
    ctx: click.Context,
    version: bool,
) -> None:
    """Airbyte CDK Manifest Runner CLI."""
    if version:
        click.echo("Airbyte CDK Manifest Runner v1.0.0")
        ctx.exit()

    if ctx.invoked_subcommand is None:
        # If no subcommand is provided, show the help message.
        click.echo(ctx.get_help())
        ctx.exit()


@cli.command()
@click.option(
    "--host",
    default="127.0.0.1",
    help="Host to bind the server to",
    show_default=True,
)
@click.option(
    "--port",
    default=8000,
    help="Port to bind the server to",
    show_default=True,
)
@click.option(
    "--reload",
    is_flag=True,
    help="Enable auto-reload for development",
)
def start(host: str, port: int, reload: bool) -> None:
    """Start the FastAPI manifest runner server."""
    _check_manifest_runner_dependencies()

    # Import and use the main server function
    from airbyte_cdk.manifest_runner.main import run_server

    run_server(
        host=host,
        port=port,
        reload=reload,
    )


@cli.command()
def info() -> None:
    """Show manifest runner information and status."""
    if FASTAPI_AVAILABLE:
        click.echo("✅ Manifest runner dependencies are installed")
        click.echo(f"   FastAPI version: {fastapi.__version__}")
        click.echo(f"   Uvicorn version: {uvicorn.__version__}")
    else:
        click.echo("❌ Manifest runner dependencies not installed")
        click.echo("   Install with: pip install airbyte-cdk[manifest-runner]")


def run() -> None:
    """Entry point for the manifest-runner CLI."""
    cli()


if __name__ == "__main__":
    run()
