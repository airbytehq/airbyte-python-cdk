# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Info command for the manifest runner CLI."""

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


@click.command()
def info() -> None:
    """Show manifest runner information and status."""
    if FASTAPI_AVAILABLE:
        click.echo("✅ Manifest runner dependencies are installed")
        click.echo(f"   FastAPI version: {fastapi.__version__}")
        click.echo(f"   Uvicorn version: {uvicorn.__version__}")
    else:
        click.echo("❌ Manifest runner dependencies not installed")
        click.echo("   Install with: pip install airbyte-cdk[manifest-runner]")
