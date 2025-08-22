# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Common utilities for manifest runner CLI commands."""

import sys

import rich_click as click

# Import server dependencies with graceful fallback
try:
    import fastapi  # noqa: F401
    import uvicorn  # noqa: F401

    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False


def check_manifest_runner_dependencies() -> None:
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