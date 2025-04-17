# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Implements the `airbyte-cdk build` command for building connector Docker images.

This command provides a simplified way to build connector Docker images without
requiring the full Airbyte CI pipeline, which uses Dagger.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from airbyte_cdk.utils.docker.build import run_command as docker_run_command


def run() -> None:
    """Entry point for the airbyte-cdk build command."""
    parser = argparse.ArgumentParser(description="Build connector Docker images")
    parser.add_argument("connector_dir", type=str, help="Path to the connector directory")
    parser.add_argument(
        "--tag", type=str, default="dev", help="Tag to apply to the built image (default: dev)"
    )
    parser.add_argument(
        "--no-verify", action="store_true", help="Skip verification of the built image"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args(sys.argv[1:])

    sys.exit(
        docker_run_command(
            connector_dir=Path(args.connector_dir),
            tag=args.tag,
            no_verify=args.no_verify,
            verbose=args.verbose,
        )
    )


if __name__ == "__main__":
    run()
