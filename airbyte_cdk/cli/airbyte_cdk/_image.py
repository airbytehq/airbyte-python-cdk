# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Airbyte CDK 'image' commands.

The `airbyte-cdk image build` command provides a simple way to work with Airbyte
connector images.
"""

import sys
from pathlib import Path

import rich_click as click

from airbyte_cdk.cli.airbyte_cdk._util import resolve_connector_name_and_directory
from airbyte_cdk.models.connector_metadata import MetadataFile
from airbyte_cdk.utils.docker import (
    ConnectorImageBuildError,
    build_connector_image,
    verify_docker_installation,
)


@click.group(
    name="image",
    help=__doc__.replace("\n", "\n\n"),  # Render docstring as help text (markdown)
)
def image_cli_group() -> None:
    """Commands for working with connector Docker images."""


@image_cli_group.command()
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
@click.option("--tag", default="dev", help="Tag to apply to the built image (default: dev)")
@click.option("--no-verify", is_flag=True, help="Skip verification of the built image")
def build(
    connector_name: str | None = None,
    connector_directory: Path | None = None,
    *,
    tag: str = "dev",
    no_verify: bool = False,
) -> None:
    """Build a connector Docker image.

    This command builds a Docker image for a connector, using either
    the connector's Dockerfile or a base image specified in the metadata.
    The image is built for both AMD64 and ARM64 architectures.
    """
    if not verify_docker_installation():
        click.echo(
            "Docker is not installed or not running. Please install Docker and try again.", err=True
        )
        sys.exit(1)

    connector_name, connector_directory = resolve_connector_name_and_directory(
        connector_name=connector_name,
        connector_directory=connector_directory,
    )

    metadata_file_path: Path = connector_directory / "metadata.yaml"
    try:
        metadata = MetadataFile.from_file(metadata_file_path)
    except (FileNotFoundError, ValueError) as e:
        click.echo(
            f"Error loading metadata file '{metadata_file_path}': {e!s}",
            err=True,
        )
        sys.exit(1)
    click.echo(f"Building Image for Connector: {metadata.data.dockerRepository}:{tag}")
    try:
        build_connector_image(
            connector_directory=connector_directory,
            connector_name=connector_name,
            metadata=metadata,
            tag=tag,
            no_verify=no_verify,
        )
    except ConnectorImageBuildError as e:
        click.echo(
            f"Error building connector image: {e!s}",
            err=True,
        )
        sys.exit(1)


__all__ = [
    "image_cli_group",
]
