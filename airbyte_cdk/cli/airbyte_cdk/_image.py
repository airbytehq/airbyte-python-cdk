"""Airbyte CDK 'image' commands.

The `airbyte-cdk` command provides a simplified way to build connector Docker images without requiring the full Airbyte CI pipeline.

```bash
pip install airbyte-cdk

pipx run airbyte-cdk image [arguments]
```


```bash
airbyte-cdk image build /path/to/connector

airbyte-cdk image build /path/to/connector --tag custom_tag

airbyte-cdk image build /path/to/connector --no-verify

airbyte-cdk image build /path/to/connector --verbose
```


- `connector_dir`: Path to the connector directory (required)
- `--tag`: Tag to apply to the built image (default: "dev")
- `--no-verify`: Skip verification of the built image
- `--verbose`, `-v`: Enable verbose logging


The command reads the connector's metadata from the `metadata.yaml` file, builds a Docker image using the connector's Dockerfile, and verifies the image by running the `spec` command. The image is tagged according to the repository name specified in the metadata and the provided tag.

This command is designed to be a simpler alternative to the `airbyte-ci build` command, using Docker directly on the host machine instead of Dagger.
"""

import sys
from pathlib import Path

import click

from airbyte_cdk.models.connector_metadata import MetadataFile
from airbyte_cdk.utils.docker import (
    build_from_base_image,
    build_from_dockerfile,
    verify_docker_installation,
    verify_image,
)


@click.group(name="image")
def image_cli_group() -> None:
    """Commands for working with connector Docker images."""


@image_cli_group.command()
@click.argument(
    "connector_directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
)
@click.option("--tag", default="dev", help="Tag to apply to the built image (default: dev)")
@click.option("--no-verify", is_flag=True, help="Skip verification of the built image")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def build(
    connector_directory: Path,
    tag: str = "dev",
    no_verify: bool = False,
    verbose: bool = False,
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

    try:
        metadata = MetadataFile.from_file(connector_directory / "metadata.yaml")
        click.echo(f"Connector: {metadata.data.dockerRepository}")
        click.echo(f"Version: {metadata.data.dockerImageTag}")

        if metadata.data.language:
            click.echo(f"Connector language from metadata: {metadata.data.language}")
        else:
            click.echo("Connector language not specified in metadata")

        try:
            import subprocess

            result = subprocess.run(
                ["docker", "buildx", "inspect"], capture_output=True, text=True, check=False
            )

            if "linux/amd64" in result.stdout and "linux/arm64" in result.stdout:
                platforms = "linux/amd64,linux/arm64"
                click.echo(f"Building for platforms: {platforms}")
            else:
                platforms = "linux/amd64"
                click.echo(
                    f"Multi-platform build not available. Building for platform: {platforms}"
                )
                click.echo(
                    "To enable multi-platform builds, configure Docker buildx with: docker buildx create --use"
                )
        except Exception:
            platforms = "linux/amd64"
            click.echo(f"Multi-platform build check failed. Building for platform: {platforms}")

        if metadata.data.connectorBuildOptions and metadata.data.connectorBuildOptions.baseImage:
            image_name = build_from_base_image(connector_directory, metadata, tag, platforms)
        else:
            image_name = build_from_dockerfile(connector_directory, metadata, tag, platforms)

        if not no_verify:
            if verify_image(image_name):
                click.echo(f"Build completed successfully: {image_name}")
                sys.exit(0)
            else:
                click.echo(f"Built image failed verification: {image_name}", err=True)
                sys.exit(1)
        else:
            click.echo(f"Build completed successfully (without verification): {image_name}")
            sys.exit(0)

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        if verbose:
            import traceback

            click.echo(traceback.format_exc(), err=True)
        sys.exit(1)


__all__ = [
    "image_cli_group",
]
