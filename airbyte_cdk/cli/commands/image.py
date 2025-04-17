"""Image-related commands for the Airbyte CDK CLI."""

import sys
from pathlib import Path

import click

from airbyte_cdk.cli.build._run import (
    build_from_base_image,
    build_from_dockerfile,
    infer_connector_language,
    read_metadata,
    set_up_logging,
    verify_docker_installation,
    verify_image,
)


@click.group()
def image() -> None:
    """Commands for working with connector Docker images."""
    pass


@image.command()
@click.argument(
    "connector_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path)
)
@click.option("--tag", default="dev", help="Tag to apply to the built image (default: dev)")
@click.option(
    "--platform",
    type=click.Choice(["linux/amd64", "linux/arm64"]),
    default="linux/amd64",
    help="Platform to build for (default: linux/amd64)",
)
@click.option("--no-verify", is_flag=True, help="Skip verification of the built image")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def build(connector_dir: Path, tag: str, platform: str, no_verify: bool, verbose: bool) -> None:
    """Build a connector Docker image.

    This command builds a Docker image for a connector, using either
    the connector's Dockerfile or a base image specified in the metadata.
    """
    set_up_logging(verbose)

    if not verify_docker_installation():
        click.echo(
            "Docker is not installed or not running. Please install Docker and try again.", err=True
        )
        sys.exit(1)

    try:
        metadata = read_metadata(connector_dir)
        click.echo(f"Connector: {metadata.dockerRepository}")
        click.echo(f"Version: {metadata.dockerImageTag}")

        language = infer_connector_language(metadata, connector_dir)
        click.echo(f"Detected connector language: {language}")

        if metadata.connectorBuildOptions and metadata.connectorBuildOptions.baseImage:
            image_name = build_from_base_image(connector_dir, metadata, tag, platform)
        else:
            image_name = build_from_dockerfile(connector_dir, metadata, tag, platform)

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
