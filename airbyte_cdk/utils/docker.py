"""Docker build utilities for Airbyte CDK."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import tempfile
from email.policy import default
from pathlib import Path

import click

from airbyte_cdk.models.connector_metadata import ConnectorMetadata, MetadataFile

logger = logging.getLogger(__name__)

# This template accepts the following variables:
# - base_image: The base image to use for the build
# - extra_build_steps: Additional build steps to include in the Dockerfile
# - connector_snake_name: The snake_case name of the connector
# - connector_kebab_name: The kebab-case name of the connector
DOCKERFILE_TEMPLATE = """
FROM {base_image} AS builder

WORKDIR /airbyte/integration_code

COPY . ./
COPY {connector_snake_name} ./{connector_snake_name}
{extra_build_steps}

# RUN pip install --no-cache-dir uv
RUN pip install --no-cache-dir .

FROM {base_image}

WORKDIR /airbyte/integration_code

COPY --from=builder /usr/local /usr/local

COPY . .

ENV AIRBYTE_ENTRYPOINT="{connector_kebab_name}"
ENTRYPOINT ["{connector_kebab_name}"]
"""


def _build_image(
    context_dir: Path,
    dockerfile: Path,
    metadata: MetadataFile,
    tag: str,
    arch: str,
) -> str:
    """Build a Docker image for the specified architecture.

    Returns the tag of the built image.
    """
    docker_args: list[str] = [
        "docker",
        "build",
        "--platform",
        arch,
        "--file",
        str(dockerfile),
        "--label",
        f"io.airbyte.version={metadata.data.dockerImageTag}",
        "--label",
        f"io.airbyte.name={metadata.data.dockerRepository}",
        "-t",
        tag,
        str(context_dir),
    ]
    print(f"Building image: {tag} ({arch})")
    _ = run_docker_command(
        docker_args,
    )
    return tag


def _tag_image(
    tag: str,
    new_tags: list[str] | str,
) -> str:
    """Build a Docker image for the specified architecture.

    Returns the tag of the built image.
    """
    if not isinstance(new_tags, list):
        new_tags = [new_tags]

    print(f"Tagging image '{tag}' as: {', '.join(new_tags)}")
    docker_args = [
        "docker",
        "tag",
        tag,
        *new_tags,
    ]
    _ = subprocess.run(
        docker_args,
        text=True,
        check=True,
    )
    return tag


def build_connector_image(
    connector_name: str,
    connector_directory: Path,
    metadata: MetadataFile,
    tag: str,
    arch: str | None = None,
    no_verify: bool = False,
) -> None:
    connector_kebab_name = connector_name
    connector_snake_name = connector_kebab_name.replace("-", "_")

    dockerfile_path = connector_directory / "build" / "docker" / "Dockerfile"
    dockerignore_path = connector_directory / "build" / "docker" / "Dockerfile.dockerignore"

    dockerfile_path.parent.mkdir(parents=True, exist_ok=True)
    dockerfile_path.write_text(
        DOCKERFILE_TEMPLATE.format(
            base_image=metadata.data.connectorBuildOptions.baseImage,
            connector_snake_name=connector_snake_name,
            connector_kebab_name=connector_kebab_name,
            extra_build_steps="",
        )
    )
    dockerignore_path.write_text(
        "\n".join([
            "# This file is auto-generated. Do not edit.",
            "build/",
            ".venv/",
            "secrets/",
            "!setup.py",
            "!pyproject.toml",
            "!poetry.lock",
            "!poetry.toml",
            "!components.py",
            "!requirements.txt",
            "!README.md",
            "!metadata.yaml",
            "!build_customization.py",
            # f"!{connector_snake_name}/",
        ])
    )

    base_tag = f"{metadata.data.dockerRepository}:{tag}"
    arch_images: list[str] = []
    default_arch = "linux/amd64"
    for arch in ["linux/amd64", "linux/arm64"]:
        docker_tag = f"{base_tag}-{arch.replace('/', '-')}"
        docker_tag_parts = docker_tag.split("/")
        if len(docker_tag_parts) > 2:
            docker_tag = "/".join(docker_tag_parts[-1:])
        arch_images.append(
            _build_image(
                context_dir=connector_directory,
                dockerfile=dockerfile_path,
                metadata=metadata,
                tag=docker_tag,
                arch=arch,
            )
        )

    _tag_image(
        tag=f"{base_tag}-{default_arch.replace('/', '-')}",
        new_tags=[base_tag],
    )
    if not no_verify:
        if verify_image(base_tag):
            click.echo(f"Build completed successfully: {base_tag}")
            sys.exit(0)
        else:
            click.echo(f"Built image failed verification: {base_tag}", err=True)
            sys.exit(1)
    else:
        click.echo(f"Build completed successfully (without verification): {base_tag}")
        sys.exit(0)


def run_docker_command(cmd: list[str]) -> None:
    """Run a Docker command as a subprocess.

    Raises:
        subprocess.CalledProcessError: If the command fails and check is True.
    """
    logger.debug(f"Running command: {' '.join(cmd)}")

    process = subprocess.run(
        cmd,
        text=True,
        check=True,
    )


def verify_docker_installation() -> bool:
    """Verify Docker is installed and running."""
    try:
        run_docker_command(["docker", "--version"])
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def get_main_file_name(connector_dir: Path) -> str:
    """Get the main file name for the connector.

    Args:
        connector_dir: Path to the connector directory.

    Returns:
        The main file name.
    """
    build_customization_path = connector_dir / "build_customization.py"
    if build_customization_path.exists():
        content = build_customization_path.read_text()
        if "MAIN_FILE_NAME" in content:
            for line in content.splitlines():
                if line.strip().startswith("MAIN_FILE_NAME"):
                    parts = line.split("=", 1)
                    if len(parts) == 2:
                        main_file = parts[1].strip().strip("\"'")
                        return main_file

    return "main.py"


def execute_build_customization_hooks(connector_dir: Path, hook_type: str) -> None:
    """Execute pre or post install hooks from build_customization.py if present.

    Args:
        connector_dir: Path to the connector directory.
        hook_type: Type of hook to execute ('pre_connector_install' or 'post_connector_install').
    """
    build_customization_path = connector_dir / "build_customization.py"
    if not build_customization_path.exists():
        return

    logger.info(f"Checking for {hook_type} hook in build_customization.py")

    content = build_customization_path.read_text()
    if f"def {hook_type}" not in content:
        logger.debug(f"No {hook_type} hook found in build_customization.py")
        return

    logger.info(f"Executing {hook_type} hook from build_customization.py")

    hook_script = f"""
import sys
import os
sys.path.append('{connector_dir}')
try:
    from build_customization import {hook_type}
    print(f"Executing {hook_type} hook...")
    {hook_type}()
    print(f"{hook_type} hook executed successfully")
except Exception as e:
    print(f"Error executing {hook_type} hook: {{e}}")
    sys.exit(1)
"""

    with tempfile.NamedTemporaryFile(suffix=".py", mode="w") as temp_script:
        temp_script.write(hook_script)
        temp_script.flush()

        try:
            subprocess.run(
                ["python", temp_script.name],
                cwd=str(connector_dir),
                check=True,
                capture_output=True,
                text=True,
            )
            logger.info(f"Successfully executed {hook_type} hook")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to execute {hook_type} hook: {e.stderr}")
            raise


def verify_image(image_name: str) -> bool:
    """Verify the built image by running the spec command.

    Args:
        image_name: The full image name with tag.

    Returns:
        True if the spec command succeeds, False otherwise.
    """
    logger.info(f"Verifying image {image_name} with 'spec' command...")

    cmd = ["docker", "run", "--rm", image_name, "spec"]

    run_docker_command(cmd)
