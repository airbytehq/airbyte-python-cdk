"""Docker build utilities for Airbyte CDK."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import click

from airbyte_cdk.models.connector_metadata import ConnectorLanguage, MetadataFile
from airbyte_cdk.utils.docker_image_templates import (
    DOCKERIGNORE_TEMPLATE,
    JAVA_CONNECTOR_DOCKERFILE_TEMPLATE,
    MANIFEST_ONLY_DOCKERFILE_TEMPLATE,
    PYTHON_CONNECTOR_DOCKERFILE_TEMPLATE,
)


@dataclass(kw_only=True)
class ConnectorImageBuildError(Exception):
    """Custom exception for Docker build errors."""

    error_text: str
    build_args: list[str]

    def __str__(self) -> str:
        return "\n".join(
            [
                f"ConnectorImageBuildError: Could not build image.",
                f"Build args: {self.build_args}",
                f"Error text: {self.error_text}",
            ]
        )


logger = logging.getLogger(__name__)


class ArchEnum(str, Enum):
    """Enum for supported architectures."""

    ARM64 = "arm64"
    AMD64 = "amd64"


def _build_image(
    context_dir: Path,
    dockerfile: Path,
    metadata: MetadataFile,
    tag: str,
    arch: ArchEnum,
    build_args: dict[str, str | None] | None = None,
) -> str:
    """Build a Docker image for the specified architecture.

    Returns the tag of the built image.

    Raises: ConnectorImageBuildError if the build fails.
    """
    docker_args: list[str] = [
        "docker",
        "build",
        "--platform",
        f"linux/{arch.value}",
        "--file",
        str(dockerfile),
        "--label",
        f"io.airbyte.version={metadata.data.dockerImageTag}",
        "--label",
        f"io.airbyte.name={metadata.data.dockerRepository}",
    ]
    if build_args:
        for key, value in build_args.items():
            if value is not None:
                docker_args.append(f"--build-arg={key}={value}")
            else:
                docker_args.append(f"--build-arg={key}")
    docker_args.extend(
        [
            "-t",
            tag,
            str(context_dir),
        ]
    )

    print(f"Building image: {tag} ({arch})")
    try:
        run_docker_command(
            docker_args,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise ConnectorImageBuildError(
            error_text=e.stderr,
            build_args=docker_args,
        ) from e

    return tag


def _tag_image(
    tag: str,
    new_tags: list[str] | str,
) -> None:
    """Build a Docker image for the specified architecture.

    Returns the tag of the built image.

    Raises:
        ConnectorImageBuildError: If the docker tag command fails.
    """
    if not isinstance(new_tags, list):
        new_tags = [new_tags]

    for new_tag in new_tags:
        print(f"Tagging image '{tag}' as: {new_tag}")
        docker_args = [
            "docker",
            "tag",
            tag,
            new_tag,
        ]
        try:
            run_docker_command(
                docker_args,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise ConnectorImageBuildError(
                error_text=e.stderr,
                build_args=docker_args,
            ) from e


def build_connector_image(
    connector_name: str,
    connector_directory: Path,
    metadata: MetadataFile,
    tag: str,
    primary_arch: ArchEnum = ArchEnum.ARM64,  # Assume MacBook M series by default
    no_verify: bool = False,
) -> None:
    """Build a connector Docker image.

    This command builds a Docker image for a connector, using either
    the connector's Dockerfile or a base image specified in the metadata.
    The image is built for both AMD64 and ARM64 architectures.

    Args:
        connector_name: The name of the connector.
        connector_directory: The directory containing the connector code.
        metadata: The metadata of the connector.
        tag: The tag to apply to the built image.
        primary_arch: The primary architecture for the build (default: arm64). This
            architecture will be used for the same-named tag. Both AMD64 and ARM64
            images will be built, with the suffixes '-amd64' and '-arm64'.
        no_verify: If True, skip verification of the built image.

    Raises:
        ValueError: If the connector build options are not defined in metadata.yaml.
        ConnectorImageBuildError: If the image build or tag operation fails.
    """
    connector_kebab_name = connector_name
    connector_snake_name = connector_kebab_name.replace("-", "_")

    dockerfile_path = connector_directory / "build" / "docker" / "Dockerfile"
    dockerignore_path = connector_directory / "build" / "docker" / "Dockerfile.dockerignore"

    extra_build_script: str = ""
    build_customization_path = connector_directory / "build_customization.py"
    if build_customization_path.exists():
        extra_build_script = str(build_customization_path)

    dockerfile_path.parent.mkdir(parents=True, exist_ok=True)
    if not metadata.data.connectorBuildOptions:
        raise ValueError(
            "Connector build options are not defined in metadata.yaml. "
            "Please check the connector's metadata file."
        )

    base_image = metadata.data.connectorBuildOptions.baseImage

    dockerfile_path.write_text(get_dockerfile_template(metadata))
    dockerignore_path.write_text(DOCKERIGNORE_TEMPLATE)

    build_args: dict[str, str | None] = {
        "BASE_IMAGE": base_image,
        "CONNECTOR_SNAKE_NAME": connector_snake_name,
        "CONNECTOR_KEBAB_NAME": connector_kebab_name,
        "EXTRA_BUILD_SCRIPT": extra_build_script,
    }

    base_tag = f"{metadata.data.dockerRepository}:{tag}"
    arch_images: list[str] = []

    if metadata.data.language == ConnectorLanguage.JAVA:
        # This assumes that the repo root ('airbyte') is three levels above the
        # connector directory (airbyte/airbyte-integrations/connectors/source-foo).
        repo_root = connector_directory.parent.parent.parent
        # For Java connectors, we need to build the connector tar file first.
        subprocess.run(
            [
                "./gradlew",
                f":airbyte-integrations:connectors:{connector_name}:distTar",
            ],
            cwd=repo_root,
            text=True,
            check=True,
        )

    for arch in [ArchEnum.AMD64, ArchEnum.ARM64]:
        docker_tag = f"{base_tag}-{arch.value}"
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
                build_args=build_args,
            )
        )

    _tag_image(
        tag=f"{base_tag}-{primary_arch.value}",
        new_tags=[base_tag],
    )
    if not no_verify:
        if verify_connector_image(base_tag):
            click.echo(f"Build completed successfully: {base_tag}")
            sys.exit(0)
        else:
            click.echo(f"Built image failed verification: {base_tag}", err=True)
            sys.exit(1)
    else:
        click.echo(f"Build completed successfully (without verification): {base_tag}")
        sys.exit(0)


def get_dockerfile_template(
    metadata: MetadataFile,
) -> str:
    """Get the Dockerfile template for the connector.

    Args:
        metadata: The metadata of the connector.
        connector_name: The name of the connector.

    Returns:
        The Dockerfile template as a string.
    """
    if metadata.data.language == ConnectorLanguage.PYTHON:
        return PYTHON_CONNECTOR_DOCKERFILE_TEMPLATE

    if metadata.data.language == ConnectorLanguage.MANIFEST_ONLY:
        return MANIFEST_ONLY_DOCKERFILE_TEMPLATE

    if metadata.data.language == ConnectorLanguage.JAVA:
        return JAVA_CONNECTOR_DOCKERFILE_TEMPLATE

    raise ValueError(
        f"Unsupported connector language: {metadata.data.language}. "
        "Please check the connector's metadata file."
    )


def run_docker_command(
    cmd: list[str],
    *,
    check: bool = True,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run a Docker command as a subprocess.

    Args:
        cmd: The command to run as a list of strings.
        check: If True, raises an exception if the command fails. If False, the caller is
            responsible for checking the return code.
        capture_output: If True, captures stdout and stderr and returns to the caller.
            If False, the output is printed to the console.

    Raises:
        subprocess.CalledProcessError: If the command fails and check is True.
    """
    print(f"Running command: {' '.join(cmd)}")

    process = subprocess.run(
        cmd,
        text=True,
        check=check,
        # If capture_output=True, stderr and stdout are captured and returned to caller:
        capture_output=capture_output,
        env={**os.environ, "DOCKER_BUILDKIT": "1"},
    )
    return process


def verify_docker_installation() -> bool:
    """Verify Docker is installed and running."""
    try:
        run_docker_command(["docker", "--version"])
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def verify_connector_image(
    image_name: str,
) -> bool:
    """Verify the built image by running the spec command.

    Args:
        image_name: The full image name with tag.

    Returns:
        True if the spec command succeeds, False otherwise.
    """
    logger.info(f"Verifying image {image_name} with 'spec' command...")

    cmd = ["docker", "run", "--rm", image_name, "spec"]

    try:
        result = run_docker_command(
            cmd,
            check=True,
            capture_output=True,
        )
        # check that the output is valid JSON
        if result.stdout:
            found_spec_output = False
            for line in result.stdout.split("\n"):
                if line.strip():
                    try:
                        # Check if the line is a valid JSON object
                        msg = json.loads(line)
                        if isinstance(msg, dict) and "type" in msg and msg["type"] == "SPEC":
                            found_spec_output = True

                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON output from spec command: {e}: {line}")

            if not found_spec_output:
                logger.error("No valid JSON output found for spec command.")
                return False
        else:
            logger.error("No output from spec command.")
            return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Image verification failed: {e.stderr}")
        return False

    return True
