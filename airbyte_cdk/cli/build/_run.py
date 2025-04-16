"""Implements the `airbyte-cdk build` command for building connector Docker images.

This command provides a simplified way to build connector Docker images without
requiring the full Airbyte CI pipeline, which uses Dagger.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from airbyte_cdk.cli.build.models import ConnectorLanguage, ConnectorMetadata, MetadataFile

logger = logging.getLogger("airbyte-cdk.cli.build")


def set_up_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args(args: List[str]) -> argparse.Namespace:
    """Parse command line arguments for the build command."""
    parser = argparse.ArgumentParser(
        description="Build connector Docker images using the host Docker daemon"
    )
    parser.add_argument("connector_dir", type=str, help="Path to the connector directory")
    parser.add_argument(
        "--tag", type=str, default="dev", help="Tag to apply to the built image (default: dev)"
    )
    parser.add_argument(
        "--platform",
        type=str,
        choices=["linux/amd64", "linux/arm64"],
        default="linux/amd64",
        help="Platform to build for (default: linux/amd64)",
    )
    parser.add_argument(
        "--no-verify", action="store_true", help="Skip verification of the built image"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    return parser.parse_args(args)


def read_metadata(connector_dir: Path) -> ConnectorMetadata:
    """Read and parse connector metadata from metadata.yaml.

    Args:
        connector_dir: Path to the connector directory.

    Returns:
        The parsed connector metadata.

    Raises:
        FileNotFoundError: If the metadata.yaml file doesn't exist.
        ValueError: If the metadata is invalid.
    """
    metadata_path = connector_dir / "metadata.yaml"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    metadata_content = metadata_path.read_text()
    metadata_dict = yaml.safe_load(metadata_content)

    if not metadata_dict or "data" not in metadata_dict:
        raise ValueError("Invalid metadata format: missing 'data' field")

    metadata_file = MetadataFile.model_validate(metadata_dict)
    return metadata_file.data


def infer_connector_language(metadata: ConnectorMetadata, connector_dir: Path) -> ConnectorLanguage:
    """Infer the connector language from metadata and the file structure.

    Args:
        metadata: The connector metadata.
        connector_dir: Path to the connector directory.

    Returns:
        The inferred connector language.
    """
    if metadata.language is not None:
        return metadata.language

    if (connector_dir / "setup.py").exists() or (connector_dir / "pyproject.toml").exists():
        return ConnectorLanguage.PYTHON

    if (connector_dir / "build.gradle").exists():
        return ConnectorLanguage.JAVA

    if any((connector_dir / f).exists() for f in ["manifest.yaml", "spec.yaml", "spec.json"]):
        return ConnectorLanguage.LOW_CODE

    logger.warning("Could not determine connector language, using UNKNOWN.")
    return ConnectorLanguage.UNKNOWN


def run_docker_command(cmd: List[str], check: bool = True) -> Tuple[int, str, str]:
    """Run a Docker command as a subprocess.

    Args:
        cmd: The command to run.
        check: Whether to raise an exception if the command fails.

    Returns:
        Tuple of (return_code, stdout, stderr).

    Raises:
        subprocess.CalledProcessError: If the command fails and check is True.
    """
    logger.debug(f"Running command: {' '.join(cmd)}")

    process = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False
    )

    stdout = process.stdout.strip()
    stderr = process.stderr.strip()

    if process.returncode != 0:
        logger.error(f"Command failed with exit code {process.returncode}")
        logger.error(f"stderr: {stderr}")
        if check:
            raise subprocess.CalledProcessError(process.returncode, cmd, stdout, stderr)

    return process.returncode, stdout, stderr


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


def build_from_dockerfile(
    connector_dir: Path,
    metadata: ConnectorMetadata,
    tag: str,
    platform: str,
) -> str:
    """Build a Docker image for the connector using its Dockerfile.

    Args:
        connector_dir: Path to the connector directory.
        metadata: The connector metadata.
        tag: The tag to apply to the built image.
        platform: The platform to build for (e.g., linux/amd64).

    Returns:
        The full image name with tag.

    Raises:
        FileNotFoundError: If the Dockerfile is not found.
        subprocess.CalledProcessError: If the build fails.
    """
    dockerfile_path = connector_dir / "Dockerfile"
    if not dockerfile_path.exists():
        raise FileNotFoundError(f"Dockerfile not found: {dockerfile_path}")

    image_name = metadata.dockerRepository
    full_image_name = f"{image_name}:{tag}"

    logger.info(f"Building Docker image from Dockerfile: {full_image_name} for platform {platform}")
    logger.warning(
        "Building from Dockerfile is deprecated. Consider using a base image in metadata.yaml."
    )

    build_cmd = [
        "docker",
        "build",
        "--platform",
        platform,
        "-t",
        full_image_name,
        "--label",
        f"io.airbyte.version={metadata.dockerImageTag}",
        "--label",
        f"io.airbyte.name={metadata.dockerRepository}",
        str(connector_dir),
    ]

    try:
        run_docker_command(build_cmd)
        logger.info(f"Successfully built image: {full_image_name}")
        return full_image_name
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to build image: {e}")
        raise


def build_from_base_image(
    connector_dir: Path,
    metadata: ConnectorMetadata,
    tag: str,
    platform: str,
) -> str:
    """Build a Docker image for the connector using a base image.

    Args:
        connector_dir: Path to the connector directory.
        metadata: The connector metadata.
        tag: The tag to apply to the built image.
        platform: The platform to build for (e.g., linux/amd64).

    Returns:
        The full image name with tag.

    Raises:
        ValueError: If the base image is not specified in the metadata.
        subprocess.CalledProcessError: If the build fails.
    """
    if not metadata.connectorBuildOptions or not metadata.connectorBuildOptions.baseImage:
        raise ValueError("Base image not specified in metadata.connectorBuildOptions.baseImage")

    base_image = metadata.connectorBuildOptions.baseImage
    image_name = metadata.dockerRepository
    full_image_name = f"{image_name}:{tag}"

    logger.info(
        f"Building Docker image from base image {base_image}: {full_image_name} for platform {platform}"
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        dockerfile_content = f"""
FROM {base_image}

WORKDIR /airbyte/integration_code

COPY . .

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/{get_main_file_name(connector_dir)}"
ENTRYPOINT ["python", "/airbyte/integration_code/{get_main_file_name(connector_dir)}"]
"""

        dockerfile_path = temp_dir_path / "Dockerfile"
        dockerfile_path.write_text(dockerfile_content)

        for item in connector_dir.iterdir():
            if item.is_dir():
                shutil.copytree(item, temp_dir_path / item.name)
            else:
                shutil.copy2(item, temp_dir_path / item.name)

        build_cmd = [
            "docker",
            "build",
            "--platform",
            platform,
            "-t",
            full_image_name,
            "--label",
            f"io.airbyte.version={metadata.dockerImageTag}",
            "--label",
            f"io.airbyte.name={metadata.dockerRepository}",
            str(temp_dir_path),
        ]

        try:
            run_docker_command(build_cmd)
            logger.info(f"Successfully built image: {full_image_name}")
            return full_image_name
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to build image: {e}")
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

    returncode, stdout, stderr = run_docker_command(cmd, check=False)

    if returncode != 0:
        logger.error(f"Spec command failed with exit code {returncode}")
        logger.error(f"stderr: {stderr}")
        return False

    try:
        spec_output = json.loads(stdout)
        if "connectionSpecification" in spec_output:
            logger.info("Spec command succeeded and returned valid specification.")
            return True
        logger.warning("Spec command succeeded but returned unexpected format.")
        return True
    except json.JSONDecodeError:
        logger.warning("Spec command succeeded but returned invalid JSON.")
        return True


def run_command(args: List[str]) -> int:
    """Run the build command with the given arguments.

    Args:
        args: Command line arguments.

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    try:
        parsed_args = parse_args(args)
        set_up_logging(parsed_args.verbose)

        if not verify_docker_installation():
            logger.error(
                "Docker is not installed or not running. Please install Docker and try again."
            )
            return 1

        connector_dir = Path(parsed_args.connector_dir).absolute()

        if not connector_dir.exists():
            logger.error(f"Connector directory not found: {connector_dir}")
            return 1

        try:
            metadata = read_metadata(connector_dir)
            logger.info(f"Connector: {metadata.dockerRepository}")
            logger.info(f"Version: {metadata.dockerImageTag}")
        except (FileNotFoundError, ValueError) as e:
            logger.error(f"Error reading connector metadata: {e}")
            return 1

        language = infer_connector_language(metadata, connector_dir)
        logger.info(f"Detected connector language: {language}")

        try:
            if metadata.connectorBuildOptions and metadata.connectorBuildOptions.baseImage:
                image_name = build_from_base_image(
                    connector_dir, metadata, parsed_args.tag, parsed_args.platform
                )
            else:
                image_name = build_from_dockerfile(
                    connector_dir, metadata, parsed_args.tag, parsed_args.platform
                )

            if not parsed_args.no_verify:
                if verify_image(image_name):
                    logger.info(f"Build completed successfully: {image_name}")
                    return 0
                else:
                    logger.error(f"Built image failed verification: {image_name}")
                    return 1
            else:
                logger.info(f"Build completed successfully (without verification): {image_name}")
                return 0

        except (FileNotFoundError, ValueError, subprocess.CalledProcessError) as e:
            logger.error(f"Error building Docker image: {e}")
            return 1

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if parsed_args and parsed_args.verbose:
            import traceback

            logger.error(traceback.format_exc())
        return 1


def run() -> None:
    """Entry point for the airbyte-cdk build command."""
    sys.exit(run_command(sys.argv[1:]))


if __name__ == "__main__":
    run()
