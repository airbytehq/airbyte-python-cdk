"""Implements the `airbyte-cdk build` command for building connector Docker images.

This command provides a simplified way to build connector Docker images without
requiring the full Airbyte CI pipeline, which uses Dagger.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

from airbyte_cdk.cli.build.models import ConnectorMetadata, MetadataFile

logger = logging.getLogger("airbyte-cdk.cli.build")


def set_up_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


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
    platforms: str = "linux/amd64,linux/arm64",
) -> str:
    """Build a Docker image for the connector using its Dockerfile.

    Args:
        connector_dir: Path to the connector directory.
        metadata: The connector metadata.
        tag: The tag to apply to the built image.
        platforms: The platforms to build for (default: "linux/amd64,linux/arm64").

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

    logger.info(
        f"Building Docker image from Dockerfile: {full_image_name} for platforms {platforms}"
    )
    logger.warning(
        "Building from Dockerfile is deprecated. Consider using a base image in metadata.yaml."
    )

    build_cmd = [
        "docker",
        "buildx",
        "build",
        "--platform",
        platforms,
        "-t",
        full_image_name,
        "--label",
        f"io.airbyte.version={metadata.dockerImageTag}",
        "--label",
        f"io.airbyte.name={metadata.dockerRepository}",
        "--load",  # Load the image into the local Docker daemon
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
    platforms: str = "linux/amd64,linux/arm64",
) -> str:
    """Build a Docker image for the connector using a base image.

    Args:
        connector_dir: Path to the connector directory.
        metadata: The connector metadata.
        tag: The tag to apply to the built image.
        platforms: The platforms to build for (default: "linux/amd64,linux/arm64").

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
        f"Building Docker image from base image {base_image}: {full_image_name} for platforms {platforms}"
    )

    docker_dir = connector_dir / "build" / "docker"
    docker_dir.mkdir(parents=True, exist_ok=True)

    dockerfile_path = docker_dir / "Dockerfile"
    dockerignore_path = docker_dir / ".dockerignore"

    os.environ["DOCKER_BUILDKIT"] = "1"

    try:
        main_file = get_main_file_name(connector_dir)
        logger.info(f"Using main file: {main_file}")

        dockerfile_content = f"""
FROM {base_image}

WORKDIR /airbyte/integration_code

COPY . .

RUN pip install .

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/{main_file}"
ENTRYPOINT ["python", "/airbyte/integration_code/{main_file}"]
"""

        dockerignore_content = """
*

!**/*.py
!**/*.yaml
!**/*.yml
!**/*.json
!**/*.md
!**/*.txt
!**/*.sh
!**/*.sql
!**/*.csv
!**/*.tsv
!**/*.ini
!**/*.toml
!**/*.lock
!**/*.cfg
!**/*.conf
!**/*.properties
!LICENSE
!NOTICE
!requirements.txt
!setup.py
!pyproject.toml
!poetry.lock
!poetry.toml
!Dockerfile
!.dockerignore

**/__pycache__
**/.pytest_cache
**/.venv
**/.coverage
**/venv
**/.idea
**/.vscode
**/.DS_Store
**/node_modules
**/.git
build/docker
"""

        dockerfile_path.write_text(dockerfile_content)
        dockerignore_path.write_text(dockerignore_content)

        build_cmd = [
            "docker",
            "buildx",
            "build",
            "--platform",
            platforms,
            "-t",
            full_image_name,
            "--label",
            f"io.airbyte.version={metadata.dockerImageTag}",
            "--label",
            f"io.airbyte.name={metadata.dockerRepository}",
            "--file",
            str(dockerfile_path),
            "--ignorefile",
            str(dockerignore_path),
            "--load",  # Load the image into the local Docker daemon
            str(connector_dir),
        ]

        try:
            run_docker_command(build_cmd)
            logger.info(f"Successfully built image: {full_image_name}")
            return full_image_name
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to build image: {e}")
            raise
    finally:
        if dockerfile_path.exists():
            try:
                dockerfile_path.unlink()
                logger.debug(f"Cleaned up temporary file: {dockerfile_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary file {dockerfile_path}: {e}")

        if dockerignore_path.exists():
            try:
                dockerignore_path.unlink()
                logger.debug(f"Cleaned up temporary file: {dockerignore_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary file {dockerignore_path}: {e}")

        try:
            docker_dir.rmdir()
            logger.debug(f"Removed empty directory: {docker_dir}")
        except Exception:
            pass


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


def run_command(connector_dir: Path, tag: str, no_verify: bool, verbose: bool) -> int:
    """Run the build command with the given arguments.

    Args:
        connector_dir: Path to the connector directory.
        tag: Tag to apply to the built image.
        no_verify: Whether to skip verification of the built image.
        verbose: Whether to enable verbose logging.

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    try:
        set_up_logging(verbose)

        if not verify_docker_installation():
            logger.error(
                "Docker is not installed or not running. Please install Docker and try again."
            )
            return 1

        connector_dir = connector_dir.absolute()

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

        try:
            platforms = "linux/amd64,linux/arm64"
            logger.info(f"Building for platforms: {platforms}")

            if metadata.connectorBuildOptions and metadata.connectorBuildOptions.baseImage:
                image_name = build_from_base_image(connector_dir, metadata, tag, platforms)
            else:
                image_name = build_from_dockerfile(connector_dir, metadata, tag, platforms)

            if not no_verify:
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
        if verbose:
            import traceback

            logger.error(traceback.format_exc())
        return 1


def run() -> None:
    """Entry point for the airbyte-cdk build command."""
    import argparse

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
        run_command(
            connector_dir=Path(args.connector_dir),
            tag=args.tag,
            no_verify=args.no_verify,
            verbose=args.verbose,
        )
    )


if __name__ == "__main__":
    run()
