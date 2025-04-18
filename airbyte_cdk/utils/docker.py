"""Docker build utilities for Airbyte CDK."""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from pathlib import Path

from airbyte_cdk.models.connector_metadata import ConnectorMetadata, MetadataFile

logger = logging.getLogger(__name__)


def run_docker_command(cmd: list[str], check: bool = True) -> tuple[int, str, str]:
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
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
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


def cleanup_temp_files(connector_dir: Path) -> None:
    """Clean up temporary build files from the connector directory.

    Args:
        connector_dir: Path to the connector directory.
    """
    temp_files = ["Dockerfile.temp", ".dockerignore.temp"]
    for temp_file in temp_files:
        temp_file_path = connector_dir / temp_file
        if temp_file_path.exists():
            try:
                temp_file_path.unlink()
                logger.debug(f"Cleaned up temporary file: {temp_file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary file {temp_file_path}: {e}")


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
        "buildx",
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
    metadata: MetadataFile,
    tag: str,
    platform: str,
) -> str:
    """Build a Docker image for the connector using a base image.

    This implementation follows a similar approach to airbyte-ci:
    1. Creates a builder stage to install dependencies
    2. Copies only necessary build files first
    3. Installs dependencies
    4. Copies the connector code
    5. Sets up the entrypoint

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
    if not metadata.data.connectorBuildOptions or not metadata.data.connectorBuildOptions.baseImage:
        raise ValueError("Base image not specified in metadata.connectorBuildOptions.baseImage")

    base_image = metadata.data.connectorBuildOptions.baseImage
    image_name = metadata.data.dockerRepository
    full_image_name = f"{image_name}:{tag}"

    logger.info(
        f"Building Docker image from base image {base_image}: {full_image_name} for platform {platform}"
    )

    build_dir = connector_dir / "build" / "docker"
    build_dir.mkdir(parents=True, exist_ok=True)

    dockerfile_temp_path = build_dir / "Dockerfile"
    dockerignore_temp_path = build_dir / ".dockerignore"

    try:
        try:
            execute_build_customization_hooks(connector_dir, "pre_connector_install")
        except subprocess.CalledProcessError:
            logger.warning("Pre-install hook failed, continuing with build")

        build_files = [
            "setup.py",
            "pyproject.toml",
            "poetry.lock",
            "poetry.toml",
            "requirements.txt",
            "README.md",
            "build_customization.py",
        ]

        connector_package_name = connector_dir.name.replace("-", "_")

        dockerfile_content = f"""
FROM {base_image} as builder

WORKDIR /airbyte/integration_code

COPY setup.py pyproject.toml poetry.lock poetry.toml requirements.txt README.md build_customization.py ./
COPY {connector_package_name} ./{connector_package_name}

RUN pip install --no-cache-dir .

FROM {base_image}

WORKDIR /airbyte/integration_code

COPY --from=builder /usr/local /usr/local

COPY . .

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/{get_main_file_name(connector_dir)}"
ENTRYPOINT ["python", "/airbyte/integration_code/{get_main_file_name(connector_dir)}"]
"""

        dockerfile_temp_path.write_text(dockerfile_content)
        logger.debug(f"Created temporary Dockerfile at {dockerfile_temp_path}")

        dockerignore_content = """
*

!setup.py
!pyproject.toml
!poetry.lock
!poetry.toml
!requirements.txt
!README.md
!build_customization.py
!unit_tests
!integration_tests
!acceptance_tests
!src
!main.py
!source.py
!destination.py
!source_*
!destination_*
"""
        dockerignore_temp_path.write_text(dockerignore_content)
        logger.debug(f"Created temporary .dockerignore at {dockerignore_temp_path}")

        root_dockerignore_path = connector_dir / ".dockerignore"
        original_dockerignore = None
        if root_dockerignore_path.exists():
            original_dockerignore = root_dockerignore_path.read_text()
            logger.debug(f"Backing up original .dockerignore at {root_dockerignore_path}")

        root_dockerignore_path.write_text(dockerignore_content)
        logger.debug(f"Temporarily replaced .dockerignore at {root_dockerignore_path}")

        try:
            execute_build_customization_hooks(connector_dir, "post_connector_install")
        except subprocess.CalledProcessError:
            logger.warning("Post-install hook failed, continuing with build")

        build_cmd = [
            "docker",
            "buildx",
            "build",
            "--platform",
            platform,
            "-t",
            full_image_name,
            "--label",
            f"io.airbyte.version={metadata.data.dockerImageTag}",
            "--label",
            f"io.airbyte.name={metadata.data.dockerRepository}",
            "-f",
            str(dockerfile_temp_path),
            str(connector_dir),
        ]

        try:
            run_docker_command(build_cmd)
            logger.info(f"Successfully built image: {full_image_name}")
            return full_image_name
        finally:
            if original_dockerignore is not None:
                root_dockerignore_path.write_text(original_dockerignore)
                logger.debug(f"Restored original .dockerignore at {root_dockerignore_path}")
            elif root_dockerignore_path.exists():
                root_dockerignore_path.unlink()
                logger.debug(f"Removed temporary .dockerignore at {root_dockerignore_path}")

    except Exception as e:
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
