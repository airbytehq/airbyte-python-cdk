"""Test containers."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any, Mapping

from airbyte_cdk.connector import BaseConnector
from airbyte_cdk.test.standard_tests._job_runner import IConnector
from airbyte_cdk.utils.docker import build_connector_image, run_docker_command


class CliConnector(IConnector):
    """CLI connector class."""

    def __init__(
        self,
        *,
        connector_name: str,
        logger: logging.Logger,
    ) -> None:
        self.connector_name = connector_name
        self.default_logger = logger or logging.getLogger(__name__)

    @staticmethod
    def read_config(config_path: str) -> Mapping[str, Any]:
        config = BaseConnector._read_json_file(config_path)
        if isinstance(config, Mapping):
            return config
        else:
            raise ValueError(
                f"The content of {config_path} is not an object and therefore is not a valid config. Please ensure the file represent a config."
            )

    def spec(
        self,
        logger: logging.Logger,
    ) -> Any:
        """Run `spec` command."""
        self.launch(
            ["spec"],
            logger=logger or self.default_logger,
        )

    def check(
        self,
        logger: logging.Logger,
        config: dict[str, Any] | Path,
    ) -> None:
        """Run the `check` command."""
        self.launch(
            ["check"],
            logger=logger,
        )

    def _run_cli(
        self,
        args: list[str],
        logger: logging.Logger,
    ) -> None:
        """Run the CLI command."""
        logger.info(f"Running CLI connector: {self.connector_name} with args: {args}")
        base_cmd: list[str] = [
            self.connector_name,
            *args,
        ]
        subprocess.run(
            base_cmd,
            check=True,
        )

    def launch(
        self,
        args: list[str],
        *,
        logger: logging.Logger,
    ) -> None:
        """Run the connector."""
        logger = logger or self.default_logger
        self._run_cli(
            [self.connector_name, *args],
            logger=logger,
        )


class DockerConnector(CliConnector):
    """Docker connector class."""

    def __init__(
        self,
        *,
        connector_name: str,
        docker_image: str,
        logger: logging.Logger | None = None,
    ) -> None:
        self.docker_image = docker_image
        super().__init__(
            connector_name=connector_name,
            logger=logger,
        )

    def launch(
        self,
        args: list[str],
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        """Run the connector."""
        _ = logger
        print(f"Running docker connector: {self.connector_name} with args: {args}")
        docker_base_cmd: list[str] = [
            "docker",
            "run",
            "--rm",
            "--network=host",
        ]
        run_docker_command(
            cmd=[
                *docker_base_cmd,
                self.docker_image,
                *args,
            ],
        )

    @classmethod
    def from_connector_directory(
        cls,
        connector_directory: Path,
        *,
        logger: logging.Logger | None = None,
    ) -> DockerConnector:
        """Create a new Docker connector."""
        connector_name = connector_directory.name
        docker_image = build_connector_image(
            connector_name=connector_name,
            connector_directory=connector_directory,
            tag="dev",
        )
        return cls(
            connector_name=connector_name,
            docker_image=docker_image,
            logger=logger,
        )
