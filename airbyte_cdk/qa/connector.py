"""Connector utilities for QA checks."""

import json
import os
import re
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml


class ConnectorLanguage(Enum):
    """The programming language of a connector."""

    PYTHON = "python"
    JAVA = "java"
    LOW_CODE = "low-code"
    MANIFEST_ONLY = "manifest-only"


class ConnectorLanguageError(Exception):
    """Raised when the connector language cannot be determined."""

    pass


class Connector:
    """A class representing an Airbyte connector.

    This is a simplified version of the Connector class from connector_ops.utils,
    with only the functionality needed for QA checks.
    """

    def __init__(self, technical_name: str, connector_directory: Optional[Path] = None):
        """Initialize a Connector instance.

        Args:
            technical_name: The technical name of the connector (e.g. "source-postgres")
            connector_directory: The directory containing the connector code. If not provided,
                it will be inferred from the technical name.
        """
        self._technical_name = technical_name
        self._connector_directory = connector_directory
        self._metadata_cache: Optional[Dict[str, Any]] = None
        self._is_released: Optional[bool] = None

    def __repr__(self) -> str:
        """Return a string representation of the connector.

        Returns:
            str: A string representation of the connector
        """
        return self.technical_name

    @property
    def technical_name(self) -> str:
        """The technical name of the connector.

        Returns:
            str: The technical name of the connector
        """
        return self._technical_name

    @property
    def name(self) -> str:
        """The name of the connector.

        Returns:
            str: The name of the connector
        """
        return self.technical_name

    @property
    def connector_type(self) -> str:
        """The type of the connector (source or destination).

        Returns:
            str: The type of the connector
        """
        return self.technical_name.split("-")[0]

    @property
    def is_third_party(self) -> bool:
        """Whether the connector is third-party.

        Returns:
            bool: Whether the connector is third-party
        """
        return False  # Simplified for now

    @property
    def code_directory(self) -> Path:
        """The directory containing the connector code.

        Returns:
            Path: The directory containing the connector code
        """
        if self._connector_directory:
            return self._connector_directory
        
        cwd = Path.cwd()
        if cwd.name == self.technical_name:
            return cwd
        
        for parent in cwd.parents:
            if parent.name == self.technical_name:
                return parent
        
        return cwd

    @property
    def metadata_file_path(self) -> Path:
        """The path to the metadata.yaml file.

        Returns:
            Path: The path to the metadata.yaml file
        """
        return self.code_directory / "metadata.yaml"

    @property
    def metadata(self) -> Optional[Dict[str, Any]]:
        """The metadata from the metadata.yaml file.

        Returns:
            Dict[str, Any]: The metadata from the metadata.yaml file, or None if the file doesn't exist
        """
        if self._metadata_cache is not None:
            return self._metadata_cache

        if not self.metadata_file_path.exists():
            return None

        with open(self.metadata_file_path, "r") as f:
            self._metadata_cache = yaml.safe_load(f)
        return self._metadata_cache

    @property
    def language(self) -> Optional[ConnectorLanguage]:
        """The programming language of the connector.

        Returns:
            ConnectorLanguage: The programming language of the connector
        """
        if Path(self.code_directory / "setup.py").exists() or Path(self.code_directory / "pyproject.toml").exists():
            return ConnectorLanguage.PYTHON
        if Path(self.code_directory / "src" / "main" / "java").exists() or Path(self.code_directory / "src" / "main" / "kotlin").exists():
            return ConnectorLanguage.JAVA
        if self.manifest_path.exists():
            return ConnectorLanguage.LOW_CODE
        return None

    @property
    def manifest_path(self) -> Path:
        """The path to the manifest.yaml file.

        Returns:
            Path: The path to the manifest.yaml file
        """
        return self.code_directory / "manifest.yaml"

    @property
    def version(self) -> Optional[str]:
        """The version of the connector.

        Returns:
            str: The version of the connector, or None if it can't be determined
        """
        if self.metadata:
            docker_image_tag = self.metadata.get("data", {}).get("dockerImageTag")
            return str(docker_image_tag) if docker_image_tag is not None else None
        return None

    @property
    def version_in_dockerfile_label(self) -> Optional[str]:
        """The version of the connector from the Dockerfile label.

        Returns:
            str: The version of the connector from the Dockerfile label, or None if it can't be determined
        """
        dockerfile_path = self.code_directory / "Dockerfile"
        if not dockerfile_path.exists():
            return None

        with open(dockerfile_path, "r") as f:
            dockerfile_content = f.read()

        version_match = re.search(r'LABEL io.airbyte.version="([^"]+)"', dockerfile_content)
        if version_match:
            return version_match.group(1)
        return None

    @property
    def name_from_metadata(self) -> Optional[str]:
        """The name of the connector from the metadata.

        Returns:
            str: The name of the connector from the metadata, or None if it can't be determined
        """
        if self.metadata:
            name = self.metadata.get("data", {}).get("name")
            return str(name) if name is not None else None
        return None

    @property
    def support_level(self) -> Optional[str]:
        """The support level of the connector.

        Returns:
            str: The support level of the connector, or None if it can't be determined
        """
        if self.metadata:
            support_level = self.metadata.get("data", {}).get("supportLevel")
            return str(support_level) if support_level is not None else None
        return None

    @property
    def cloud_usage(self) -> Optional[str]:
        """The cloud usage of the connector.

        Returns:
            str: The cloud usage of the connector, or None if it can't be determined
        """
        if self.metadata:
            cloud_usage = self.metadata.get("data", {}).get("cloudUsage")
            return str(cloud_usage) if cloud_usage is not None else None
        return None

    @property
    def ab_internal_sl(self) -> int:
        """The ab_internal_sl of the connector.

        Returns:
            int: The ab_internal_sl of the connector, or 0 if it can't be determined
        """
        if self.metadata:
            sl_value = self.metadata.get("data", {}).get("ab_internal", {}).get("sl", 0)
            return int(sl_value) if sl_value is not None else 0
        return 0

    @property
    def is_released(self) -> bool:
        """Whether the connector is released.

        Returns:
            bool: Whether the connector is released
        """
        if self._is_released is not None:
            return self._is_released
        
        self._is_released = True
        return True

    @property
    def pyproject_file_path(self) -> Path:
        """The path to the pyproject.toml file.

        Returns:
            Path: The path to the pyproject.toml file
        """
        return self.code_directory / "pyproject.toml"

    @property
    def dockerfile_file_path(self) -> Path:
        """The path to the Dockerfile.

        Returns:
            Path: The path to the Dockerfile
        """
        return self.code_directory / "Dockerfile"

    @property
    def has_dockerfile(self) -> bool:
        """Whether the connector has a Dockerfile.

        Returns:
            bool: Whether the connector has a Dockerfile
        """
        return self.dockerfile_file_path.exists()
