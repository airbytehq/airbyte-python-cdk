"""Models to represent the structure of a `metadata.yaml` file."""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, ValidationError


class ConnectorLanguage(str, Enum):
    """Connector implementation language."""

    PYTHON = "python"
    JAVA = "java"
    LOW_CODE = "low-code"
    MANIFEST_ONLY = "manifest-only"
    UNKNOWN = "unknown"


class ConnectorBuildOptions(BaseModel):
    """Connector build options from metadata.yaml."""

    model_config = {"extra": "allow"}

    baseImage: str | None = Field(
        None,
        description="Base image to use for building the connector",
    )
    path: str | None = Field(
        None,
        description="Path to the connector code within the repository",
    )


class SuggestedStreams(BaseModel):
    """Suggested streams from metadata.yaml."""

    streams: list[str] = Field(
        default=[],
        description="List of suggested streams for the connector",
    )


class ConnectorMetadata(BaseModel):
    """Connector metadata from metadata.yaml."""

    model_config = {"extra": "allow"}

    dockerRepository: str = Field(..., description="Docker repository for the connector image")
    dockerImageTag: str = Field(..., description="Docker image tag for the connector")

    tags: list[str] = Field(
        default=[],
        description="List of tags for the connector",
    )

    suggestedStreams: SuggestedStreams | None = Field(
        default=None,
        description="Suggested streams for the connector",
    )

    @property
    def language(self) -> ConnectorLanguage:
        """Get the connector language."""
        for tag in self.tags:
            if tag.startswith("language:"):
                language = tag.split(":", 1)[1]
                if language == "python":
                    return ConnectorLanguage.PYTHON
                elif language == "java":
                    return ConnectorLanguage.JAVA
                elif language == "low-code":
                    return ConnectorLanguage.LOW_CODE
                elif language == "manifest-only":
                    return ConnectorLanguage.MANIFEST_ONLY

        return ConnectorLanguage.UNKNOWN

    connectorBuildOptions: ConnectorBuildOptions | None = Field(
        None, description="Options for building the connector"
    )


class MetadataFile(BaseModel):
    """Represents the structure of a metadata.yaml file."""

    model_config = {"extra": "allow"}

    data: ConnectorMetadata = Field(..., description="Connector metadata")

    @classmethod
    def from_file(
        cls,
        file_path: Path,
    ) -> MetadataFile:
        """Load metadata from a YAML file."""
        if not file_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {file_path!s}")

        metadata_content = file_path.read_text()
        metadata_dict = yaml.safe_load(metadata_content)

        if not metadata_dict or "data" not in metadata_dict:
            raise ValueError(
                "Invalid metadata format: missing 'data' field in YAML file '{file_path!s}'"
            )

        metadata_file = MetadataFile.model_validate(metadata_dict)
        return metadata_file


class ValidationResult(BaseModel):
    """Result of metadata validation."""

    valid: bool = Field(..., description="Whether the metadata is valid")
    errors: list[dict[str, Any]] = Field(
        default_factory=list, description="List of validation errors"
    )
    metadata: dict[str, Any] | None = Field(None, description="Parsed metadata if available")


def validate_metadata_file(file_path: Path) -> ValidationResult:
    """Validate a metadata.yaml file.

    Args:
        file_path: Path to the metadata.yaml file to validate

    Returns:
        ValidationResult with validation status, errors, and parsed metadata
    """
    errors = []
    metadata_dict = None

    try:
        if not file_path.exists():
            return ValidationResult(
                valid=False,
                errors=[
                    {"type": "file_not_found", "message": f"Metadata file not found: {file_path}"}
                ],
                metadata=None,
            )

        try:
            metadata_content = file_path.read_text()
            metadata_dict = yaml.safe_load(metadata_content)
        except yaml.YAMLError as e:
            return ValidationResult(
                valid=False,
                errors=[{"type": "yaml_parse_error", "message": f"Failed to parse YAML: {e}"}],
                metadata=None,
            )

        if not metadata_dict or "data" not in metadata_dict:
            return ValidationResult(
                valid=False,
                errors=[
                    {
                        "type": "missing_field",
                        "path": "data",
                        "message": "Missing 'data' field in metadata",
                    }
                ],
                metadata=metadata_dict,
            )

        try:
            metadata_file = MetadataFile.model_validate(metadata_dict)
            return ValidationResult(
                valid=True,
                errors=[],
                metadata=metadata_dict,
            )
        except ValidationError as e:
            for error in e.errors():
                errors.append(
                    {
                        "type": error["type"],
                        "path": ".".join(str(loc) for loc in error["loc"]),
                        "message": error["msg"],
                    }
                )

            return ValidationResult(
                valid=False,
                errors=errors,
                metadata=metadata_dict,
            )

    except Exception as e:
        return ValidationResult(
            valid=False,
            errors=[{"type": "unexpected_error", "message": f"Unexpected error: {e}"}],
            metadata=metadata_dict,
        )
