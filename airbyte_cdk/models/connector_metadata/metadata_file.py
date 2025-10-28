"""Models to represent the structure of a `metadata.yaml` file."""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import jsonschema
import yaml
from pydantic import BaseModel, Field, ValidationError

# TODO: Update to master branch URL after associated PR merges
# https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-ci/connectors/metadata_service/lib/metadata_service/models/generated/ConnectorMetadataDefinitionV0.json
DEFAULT_SCHEMA_URL = "https://raw.githubusercontent.com/airbytehq/airbyte/61048d88732df93c50bd3da490de8d3cc1aa66b0/airbyte-ci/connectors/metadata_service/lib/metadata_service/models/generated/ConnectorMetadataDefinitionV0.json"


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


def get_metadata_schema(schema_source: str | Path | None = None) -> dict[str, Any]:
    """Load metadata JSON schema from URL or file path.

    Args:
        schema_source: URL or file path to JSON schema. If None, uses DEFAULT_SCHEMA_URL.

    Returns:
        Parsed JSON schema as dictionary
    """
    if schema_source is None:
        schema_source = DEFAULT_SCHEMA_URL

    if isinstance(schema_source, Path) or (
        isinstance(schema_source, str) and not schema_source.startswith(("http://", "https://"))
    ):
        schema_path = Path(schema_source)
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        return json.loads(schema_path.read_text())

    try:
        with urlopen(schema_source, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"Failed to fetch schema from {schema_source}: {e}") from e


def validate_metadata_file(
    file_path: Path, schema_source: str | Path | None = None
) -> ValidationResult:
    """Validate a metadata.yaml file against JSON schema.

    Args:
        file_path: Path to the metadata.yaml file to validate
        schema_source: URL or file path to JSON schema. If None, uses DEFAULT_SCHEMA_URL.

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
            schema = get_metadata_schema(schema_source)
        except Exception as e:
            return ValidationResult(
                valid=False,
                errors=[{"type": "schema_load_error", "message": f"Failed to load schema: {e}"}],
                metadata=metadata_dict,
            )

        try:
            jsonschema.validate(instance=metadata_dict, schema=schema)
            return ValidationResult(
                valid=True,
                errors=[],
                metadata=metadata_dict,
            )
        except jsonschema.ValidationError as e:
            errors.append(
                {
                    "type": "validation_error",
                    "path": ".".join(str(p) for p in e.absolute_path) if e.absolute_path else "",
                    "message": e.message,
                }
            )
            return ValidationResult(
                valid=False,
                errors=errors,
                metadata=metadata_dict,
            )
        except jsonschema.SchemaError as e:
            return ValidationResult(
                valid=False,
                errors=[{"type": "schema_error", "message": f"Invalid schema: {e.message}"}],
                metadata=metadata_dict,
            )

    except Exception as e:
        return ValidationResult(
            valid=False,
            errors=[{"type": "unexpected_error", "message": f"Unexpected error: {e}"}],
            metadata=metadata_dict,
        )
