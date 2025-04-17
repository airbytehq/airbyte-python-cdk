"""Models for connector metadata."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


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

    baseImage: Optional[str] = Field(
        None, description="Base image to use for building the connector"
    )
    path: Optional[str] = Field(
        None, description="Path to the connector code within the repository"
    )


class ConnectorMetadata(BaseModel):
    """Connector metadata from metadata.yaml."""

    model_config = {"extra": "allow"}

    dockerRepository: str = Field(..., description="Docker repository for the connector image")
    dockerImageTag: str = Field(..., description="Docker image tag for the connector")
    language: Optional[ConnectorLanguage] = Field(
        None, description="Language of the connector implementation"
    )
    connectorBuildOptions: Optional[ConnectorBuildOptions] = Field(
        None, description="Options for building the connector"
    )


class MetadataFile(BaseModel):
    """Represents the structure of a metadata.yaml file."""

    model_config = {"extra": "allow"}

    data: ConnectorMetadata = Field(..., description="Connector metadata")
