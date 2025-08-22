"""
Manifest-related API models.

These models define the request and response structures for manifest operations
like reading, resolving, and full resolution.
"""

from typing import Any, List, Optional

from pydantic import BaseModel, Field

from .dicts import ConnectorConfig, Manifest


class StreamTestReadRequest(BaseModel):
    """Request to test read from a specific stream."""

    manifest: Manifest
    config: ConnectorConfig
    stream_name: str
    state: List[Any] = []
    custom_components_code: Optional[str] = None
    record_limit: int = Field(default=100, ge=1, le=5000)
    page_limit: int = Field(default=5, ge=1, le=20)
    slice_limit: int = Field(default=5, ge=1, le=20)


class ResolveRequest(BaseModel):
    """Request to resolve a manifest."""

    manifest: Manifest


class ManifestResponse(BaseModel):
    """Response containing a manifest."""

    manifest: Manifest


class FullResolveRequest(BaseModel):
    """Request to fully resolve a manifest."""

    manifest: Manifest
    config: ConnectorConfig
    stream_limit: int = Field(default=100, ge=1, le=100)
