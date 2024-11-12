#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

# ruff: noqa: TCH003  # Don'e move types to TYPE_CHECKING blocks. Pydantic needs them at runtime.
from datetime import datetime

from pydantic.v1 import BaseModel


class RemoteFile(BaseModel):
    """A file in a file-based stream."""

    uri: str
    last_modified: datetime
    mime_type: str | None = None
