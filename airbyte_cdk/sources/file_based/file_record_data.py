#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from datetime import datetime
from typing import Optional

from pydantic.v1 import BaseModel


class FileRecordData(BaseModel):
    """
    A record in a file-based stream.
    """

    folder: str
    filename: str
    bytes: int

    id: Optional[str] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    mime_type: Optional[str] = None
    description: Optional[str] = None
