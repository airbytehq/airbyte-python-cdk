#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import uuid
from datetime import datetime
from enum import Enum

from pydantic.v1 import BaseModel


class RemoteFileIdentityType(Enum):
    USER = "user"
    GROUP = "group"


class RemoteFileIdentity(BaseModel):
    id: uuid.UUID
    remote_id: str
    parent_id: str | None = None
    name: str | None = None
    description: str | None = None
    email_address: str | None = None
    member_email_addresses: list[str] | None = None
    type: RemoteFileIdentityType
    modified_at: datetime


class RemoteFilePermissions(BaseModel):
    id: str
    file_path: str
    allowed_identity_remote_ids: list[str] | None = None
    denied_identity_remote_ids: list[str] | None = None
    publicly_accessible: bool = False
