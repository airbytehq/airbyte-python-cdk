# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from dataclasses import dataclass
from typing import Any


@dataclass
class AirbyteFileTransferRecordMessage:
    stream: str
    file: dict[str, Any]
    emitted_at: int
    namespace: str | None = None
    data: dict[str, Any] | None = None
