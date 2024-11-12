#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Mapping


class AbstractSchemaValidationPolicy(ABC):
    name: str
    validate_schema_before_sync = False  # Whether to verify that records conform to the schema during the stream's availabilty check

    @abstractmethod
    def record_passes_validation_policy(
        self, record: Mapping[str, Any], schema: Mapping[str, Any] | None
    ) -> bool:
        """Return True if the record passes the user's validation policy."""
        raise NotImplementedError
