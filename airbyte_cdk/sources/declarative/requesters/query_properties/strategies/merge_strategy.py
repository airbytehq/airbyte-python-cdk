# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from airbyte_cdk.sources.types import Record


@dataclass
class RecordMergeStrategy(ABC):
    """
    Describe the interface for how records that required multiple requests to get the complete set of fields
    should be merged back into a single record.
    """

    @abstractmethod
    def get_group_key(self, record: Record) -> Optional[str]:
        pass
