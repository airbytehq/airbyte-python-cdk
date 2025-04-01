# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional

from airbyte_cdk.sources.declarative.requesters.query_properties.strategies.merge_strategy import (
    RecordMergeStrategy,
)
from airbyte_cdk.sources.types import Config, Record


@dataclass
class EmitPartialRecord(RecordMergeStrategy):
    """
    Record merge strategy that emits partial records as they are without merging them together usually if
    there is not a suitable primary key to merge on.
    """

    parameters: InitVar[Mapping[str, Any]]
    config: Config

    def get_group_key(self, record: Record) -> Optional[str]:
        return None
