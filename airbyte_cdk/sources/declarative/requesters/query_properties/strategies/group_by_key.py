# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from dataclasses import InitVar, dataclass
from typing import Any, List, Mapping, Optional, Union

from airbyte_cdk.sources.declarative.requesters.query_properties.strategies.merge_strategy import (
    RecordMergeStrategy,
)
from airbyte_cdk.sources.types import Config, Record


@dataclass
class GroupByKey(RecordMergeStrategy):
    """
    Record merge strategy that combines records together according to values on the record for one or many keys.
    """

    key: Union[str, List[str]]
    parameters: InitVar[Mapping[str, Any]]
    config: Config

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._keys = [self.key] if isinstance(self.key, str) else self.key

    def get_group_key(self, record: Record) -> Optional[str]:
        resolved_keys = []
        for key in self._keys:
            key_value = record.data.get(key)
            if key_value:
                resolved_keys.append(key_value)
            else:
                return None
        return ",".join(resolved_keys)
