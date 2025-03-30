# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from dataclasses import InitVar, dataclass
from typing import Any, List, Mapping, Union

from airbyte_cdk.sources.types import Config, Record


@dataclass
class GroupByKey:
    """
    tbd
    """

    key: Union[str, List[str]]
    parameters: InitVar[Mapping[str, Any]]
    config: Config

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._keys = [self.key] if isinstance(self.key, str) else self.key

    def get_group_key(self, record: Record) -> str:
        resolved_keys = [str(record.data.get(key)) for key in self._keys]
        return ",".join(resolved_keys)
