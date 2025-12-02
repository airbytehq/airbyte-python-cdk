#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import dpath

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config


@dataclass
class RecordExpander:
    """
    Expands records by extracting items from a nested array field.

    When configured, this component extracts items from a specified nested array path
    within each record and emits each item as a separate record. Optionally, the original
    parent record can be embedded in each expanded item for context preservation.

    Examples of instantiating this component:
    ```
      record_expander:
        type: RecordExpander
        expand_records_from_field:
          - "lines"
          - "data"
        remain_original_record: true
    ```

    Attributes:
        expand_records_from_field (List[Union[InterpolatedString, str]]): Path to a nested array field within each record. Items from this array will be extracted and emitted as separate records.
        remain_original_record (bool): If True, each expanded record will include the original parent record in an "original_record" field. Defaults to False.
        config (Config): The user-provided configuration as specified by the source's spec
    """

    expand_records_from_field: List[Union[InterpolatedString, str]]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    remain_original_record: bool = False

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._expand_path: Optional[List[InterpolatedString]] = [
            InterpolatedString.create(path, parameters=parameters)
            for path in self.expand_records_from_field
        ]

    def expand_record(self, record: MutableMapping[Any, Any]) -> Iterable[MutableMapping[Any, Any]]:
        """Expand a record by extracting items from a nested array field."""
        if not self._expand_path:
            yield record
            return

        expand_path = [path.eval(self.config) for path in self._expand_path]

        try:
            nested_array = dpath.get(record, expand_path)
        except (KeyError, TypeError):
            yield record
            return

        if not isinstance(nested_array, list):
            yield record
            return

        if len(nested_array) == 0:
            return

        for item in nested_array:
            if isinstance(item, dict):
                expanded_record = dict(item)
                if self.remain_original_record:
                    expanded_record["original_record"] = record
                yield expanded_record
            else:
                yield item
