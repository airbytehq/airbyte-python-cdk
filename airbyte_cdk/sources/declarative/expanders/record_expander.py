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

    The expand_records_from_field path supports wildcards (*) for matching multiple arrays.
    When wildcards are used, items from all matched arrays are extracted and emitted.

    Examples of instantiating this component:
    ```
      record_expander:
        type: RecordExpander
        expand_records_from_field:
          - "lines"
          - "data"
        remain_original_record: true
    ```

    ```
      record_expander:
        type: RecordExpander
        expand_records_from_field:
          - "sections"
          - "*"
          - "items"
        remain_original_record: false
    ```

    Attributes:
        expand_records_from_field (List[Union[InterpolatedString, str]]): Path to a nested array field within each record. Items from this array will be extracted and emitted as separate records. Supports wildcards (*).
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

        if "*" in expand_path:
            matches = dpath.values(record, expand_path)
            list_nodes = [m for m in matches if isinstance(m, list)]
            if not list_nodes:
                return

            for nested_array in list_nodes:
                if len(nested_array) == 0:
                    continue
                for item in nested_array:
                    if isinstance(item, dict):
                        expanded_record = dict(item)
                        if self.remain_original_record:
                            expanded_record["original_record"] = record
                        yield expanded_record
                    else:
                        yield item
        else:
            try:
                nested_array = dpath.get(record, expand_path)
            except KeyError:
                return

            if not isinstance(nested_array, list):
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
