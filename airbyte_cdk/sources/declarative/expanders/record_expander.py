#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Iterable, Mapping, MutableMapping

import dpath

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config


@dataclass
class RecordExpander:
    """Expands records by extracting items from a nested array field.

    When configured, this component extracts items from a specified nested array path
    within each record and emits each item as a separate record. Set `remain_original_record: true`
    to embed the full parent record under `original_record` in each expanded item when you need
    downstream transformations to access parent context.

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
        on_no_records: emit_parent
    ```

    Attributes:
        expand_records_from_field: Path to a nested array field within each record.
            Items from this array will be extracted and emitted as separate records.
            Supports wildcards (*).
        remain_original_record: If True, each expanded record will include the original
            parent record in an "original_record" field. Defaults to False.
        on_no_records: Behavior when expansion produces no records. "skip" (default)
            emits nothing. "emit_parent" emits the original parent record unchanged.
        config: The user-provided configuration as specified by the source's spec.
    """

    expand_records_from_field: Sequence[str | InterpolatedString]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    remain_original_record: bool = False
    on_no_records: str = "skip"

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._expand_path: list[InterpolatedString] | None = [
            InterpolatedString.create(path, parameters=parameters)
            for path in self.expand_records_from_field
        ]

    def expand_record(self, record: MutableMapping[Any, Any]) -> Iterable[MutableMapping[Any, Any]]:
        """Expand a record by extracting items from a nested array field."""
        if not self._expand_path:
            yield record
            return

        parent_record = record
        expand_path = [path.eval(self.config) for path in self._expand_path]
        expanded_any = False

        if "*" in expand_path:
            extracted: Any = dpath.values(parent_record, expand_path)
            for record in extracted:
                if isinstance(record, list):
                    for item in record:
                        if isinstance(item, dict):
                            expanded_record = dict(item)
                            self._apply_parent_context(parent_record, expanded_record)
                            yield expanded_record
                            expanded_any = True
                        else:
                            yield item
                            expanded_any = True
        else:
            try:
                extracted = dpath.get(parent_record, expand_path)
            except KeyError:
                extracted = None

            if isinstance(extracted, list):
                for item in extracted:
                    if isinstance(item, dict):
                        expanded_record = dict(item)
                        self._apply_parent_context(parent_record, expanded_record)
                        yield expanded_record
                        expanded_any = True
                    else:
                        yield item
                        expanded_any = True

        if not expanded_any and self.on_no_records == "emit_parent":
            yield parent_record

    def _apply_parent_context(
        self, parent_record: Mapping[str, Any], child_record: MutableMapping[str, Any]
    ) -> None:
        """Apply parent context to a child record."""
        if self.remain_original_record:
            child_record["original_record"] = parent_record
