#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass, field
from typing import Any, Iterable, Mapping, MutableMapping

import dpath

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config


@dataclass
class ParentFieldMapping:
    """Defines a mapping from a parent record field to a child record field."""

    source_field_path: list[str | InterpolatedString]
    target_field: str
    config: Config
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._source_path = [
            InterpolatedString.create(path, parameters=parameters)
            for path in self.source_field_path
        ]

    def copy_field(
        self, parent_record: Mapping[str, Any], child_record: MutableMapping[str, Any]
    ) -> None:
        """Copy a field from parent record to child record."""
        source_path = [path.eval(self.config) for path in self._source_path]
        try:
            value = dpath.get(dict(parent_record), source_path)
            child_record[self.target_field] = value
        except KeyError:
            pass


@dataclass
class RecordExpander:
    """Expands records by extracting items from a nested array field.

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
        on_no_records: emit_parent
        parent_fields_to_copy:
          - type: ParentFieldMapping
            source_field_path: ["id"]
            target_field: "parent_id"
    ```

    Attributes:
        expand_records_from_field: Path to a nested array field within each record.
            Items from this array will be extracted and emitted as separate records.
            Supports wildcards (*).
        remain_original_record: If True, each expanded record will include the original
            parent record in an "original_record" field. Defaults to False.
        on_no_records: Behavior when expansion produces no records. "skip" (default)
            emits nothing. "emit_parent" emits the original parent record unchanged.
        parent_fields_to_copy: List of field mappings to copy from parent to each
            expanded child record.
        config: The user-provided configuration as specified by the source's spec.
    """

    expand_records_from_field: list[str | InterpolatedString]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    remain_original_record: bool = False
    on_no_records: str = "skip"
    parent_fields_to_copy: list[ParentFieldMapping] = field(default_factory=list)

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

        for field_mapping in self.parent_fields_to_copy:
            field_mapping.copy_field(parent_record, child_record)
