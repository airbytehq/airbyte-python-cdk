#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
from dataclasses import InitVar, dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable, Mapping, MutableMapping, Optional, Sequence

import dpath

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config, Record, StreamSlice

if TYPE_CHECKING:
    from airbyte_cdk.sources.declarative.retrievers import Retriever


class OnNoRecords(Enum):
    """
    Behavior when record expansion produces no records.
    """

    skip = "skip"
    emit_parent = "emit_parent"


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
        truncation_indicator_path: Path within each record to a field indicating that the
            embedded nested list is truncated (e.g. a `has_more` flag on the list object).
        truncated_list_retriever: Retriever used to fetch the complete list of items when
            the field at `truncation_indicator_path` is truthy. The record being expanded is
            exposed to the retriever's interpolation context as `stream_slice['parent_record']`.
            If the retriever returns no records, the embedded items are expanded as a fallback.
        config: The user-provided configuration as specified by the source's spec.
    """

    expand_records_from_field: Sequence[str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    remain_original_record: bool = False
    on_no_records: OnNoRecords = OnNoRecords.skip
    truncation_indicator_path: Optional[Sequence[str]] = None
    truncated_list_retriever: Optional["Retriever"] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._expand_path: list[InterpolatedString] = [
            InterpolatedString.create(path, parameters=parameters)
            for path in self.expand_records_from_field
        ]
        if self.truncated_list_retriever and not self.truncation_indicator_path:
            raise ValueError(
                "`truncation_indicator_path` is required when `truncated_list_retriever` is configured."
            )
        if self.truncated_list_retriever and any(
            path == "*"
            for path in (*self.expand_records_from_field, *(self.truncation_indicator_path or []))
        ):
            raise ValueError(
                "The '*' wildcard is not supported in `expand_records_from_field` or `truncation_indicator_path` when truncation handling is configured."
            )
        self._truncation_indicator_path: list[InterpolatedString] = [
            InterpolatedString.create(path, parameters=parameters)
            for path in (self.truncation_indicator_path or [])
        ]

    def expand_record(self, record: MutableMapping[Any, Any]) -> Iterable[MutableMapping[Any, Any]]:
        """Expand a record by extracting items from a nested array field."""
        if not isinstance(record, Mapping):
            # If the input isn't a mapping, expansion can't proceed; yield as-is.
            yield record
            return

        if not self._expand_path:
            yield record
            return

        parent_record = record

        if self.truncated_list_retriever and self._is_truncated(parent_record):
            fetched_records = iter(self._fetch_complete_list(parent_record))
            first_fetched = next(fetched_records, None)
            if first_fetched is not None:
                yield first_fetched
                yield from fetched_records
                return

        expand_path = [path.eval(self.config) for path in self._expand_path]
        expanded_any = False

        try:
            extracted_values = dpath.values(parent_record, expand_path)
        except KeyError:
            extracted_values = []

        for extracted in extracted_values:
            if not isinstance(extracted, list):
                continue
            items = extracted
            for item in items:
                if isinstance(item, dict):
                    expanded_record = dict(item)
                    self._apply_parent_context(parent_record, expanded_record)
                    yield expanded_record
                    expanded_any = True
                else:
                    if self.remain_original_record:
                        yield {
                            "value": item,
                            "original_record": copy.deepcopy(parent_record),
                        }
                    else:
                        yield item
                    expanded_any = True

        if not expanded_any and self.on_no_records == OnNoRecords.emit_parent:
            yield parent_record

    def _is_truncated(self, parent_record: MutableMapping[Any, Any]) -> bool:
        indicator_path = [path.eval(self.config) for path in self._truncation_indicator_path]
        try:
            return bool(dpath.get(parent_record, indicator_path))
        except KeyError:
            return False

    def _fetch_complete_list(
        self, parent_record: Mapping[str, Any]
    ) -> Iterable[MutableMapping[str, Any]]:
        if not self.truncated_list_retriever:
            return
        stream_slice = StreamSlice(partition={"parent_record": parent_record}, cursor_slice={})
        for item in self.truncated_list_retriever.read_records(
            records_schema={}, stream_slice=stream_slice
        ):
            data = item.data if isinstance(item, Record) else item
            if not isinstance(data, Mapping):
                continue
            expanded_record = dict(data)
            self._apply_parent_context(parent_record, expanded_record)
            yield expanded_record

    def _apply_parent_context(
        self, parent_record: Mapping[str, Any], child_record: MutableMapping[str, Any]
    ) -> None:
        """Apply parent context to a child record."""
        if self.remain_original_record:
            child_record["original_record"] = copy.deepcopy(parent_record)
