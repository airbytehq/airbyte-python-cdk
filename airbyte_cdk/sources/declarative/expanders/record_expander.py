#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
import logging
import threading
from dataclasses import InitVar, dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable, Mapping, MutableMapping, Optional, Sequence

import dpath

from airbyte_cdk.models import (
    AirbyteControlMessage,
    AirbyteLogMessage,
    AirbyteMessage,
    AirbyteStateMessage,
    AirbyteTraceMessage,
    Level,
)
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.types import Config, Record, StreamSlice

if TYPE_CHECKING:
    from airbyte_cdk.sources.declarative.retrievers import Retriever

logger = logging.getLogger("airbyte")

# dpath treats these characters as glob metacharacters (fnmatch semantics) inside a path segment.
_GLOB_METACHARACTERS = ("*", "?", "[")

# Protocol payloads a retriever may yield unwrapped (i.e. not inside an `AirbyteMessage` envelope).
_BARE_PROTOCOL_MESSAGES = (
    AirbyteControlMessage,
    AirbyteLogMessage,
    AirbyteStateMessage,
    AirbyteTraceMessage,
)


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
            When the indicator is truthy and no `truncated_list_retriever` is configured, the
            embedded items are expanded as normal and a WARNING is logged (once per stream
            instance) describing the expansion path and the embedded item count, so that the
            data loss is visible instead of silent. Glob metacharacters (`*`, `?`, `[`) are
            rejected in this path, and in `expand_records_from_field` when a retriever is
            configured; the check runs on the interpolated values.
        truncated_list_retriever: Retriever used to fetch the complete list of items when
            the field at `truncation_indicator_path` is truthy. The record being expanded is
            exposed to the retriever's interpolation context as `stream_slice['parent_record']`.
            One fetch is issued per truncated parent record; enable `use_cache` on its requester
            when the same list can be fetched repeatedly. Without a `paginator` only the first
            page is read. If the retriever returns no records, the embedded items are expanded
            as a fallback; if it returns fewer records than the `total_count` field next to the
            indicator, a WARNING is logged once per stream instance. Request failures surface
            through the retriever's error handler and fail the stream like any other request.
            `$parameters` of the enclosing stream propagate into this retriever's components.
        message_repository: Optional repository through which the truncation warnings are emitted
            as Airbyte LOG messages so they are visible in the Connector Builder. When it is not
            set, the warnings go to the `airbyte` logger instead.
        config: The user-provided configuration as specified by the source's spec.
    """

    expand_records_from_field: Sequence[str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    remain_original_record: bool = False
    on_no_records: OnNoRecords = OnNoRecords.skip
    truncation_indicator_path: Optional[Sequence[str]] = None
    truncated_list_retriever: Optional["Retriever"] = None
    message_repository: Optional[MessageRepository] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._expand_path: list[InterpolatedString] = [
            InterpolatedString.create(path, parameters=parameters)
            for path in self.expand_records_from_field
        ]
        if self.truncated_list_retriever and not self.truncation_indicator_path:
            raise ValueError(
                "`truncation_indicator_path` is required when `truncated_list_retriever` is configured."
            )
        self._truncation_indicator_path: list[InterpolatedString] = [
            InterpolatedString.create(path, parameters=parameters)
            for path in (self.truncation_indicator_path or [])
        ]
        # The paths only interpolate `config`, so their evaluated values are validated up front.
        self._reject_globs(self._evaluated_indicator_path(), "truncation_indicator_path")
        if self.truncated_list_retriever:
            self._reject_globs(self._evaluated_expand_path(), "expand_records_from_field")
        self._warning_lock = threading.Lock()
        self._warned_truncation_without_retriever = False
        self._warned_incomplete_fetch = False

    @staticmethod
    def _reject_globs(path: Sequence[Any], field_name: str) -> None:
        if any(
            isinstance(segment, str) and any(char in segment for char in _GLOB_METACHARACTERS)
            for segment in path
        ):
            raise ValueError(
                f"Glob characters {_GLOB_METACHARACTERS} are not supported in `{field_name}` when "
                "truncation handling is configured: the path must identify a single field."
            )

    def _evaluated_indicator_path(self) -> list[Any]:
        return [segment.eval(self.config) for segment in self._truncation_indicator_path]

    def _evaluated_expand_path(self) -> list[Any]:
        return [segment.eval(self.config) for segment in self._expand_path]

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

        expand_path = self._evaluated_expand_path()
        truncated = bool(self._truncation_indicator_path) and self._is_truncated(parent_record)

        if truncated and self.truncated_list_retriever:
            fetched_count = 0
            for fetched in self._fetch_complete_list(parent_record):
                fetched_count += 1
                yield fetched
            self._warn_if_fetch_incomplete(parent_record, expand_path, fetched_count)
            if fetched_count > 0:
                return

        expanded_any = False
        embedded_count = 0

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
                    embedded_count += 1
                else:
                    if self.remain_original_record:
                        yield {
                            "value": item,
                            "original_record": copy.deepcopy(parent_record),
                        }
                    else:
                        yield item
                    expanded_any = True
                    embedded_count += 1

        if truncated and not self.truncated_list_retriever:
            self._warn_truncated_without_retriever(parent_record, expand_path, embedded_count)

        if not expanded_any and self.on_no_records == OnNoRecords.emit_parent:
            yield parent_record

    def _warn_truncated_without_retriever(
        self, parent_record: Mapping[str, Any], expand_path: list[Any], embedded_count: int
    ) -> None:
        with self._warning_lock:
            if self._warned_truncation_without_retriever:
                return
            self._warned_truncation_without_retriever = True

        indicator_path = self._evaluated_indicator_path()
        total_count = self._get_sibling_total_count(parent_record, indicator_path)
        total_fragment = f" of {total_count} total" if total_count is not None else ""
        self._emit_warning(
            f"The nested list at {expand_path} is marked as truncated (the field at {indicator_path} "
            f"is truthy) but no `truncated_list_retriever` is configured, so only the "
            f"{embedded_count} embedded item(s){total_fragment} were expanded and the remaining "
            "items are not emitted. Configure `truncated_list_retriever` to fetch the complete list "
            "if the API provides an endpoint for it. This warning is emitted once per stream; other "
            "records may be truncated as well."
        )

    def _warn_if_fetch_incomplete(
        self, parent_record: Mapping[str, Any], expand_path: list[Any], fetched_count: int
    ) -> None:
        indicator_path = self._evaluated_indicator_path()
        total_count = self._get_sibling_total_count(parent_record, indicator_path)
        if total_count is None or fetched_count >= total_count:
            return
        with self._warning_lock:
            if self._warned_incomplete_fetch:
                return
            self._warned_incomplete_fetch = True
        fallback_fragment = (
            " The embedded items were expanded as a fallback." if fetched_count == 0 else ""
        )
        self._emit_warning(
            f"The `truncated_list_retriever` for the nested list at {expand_path} returned "
            f"{fetched_count} record(s) but the `total_count` field next to {indicator_path} reports "
            f"{total_count}, so the fetched list is still incomplete.{fallback_fragment} Check that "
            "the retriever has a `paginator` configured and that its request matches the "
            "complete-list endpoint. This warning is emitted once per stream; other records may be "
            "affected as well."
        )

    def _emit_warning(self, message: str) -> None:
        if self.message_repository:
            self.message_repository.emit_message(
                AirbyteMessage(
                    type=MessageType.LOG,
                    log=AirbyteLogMessage(level=Level.WARN, message=message),
                )
            )
        else:
            logger.warning(message)

    def _get_sibling_total_count(
        self, parent_record: Mapping[str, Any], indicator_path: list[Any]
    ) -> Optional[int]:
        """Best-effort lookup of a `total_count` field next to the truncation indicator."""
        if not indicator_path:
            return None
        try:
            total = dpath.get(dict(parent_record), [*indicator_path[:-1], "total_count"])
        except (KeyError, ValueError):
            return None
        return total if isinstance(total, int) and not isinstance(total, bool) else None

    def _is_truncated(self, parent_record: MutableMapping[Any, Any]) -> bool:
        indicator_path = self._evaluated_indicator_path()
        try:
            return bool(dpath.get(parent_record, indicator_path))
        except (KeyError, ValueError):
            return False

    def _fetch_complete_list(self, parent_record: Mapping[str, Any]) -> Iterable[Any]:
        if not self.truncated_list_retriever:
            return
        stream_slice = StreamSlice(partition={"parent_record": parent_record}, cursor_slice={})
        for item in self.truncated_list_retriever.read_records(
            records_schema={}, stream_slice=stream_slice
        ):
            if isinstance(item, AirbyteMessage):
                if item.type != MessageType.RECORD or item.record is None:
                    continue
                data: Any = item.record.data
            elif isinstance(item, Record):
                data = item.data
            elif isinstance(item, _BARE_PROTOCOL_MESSAGES):
                continue
            else:
                data = item
            if isinstance(data, Mapping):
                expanded_record = dict(data)
                self._apply_parent_context(parent_record, expanded_record)
                yield expanded_record
            elif self.remain_original_record:
                yield {"value": data, "original_record": copy.deepcopy(parent_record)}
            else:
                yield data

    def _apply_parent_context(
        self, parent_record: Mapping[str, Any], child_record: MutableMapping[str, Any]
    ) -> None:
        """Apply parent context to a child record."""
        if self.remain_original_record:
            child_record["original_record"] = copy.deepcopy(parent_record)
