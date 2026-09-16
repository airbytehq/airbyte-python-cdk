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


def _detached(value: Any) -> Any:
    """Copy of `value` sharing nothing with the record it came from; scalars are returned as-is."""
    if isinstance(value, (Mapping, list, set, tuple)):
        return copy.deepcopy(value)
    return value


class OnNoRecords(Enum):
    """
    Behavior when record expansion produces no records.
    """

    skip = "skip"
    emit_parent = "emit_parent"


@dataclass
class ParentFieldPath:
    """One field to copy from the record being expanded onto each expanded item.

    `parent_path` locates the value on the parent, `record_path` says where to put it on the
    child. Both are lists, so a value nested inside the parent can be copied into a nested
    position on the child. Glob metacharacters are rejected: each path must identify a single
    field.
    """

    parent_path: Sequence[str]
    record_path: Sequence[str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not self.parent_path:
            raise ValueError("`parent_path` cannot be empty.")
        if not self.record_path:
            raise ValueError("`record_path` cannot be empty.")
        self._parent_path: list[InterpolatedString] = [
            InterpolatedString.create(path, parameters=parameters) for path in self.parent_path
        ]
        self._record_path: list[InterpolatedString] = [
            InterpolatedString.create(path, parameters=parameters) for path in self.record_path
        ]
        RecordExpander._reject_globs(self.evaluated_parent_path(), "parent_path")
        RecordExpander._reject_globs(self.evaluated_record_path(), "record_path")

    def evaluated_parent_path(self) -> list[Any]:
        return [segment.eval(self.config) for segment in self._parent_path]

    def evaluated_record_path(self) -> list[Any]:
        return [segment.eval(self.config) for segment in self._record_path]

    def copy_onto(
        self, parent_record: Mapping[str, Any], child_record: MutableMapping[str, Any]
    ) -> None:
        """Copy the parent value onto the child, overwriting whatever was there.

        A `parent_path` that the parent does not have copies `None`, which is what the custom
        connector classes this replaces do (`parent.get(field)`). Distinguishing "absent" from
        "present and null" would need a third option and no connector needs one.

        A container value is deep-copied, so that a downstream transformation writing inside it
        cannot reach the parent record or the items expanded from it alongside this one. The copy
        costs proportionally to the named value, not to the whole parent.
        """
        try:
            value = dpath.get(dict(parent_record), self.evaluated_parent_path())
        except (KeyError, ValueError):
            value = None
        dpath.new(child_record, self.evaluated_record_path(), _detached(value))


@dataclass
class RecordExpander:
    """Expands records by extracting items from a nested array field.

    When configured, this component extracts items from a specified nested array path
    within each record and emits each item as a separate record. Set `remain_original_record: true`
    to embed the full parent record under `original_record` in each expanded item when you need
    downstream transformations to access parent context.

    When only a few parent fields are needed, prefer `parent_fields` over
    `remain_original_record`: it copies the named values onto each expanded item instead of
    deep-copying the whole parent once per item, which matters when the parent is large and the
    nested list is long.

    Set `merge_parent: true` to flatten the parent into each item instead: the parent's fields,
    minus the expanded list, are shallow-merged underneath the item's own fields, so the item wins
    on any key both have.

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
          - "reviews"
          - "nodes"
        parent_fields:
          - parent_path: ["url"]
            record_path: ["pull_request_url"]
    ```

    ```
      record_expander:
        type: RecordExpander
        expand_records_from_field:
          - "activity"
        merge_parent: true
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
        parent_fields: Named values to copy from the record being expanded onto each expanded
            item. Each entry has a `parent_path` and a `record_path`; an existing value at
            `record_path` is overwritten, and a `parent_path` the parent does not have copies
            `None`. A copied container is deep-copied, so writing into it downstream does not
            reach the parent or the sibling items. Independent of `remain_original_record` -
            both may be set, though copying named fields is the cheaper way to carry parent
            context. Applies to items fetched through `truncated_list_retriever` as well as to
            embedded ones. Glob metacharacters are rejected in both paths.
        merge_parent: If True, each expanded item is the parent record shallow-merged with the
            item, the item's own keys winning on collision, and the expanded list removed from
            the parent's copy. Only the value at `expand_records_from_field` is removed: for a
            multi-segment path the top-level key stays with its other fields. Each item gets its
            own deep copy of the merged parent, so a downstream transformation writing into a
            nested value affects only that item. The merge happens before `parent_fields` are
            copied and before `original_record` is embedded, so both can overwrite merged
            values. Applies to items fetched through `truncated_list_retriever` as well as to
            embedded ones. Defaults to False.
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
            In Connector Builder test reads the page limit applies to each fetch independently,
            so the fetched list may be shorter than `total_count`; no incomplete-fetch warning
            is emitted there.
        message_repository: Optional repository through which the truncation warnings are emitted
            as Airbyte LOG messages so they are visible in the Connector Builder. When it is not
            set, the warnings go to the `airbyte` logger instead.
        suppress_incomplete_fetch_warning: Skip the incomplete-fetch WARNING. Set by the factory
            for Connector Builder test reads when the page limit caps the retriever's paginator,
            so a shortfall against `total_count` is expected. The truncated-without-retriever
            warning is not affected.
        config: The user-provided configuration as specified by the source's spec.
    """

    expand_records_from_field: Sequence[str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    remain_original_record: bool = False
    parent_fields: Optional[Sequence[ParentFieldPath]] = None
    merge_parent: bool = False
    on_no_records: OnNoRecords = OnNoRecords.skip
    truncation_indicator_path: Optional[Sequence[str]] = None
    truncated_list_retriever: Optional["Retriever"] = None
    message_repository: Optional[MessageRepository] = None
    suppress_incomplete_fetch_warning: bool = False

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
                f"Glob characters {_GLOB_METACHARACTERS} are not supported in `{field_name}`: "
                "the path must identify a single field."
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
        # Built once per parent, not once per item: the walk that finds the expanded list costs
        # as much as extracting it does.
        merge_base = (
            self._without_expanded_list(parent_record, expand_path) if self.merge_parent else None
        )

        if truncated and self.truncated_list_retriever:
            # Streamed, so the shortfall is only known once the retriever is exhausted. If the
            # consumer stops early the fetch was cut short by it, not by the API, and no warning
            # would be accurate anyway.
            fetched_count = 0
            for fetched in self._fetch_complete_list(parent_record, merge_base):
                fetched_count += 1
                yield fetched
            self._warn_if_fetch_incomplete(parent_record, expand_path, fetched_count)
            if fetched_count > 0:
                return

        try:
            extracted_values = dpath.values(parent_record, expand_path)
        except KeyError:
            extracted_values = []

        embedded_lists = [
            extracted for extracted in extracted_values if isinstance(extracted, list)
        ]
        embedded_count = sum(len(items) for items in embedded_lists)

        if truncated and not self.truncated_list_retriever:
            self._warn_truncated_without_retriever(parent_record, expand_path, embedded_count)

        for items in embedded_lists:
            for item in items:
                if isinstance(item, dict):
                    yield self._apply_parent_context(parent_record, dict(item), merge_base)
                elif self._carries_parent_context():
                    yield self._apply_parent_context(parent_record, {"value": item}, merge_base)
                else:
                    yield item

        if embedded_count == 0 and self.on_no_records == OnNoRecords.emit_parent:
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
        if self.suppress_incomplete_fetch_warning:
            return
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

    def _fetch_complete_list(
        self, parent_record: Mapping[str, Any], merge_base: Optional[Mapping[str, Any]]
    ) -> Iterable[Any]:
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
                yield self._apply_parent_context(parent_record, dict(data), merge_base)
            elif self._carries_parent_context():
                yield self._apply_parent_context(parent_record, {"value": data}, merge_base)
            else:
                yield data

    def _apply_parent_context(
        self,
        parent_record: Mapping[str, Any],
        child_record: MutableMapping[str, Any],
        merge_base: Optional[Mapping[str, Any]],
    ) -> MutableMapping[str, Any]:
        """Return the child record carrying the configured parent context.

        The order is fixed: the parent is merged underneath the child first, then `parent_fields`
        are copied on top, then `original_record` is embedded. A named copy can therefore
        overwrite a merged value.
        """
        if merge_base is not None:
            # Deep-copied per item: a shallow merge would alias every nested container of the
            # parent into every item, so a downstream transformation writing into one of them
            # would reach the parent record and the items already yielded. The expanded list is
            # already stripped from `merge_base`, so the copy costs less than
            # `remain_original_record` does.
            child_record = {**copy.deepcopy(merge_base), **child_record}
        for parent_field in self.parent_fields or []:
            parent_field.copy_onto(parent_record, child_record)
        if self.remain_original_record:
            child_record["original_record"] = copy.deepcopy(parent_record)
        return child_record

    def _carries_parent_context(self) -> bool:
        return bool(self.remain_original_record or self.parent_fields or self.merge_parent)

    @classmethod
    def _without_expanded_list(
        cls, parent_record: Mapping[str, Any], expand_path: list[Any]
    ) -> Mapping[str, Any]:
        """Copy of the parent with only the value at `expand_path` removed.

        The containers on the way to the list are copied and the rest of the parent is shared,
        so siblings of the list are kept and the parent itself is not mutated here. This base is
        built once per parent record and then deep-copied per item in `_apply_parent_context`,
        which is what keeps the parent and the sibling items isolated from one another. Every
        match of the path is removed, which covers glob paths that select several lists.
        """
        matched_paths = [
            path
            for path, _ in dpath.segments.walk(parent_record)  # type: ignore[no-untyped-call]
            if dpath.segments.match(path, expand_path)
        ]
        stripped: Any = parent_record
        # Deepest and right-most first, so removing a list element never shifts the index of a
        # match still to be removed, and a match nested inside another match goes first.
        for path in sorted(matched_paths, key=cls._path_sort_key, reverse=True):
            stripped = cls._without_path(stripped, path)
        return dict(stripped)

    @staticmethod
    def _path_sort_key(path: Sequence[Any]) -> list[tuple[int, Any]]:
        # Integers and strings are not mutually comparable, so tag each segment by kind.
        return [(0, segment) if isinstance(segment, int) else (1, str(segment)) for segment in path]

    @classmethod
    def _without_path(cls, container: Any, path: Sequence[Any]) -> Any:
        head, rest = path[0], path[1:]
        if isinstance(container, Mapping):
            copied: Any = dict(container)
        elif isinstance(container, list):
            copied = list(container)
        else:
            return container
        if rest:
            copied[head] = cls._without_path(copied[head], rest)
        else:
            del copied[head]
        return copied
