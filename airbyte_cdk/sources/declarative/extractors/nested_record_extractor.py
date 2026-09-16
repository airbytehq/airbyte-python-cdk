#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import copy
import logging
from dataclasses import InitVar, dataclass
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

logger = logging.getLogger("airbyte")

# The `*` wildcard is rejected rather than treated as a literal key, because a manifest that writes
# one almost certainly means it to glob. Every other character is a literal: unlike the `dpath`-based
# extractors, this component walks paths itself, so `?` and `[...]` address the keys that spell them.
_UNSUPPORTED_PATH_CHARACTER = "*"

# Values of these types are immutable, so every child can share the one the parent holds. Anything
# else is copied per child — see `NestedRecordExtractor.extract_records`.
_IMMUTABLE_VALUE_TYPES = (str, int, float, bool, type(None))


# Returned by `_descend` when a segment does not address anything on the node it is given, which is
# distinct from a segment that addresses a field holding `None`.
_MISSING = object()


def _is_list_index(segment: str) -> bool:
    """Whether `segment` addresses a list position. `isdecimal` rather than `isdigit`: the latter is
    true for superscripts, which `int` then refuses."""
    return (segment[1:] if segment.startswith("-") else segment).isdecimal()


def _descend(node: Any, segment: str) -> Any:
    """
    Follow one path segment into `node`, returning `_MISSING` when the segment addresses nothing.

    Objects are addressed by key and lists by decimal index, including a negative one, which is what
    the `dpath`-based extractors accept.
    """
    if isinstance(node, Mapping):
        return node[segment] if segment in node else _MISSING
    if isinstance(node, list):
        if not _is_list_index(segment):
            return _MISSING
        try:
            return node[int(segment)]
        except IndexError:
            return _MISSING
    return _MISSING


def _normalize_path(path: List[Any], field_name: str, component: str) -> List[str]:
    """
    Validate an interpolated path and return it as field names.

    Segments arrive as whatever the interpolation produced: `InterpolatedString` runs its result
    through `ast.literal_eval`, so a segment spelled `0` evaluates to the integer `0`. JSON object
    keys are always strings and list indices are addressed by their decimal spelling, so the segment
    is normalized to `str`. A segment that resolved to nothing cannot address a field and is
    rejected, rather than silently reading or writing a field named "".
    """
    normalized = []
    for raw_segment in path:
        segment = "" if raw_segment is None else str(raw_segment)
        if segment == "":
            raise AirbyteTracedException(
                message=f"The connector is configured with an invalid `{field_name}`. Check the connector's configuration.",
                internal_message=(
                    f"{component} received an empty segment in `{field_name}` ({path!r}). An empty "
                    f"segment usually means an interpolated expression resolved to nothing."
                ),
                failure_type=FailureType.config_error,
            )
        if _UNSUPPORTED_PATH_CHARACTER in segment:
            raise AirbyteTracedException(
                message=f"The connector is configured with an invalid `{field_name}`. Check the connector's configuration.",
                internal_message=(
                    f"{component} does not support the '{_UNSUPPORTED_PATH_CHARACTER}' wildcard in "
                    f"`{field_name}` ({path!r}). Flatten across several collections in the "
                    f"`parent_extractor` instead."
                ),
                failure_type=FailureType.config_error,
            )
        normalized.append(segment)
    return normalized


@dataclass
class ParentFieldPath:
    """
    A single copy instruction: read `parent_path` out of the parent record and write it into the
    child record at `record_path`.

    Both paths are lists of field names, so a value nested inside the parent can be copied into a
    nested position on the child. Intermediate objects on `record_path` are created as needed, and
    an intermediate that is present but is not an object is replaced by one.

    Attributes:
        parent_path (List[Union[InterpolatedString, str]]): Path of the field to read on the parent record
        record_path (List[Union[InterpolatedString, str]]): Path to write the value to on the child record
    """

    parent_path: List[Union[InterpolatedString, str]]
    record_path: List[Union[InterpolatedString, str]]
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not self.parent_path:
            raise ValueError("ParentFieldPath requires a non-empty `parent_path`")
        if not self.record_path:
            raise ValueError("ParentFieldPath requires a non-empty `record_path`")
        self._parent_path = [
            InterpolatedString.create(path, parameters=parameters) for path in self.parent_path
        ]
        self._record_path = [
            InterpolatedString.create(path, parameters=parameters) for path in self.record_path
        ]

    def eval_parent_path(self, config: Config) -> List[str]:
        return _normalize_path(
            [path.eval(config) for path in self._parent_path], "parent_path", "ParentFieldPath"
        )

    def eval_record_path(self, config: Config) -> List[str]:
        return _normalize_path(
            [path.eval(config) for path in self._record_path], "record_path", "ParentFieldPath"
        )


@dataclass
class NestedRecordExtractor(RecordExtractor):
    """
    Record extractor that yields the records of a collection nested inside another record, while
    carrying fields down from the record that contains it.

    A `DpathExtractor` pointed at a child collection has already discarded the node that held it, so
    a record that needs a field from its parent cannot be produced by that component alone. This
    component keeps the parent in scope: for every record its `parent_extractor` yields it reads
    `child_field_path` out of that record, yields each element, and copies the `parent_fields`
    entries from the parent onto every element first.

    ```
      extractor:
        type: NestedRecordExtractor
        parent_extractor:
          type: DpathExtractor
          field_path: ["data", "repository", "pullRequests", "nodes"]
        child_field_path: ["reviews", "nodes"]
        parent_fields:
          - parent_path: ["url"]
            record_path: ["pull_request_url"]
    ```

    For a single level of nesting whose child collection is always a list, `DpathExtractor` with a
    `record_expander` plus an `AddFields` transformation can produce the same records. This component
    exists for the three shapes that combination cannot express: a child field that is a single
    object rather than a list, fields copied from more than one ancestor (`record_expander` is not
    nestable), and copying named fields rather than deep-copying the whole parent onto every child.

    Documented behaviour, all of it deliberate:

    * Child records are **mutated in place** rather than copied, when the parent extractor yields
      mutable ones. The dicts come out of the response body this extractor just decoded and nothing
      else reads them, so a copy would only cost memory. The *values* copied from the parent are a
      different matter and are copied per child — see `extract_records`.
    * A `parent_path` that is absent on the parent, or whose traversal runs into a null, copies
      `None`. Every record of the stream then carries the field, which keeps the record shape stable
      instead of making the field's presence depend on the parent.
    * A `parent_path` that runs into a value that is neither an object nor null raises, because the
      path cannot address a field there and silently copying `None` would hide a `parent_path` that
      does not match the response.
    * A key already present on the child **is overwritten**, including when an intermediate segment
      of `record_path` holds something that is not an object. The parent context is the authoritative
      source for the fields the manifest names, and silently keeping the child's value would hide a
      misconfigured `record_path`.
    * A `child_field_path` that is missing, null, or resolves to an empty collection yields nothing
      rather than raising, the same way `DpathExtractor` treats a path that does not resolve.
    * Parent records and child elements that are not objects are skipped, because a scalar cannot
      carry the parent fields and emitting it would produce a record that silently lacks them.
    * Paths do not support the `*` wildcard, and it is rejected rather than read as a literal key.
      Flattening across several collections belongs in the `parent_extractor`, whose `DpathExtractor`
      supports it; the last hop has to stay a single collection for its parentage to be well defined.

    `parent_extractor` may itself be a `NestedRecordExtractor`, which is how a collection more than
    one level down is reached, and how fields from two different ancestors can be copied onto the
    same record. `type` is required on `parent_extractor`: unlike `RecordSelector.extractor` it has
    no default, because the field accepts a `NestedRecordExtractor` as readily as a `DpathExtractor`.

    This component holds no per-read mutable state: one instance is shared by every partition of a
    stream and the partitions are read concurrently.

    Attributes:
        parent_extractor (RecordExtractor): Extractor producing the records that contain the collection
        child_field_path (List[Union[InterpolatedString, str]]): Path to the nested collection on each parent record
        config (Config): The user-provided configuration as specified by the source's spec
        parent_fields (Optional[List[ParentFieldPath]]): Fields to copy from the parent onto every child record
    """

    parent_extractor: RecordExtractor
    child_field_path: List[Union[InterpolatedString, str]]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    parent_fields: Optional[List[ParentFieldPath]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not self.child_field_path:
            raise ValueError("NestedRecordExtractor requires a non-empty `child_field_path`")
        self._child_field_path = [
            InterpolatedString.create(path, parameters=parameters) for path in self.child_field_path
        ]
        self._parent_fields = self.parent_fields or []

    def extract_records(self, response: requests.Response) -> Iterable[Mapping[str, Any]]:
        child_field_path = _normalize_path(
            [path.eval(self.config) for path in self._child_field_path],
            "child_field_path",
            "NestedRecordExtractor",
        )
        field_copies = [
            (parent_field.eval_parent_path(self.config), parent_field.eval_record_path(self.config))
            for parent_field in self._parent_fields
        ]
        for parent_path, _ in field_copies:
            self._reject_self_referential_copy(parent_path, child_field_path)

        for parent in self.parent_extractor.extract_records(response):
            if not isinstance(parent, Mapping):
                logger.debug(
                    "NestedRecordExtractor skipped a parent record that is not an object (%s)",
                    type(parent).__name__,
                )
                continue

            # Reading the parent is invariant across its children, so it happens once per parent.
            # Doing it per child makes the extractor quadratic in the size of the child collection,
            # because each read walks a parent that contains that collection.
            values = []
            for parent_path, record_path in field_copies:
                value = self._read_parent_value(parent, parent_path)
                values.append((record_path, value, not isinstance(value, _IMMUTABLE_VALUE_TYPES)))

            for child in self._children(parent, child_field_path):
                if not isinstance(child, MutableMapping):
                    # A parent extractor may legally yield read-only mappings: the `RecordExtractor`
                    # contract is `Mapping`. Copy rather than drop the record.
                    child = dict(child)
                for record_path, value, needs_copy in values:
                    # A mutable value must be copied per child. Sharing one object across the
                    # collection means a later transformation that writes into it rewrites records
                    # that were already emitted — they are queued unserialized downstream.
                    self._write_child_value(
                        child, record_path, copy.deepcopy(value) if needs_copy else value
                    )
                yield child

    @staticmethod
    def _read_parent_value(parent: Mapping[str, Any], parent_path: List[str]) -> Any:
        """Read `parent_path` out of `parent`, returning `None` when the path does not resolve."""
        node: Any = parent
        for index, segment in enumerate(parent_path):
            if node is None:
                return None
            if not isinstance(node, Mapping) and not (
                isinstance(node, list) and _is_list_index(segment)
            ):
                raise AirbyteTracedException(
                    message="A record returned by the API does not have the shape the connector expects.",
                    internal_message=(
                        f"NestedRecordExtractor cannot read `parent_path` {parent_path!r}: the "
                        f"segment {segment!r} does not address a field on the "
                        f"{type(node).__name__} it was reached on. The path resolved up to "
                        f"{parent_path[:index]!r}."
                    ),
                    failure_type=FailureType.system_error,
                )
            node = _descend(node, segment)
            if node is _MISSING:
                return None
        return node

    @staticmethod
    def _write_child_value(
        child: MutableMapping[str, Any], record_path: List[str], value: Any
    ) -> None:
        """
        Write `value` into `child` at `record_path`, creating intermediate objects as needed.

        An intermediate that is present but is not an object is replaced by one, so the documented
        "a key already present on the child is overwritten" holds for a nested `record_path` too.
        Segments are always object keys — a numeric-looking segment does not create a list.
        """
        node = child
        for segment in record_path[:-1]:
            existing = node.get(segment)
            if isinstance(existing, MutableMapping):
                node = existing
            elif isinstance(existing, Mapping):
                replacement = dict(existing)
                node[segment] = replacement
                node = replacement
            else:
                replacement = {}
                node[segment] = replacement
                node = replacement
        node[record_path[-1]] = value

    @staticmethod
    def _reject_self_referential_copy(parent_path: List[str], child_field_path: List[str]) -> None:
        """
        Reject a `parent_path` that addresses the child collection, or something containing it.

        The child records are not copied out of the parent, so the value read by such a path holds
        the very records being emitted. Copying it onto each of them gives every record a snapshot
        of its whole sibling collection — quadratic in its size, and never what a manifest means.
        """
        if child_field_path[: len(parent_path)] == parent_path:
            raise AirbyteTracedException(
                message="The connector is configured with an invalid `parent_path`. Check the connector's configuration.",
                internal_message=(
                    f"NestedRecordExtractor cannot copy `parent_path` {parent_path!r} onto the "
                    f"records of `child_field_path` {child_field_path!r}: the path addresses the "
                    f"child collection itself, so every record would carry a copy of its siblings."
                ),
                failure_type=FailureType.config_error,
            )

    @staticmethod
    def _children(
        parent: Mapping[str, Any], child_field_path: List[str]
    ) -> Iterable[Mapping[str, Any]]:
        """Yield the elements of the nested collection without materialising them into a new list."""
        extracted: Any = parent
        for segment in child_field_path:
            extracted = _descend(extracted, segment)
            if extracted is _MISSING:
                return

        if isinstance(extracted, Mapping):
            if extracted:
                # A single object where a collection would also be allowed, the shape a GraphQL
                # drill-down document returns. An empty object yields nothing, matching how
                # `DpathExtractor` treats one.
                yield extracted
        elif isinstance(extracted, list):
            for child in extracted:
                if isinstance(child, Mapping):
                    yield child
                else:
                    logger.debug(
                        "NestedRecordExtractor skipped a child element that is not an object (%s)",
                        type(child).__name__,
                    )
        elif extracted is not None:
            logger.debug(
                "NestedRecordExtractor resolved `child_field_path` %s to a %s, which holds no "
                "records",
                child_field_path,
                type(extracted).__name__,
            )
