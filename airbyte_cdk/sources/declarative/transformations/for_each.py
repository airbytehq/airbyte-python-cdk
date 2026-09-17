#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Dict, List, Mapping, Optional, Set, Union

import dpath

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

# Segments containing any of these need `dpath` glob matching. Everything else is resolved by a
# plain walk, because `dpath` traverses the whole record to glob-match and that cost is paid per
# record.
_GLOB_CHARACTERS = ("*", "?", "[")


def _as_list_index_or_none(segment: str) -> Optional[int]:
    """
    `int()` is what decides whether a segment is an index at all (`dpath.segments.match`), so a
    segment `int()` rejects is not an index. Note that `int()` is not `str.isdigit()`: `"²".isdigit()`
    is `True` while `int("²")` raises.
    """
    try:
        return int(segment)
    except ValueError:
        return None


def _as_list_index(segment: str, length: int) -> Optional[int]:
    """
    Resolve `segment` against a list of `length` elements exactly the way `dpath` does: a negative
    index counts from the end (`dpath.types.ListIndex.__eq__`). A segment that is not an index, or an
    index outside the list, matches nothing -- the empty result `dpath.values` returns.
    """
    index = _as_list_index_or_none(segment)
    if index is None:
        return None
    return index if -length <= index < length else None


@dataclass
class ForEach(RecordTransformation):
    """
    Applies a list of nested transformations to every element of a collection nested inside the record,
    instead of applying them to the record as a whole.

    `RecordTransformation.transform` mutates the record in place, so binding the nested transformations to a
    collection element mutates that element inside the parent record. Nothing is copied and nothing needs to
    be written back.

    Behavior of `field_path` resolution:
      * The path entries are interpolated strings, like every other dpath-based component. A segment that
        interpolates to something other than a non-empty string or an integer is a configuration error,
        because it would otherwise turn the whole transformation into a silent no-op. An empty segment
        written literally in the manifest is rejected at construction time instead, as a manifest error.
      * A segment addressing a list is resolved the way `dpath` resolves it: any value `int()` accepts is
        an index, and a negative index counts from the end. An index outside the list matches nothing.
      * Glob segments (`*`, `?`, `[...]`) are supported. When the path contains one, every value matching the
        glob is treated as its own collection to iterate over. Because a glob selects rather than addresses,
        it is lenient: a match that is neither an object nor a list of objects is skipped instead of raising.
        A glob can also match the same object twice -- `**` matches a nested object both on its own and as a
        member of its parent list -- so matches are de-duplicated by identity and every object is transformed
        exactly once.
      * A key that literally contains `*`, `?` or `[` cannot be addressed: the segment is matched as a glob,
        which is how `dpath` and therefore every other dpath-based component (`DpathExtractor`,
        `DpathFlattenFields`) already behaves. There is no escape syntax.
      * If the path does not resolve, the transformation is a no-op. A collection missing from some records is
        expected and must not fail the sync.
      * If the resolved value is a list, the nested transformations are applied to each element.
      * If the resolved value is an object, it is treated as a collection of one, consistent with how
        `DpathExtractor` treats an object as a single record.
      * If the resolved value is a scalar or `None`, the transformation is a no-op.
      * An empty `field_path` resolves to the record itself, the same way `DpathExtractor` treats an empty
        `field_path`.
      * A `null` element inside the collection is skipped. A `null` in an array is ordinary API payload rather
        than a manifest mistake, so it must not fail the sync.
      * Any other non-object element (a string, a number) in a list addressed without a glob raises. Such an
        element cannot be mutated in place, and silently skipping it would hide a mis-pointed `field_path`
        behind missing data.

    Every collection the path matches is resolved and validated before any element is transformed, so a
    non-object element anywhere under a glob leaves the whole record untouched. This covers element
    validation only: like every other transformation in the pipeline, a nested transformation that raises
    part-way through leaves the elements already transformed mutated.

    Example:
    ```yaml
    transformations:
      - type: ForEach
        field_path: ["column_values"]
        transformations:
          - type: AddFields
            condition: "{{ record.get('display_value') and not record.get('text') }}"
            fields:
              - path: ["text"]
                value: "{{ record['display_value'] }}"
    ```

    Attributes:
        config (Config): The user-provided configuration as specified by the source's spec
        field_path (List[Union[InterpolatedString, str]]): Path to the collection to iterate over
        transformations (List[RecordTransformation]): Transformations applied to each element of the collection
    """

    config: Config
    field_path: List[Union[InterpolatedString, str]]
    transformations: List[RecordTransformation]
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._field_path: List[InterpolatedString] = [
            InterpolatedString.create(path, parameters=parameters) for path in self.field_path
        ]
        # An empty segment cannot be a template, so it can only come from the manifest. Catching it
        # here rather than per record keeps it classified as the manifest error it is.
        if any(not segment.string for segment in self._field_path):
            raise AirbyteTracedException(
                message=(
                    f"ForEach cannot resolve `field_path` {self.field_path}: it contains an empty "
                    f"path segment. Remove the empty segment, or set `field_path` to `[]` to target "
                    f"the record itself."
                ),
                internal_message=(
                    f"ForEach field_path contains an empty literal segment: {self.field_path}"
                ),
                failure_type=FailureType.system_error,
            )

    def transform(
        self,
        record: Dict[str, Any],
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> None:
        effective_config = config if config is not None else self.config
        path = self._evaluate_path(effective_config)

        # Resolving and validating every matched collection up front is what makes the documented
        # "a bad element leaves the record untouched" contract hold across a glob, not just within a
        # single collection.
        candidates = [
            element
            for collection in self._resolve_collections(record, path)
            for element in self._elements_of(collection, path)
        ]

        # Overlapping glob matches can yield the same object twice -- `**` matches a nested object
        # both on its own and as a member of its parent list. De-duplicate by identity so a
        # non-idempotent nested transformation is not applied twice to one object.
        seen: Set[int] = set()
        elements = []
        for element in candidates:
            if id(element) not in seen:
                seen.add(id(element))
                elements.append(element)

        for element in elements:
            for transformation in self.transformations:
                transformation.transform(
                    element,
                    config=effective_config,
                    stream_state=stream_state,
                    stream_slice=stream_slice,
                )

    def _evaluate_path(self, config: Config) -> List[str]:
        path = []
        for interpolated_segment in self._field_path:
            segment = interpolated_segment.eval(config)
            # Jinja renders a numeric segment as an `int`, and `dpath` accepts an integer list
            # index, so a number is a valid path element rather than a broken one.
            if isinstance(segment, int) and not isinstance(segment, bool):
                segment = str(segment)
            if not isinstance(segment, str) or not segment:
                raise AirbyteTracedException(
                    message=(
                        f"ForEach cannot resolve `field_path` {self.field_path}: a path segment "
                        f"evaluated to {segment!r} instead of a non-empty string. Check the "
                        f"configuration values it interpolates."
                    ),
                    internal_message=(
                        f"ForEach field_path segment evaluated to {segment!r} "
                        f"({type(segment).__name__}) for field_path {self.field_path}"
                    ),
                    failure_type=FailureType.config_error,
                )
            path.append(segment)
        return path

    @staticmethod
    def _resolve_collections(record: Dict[str, Any], path: List[str]) -> List[Any]:
        """
        Resolve `path` to the collections to iterate over. Both the plain walk and `dpath` return
        references to the nested objects, never copies, which is what makes the in-place mutation work.
        """
        if not path:
            return [record]
        if any(character in segment for segment in path for character in _GLOB_CHARACTERS):
            return list(dpath.values(record, path))

        node: Any = record
        for segment in path:
            if isinstance(node, dict):
                if segment in node:
                    node = node[segment]
                else:
                    # `dpath` glob-matches a segment against `str(key)`, so a string segment
                    # addresses an integer dict key too. Keep the walk in parity with it.
                    index = _as_list_index_or_none(segment)
                    if index is None or index not in node:
                        return []
                    node = node[index]
            elif isinstance(node, list):
                index = _as_list_index(segment, len(node))
                if index is None:
                    return []
                node = node[index]
            else:
                return []
        return [] if node is None else [node]

    @staticmethod
    def _elements_of(collection: Any, path: List[str]) -> List[Dict[str, Any]]:
        if isinstance(collection, dict):
            return [collection]
        if not isinstance(collection, list):
            # A scalar cannot hold nested records, so there is nothing to transform.
            return []

        elements = []
        for element in collection:
            if isinstance(element, dict):
                elements.append(element)
            elif element is not None:
                raise AirbyteTracedException(
                    message=(
                        f"ForEach cannot transform the collection at field_path {path}: it holds a "
                        f"non-object element ({type(element).__name__}). Set `field_path` to a list "
                        f"of objects."
                    ),
                    internal_message=(
                        f"ForEach transformation received a non-object element of type "
                        f"{type(element).__name__} at field_path {path}"
                    ),
                    failure_type=FailureType.system_error,
                )
        return elements
