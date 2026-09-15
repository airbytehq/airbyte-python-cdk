#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import dpath
import requests

from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config


@dataclass
class ParentFieldPath:
    """
    A single copy instruction: read `parent_path` out of the parent record and write it into the
    child record at `record_path`.

    Both paths are lists of field names, so a value nested inside the parent can be copied into a
    nested position on the child. Intermediate objects on `record_path` are created as needed.

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
        return [path.eval(config) for path in self._parent_path]

    def eval_record_path(self, config: Config) -> List[str]:
        return [path.eval(config) for path in self._record_path]


@dataclass
class NestedRecordExtractor(RecordExtractor):
    """
    Record extractor that yields the records of a collection nested inside another record, while
    carrying fields down from the record that contains it.

    A `DpathExtractor` pointed at a child collection has already discarded the node that held it, so
    a record that needs a field from its parent cannot be produced declaratively. This component
    keeps the parent in scope: for every record its `parent_extractor` yields it reads
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

    Documented behaviour, all of it deliberate:

    * Child records are **mutated in place** rather than copied. The dicts come out of the response
      body this extractor just decoded and nothing else reads them, so a copy would only cost
      memory. Please do not "fix" this into a copy.
    * A `parent_path` that is absent on the parent copies `None`. Every record of the stream then
      carries the field, which keeps the record shape stable instead of making the field's presence
      depend on the parent.
    * A key already present on the child **is overwritten**. The parent context is the authoritative
      source for the fields the manifest names, and silently keeping the child's value would hide a
      misconfigured `record_path`.
    * A `child_field_path` that is missing, null, or resolves to an empty collection yields nothing
      rather than raising, the same way `DpathExtractor` treats a path that does not resolve.
    * Parent records and child elements that are not objects are skipped, because a scalar cannot
      carry the parent fields and emitting it would produce a record that silently lacks them.
    * `child_field_path` does not support the `*` wildcard. Flattening across several collections
      belongs in the `parent_extractor`, whose `DpathExtractor` supports it; the last hop has to stay
      a single collection for its parentage to be well defined.

    `parent_extractor` may itself be a `NestedRecordExtractor`, which is how a collection more than
    one level down is reached, and how fields from two different ancestors can be copied onto the
    same record.

    This component holds no mutable state: one instance is shared by every partition of a stream and
    the partitions are read concurrently.

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

    def extract_records(self, response: requests.Response) -> Iterable[MutableMapping[Any, Any]]:
        child_field_path = [path.eval(self.config) for path in self._child_field_path]
        field_copies = [
            (parent_field.eval_parent_path(self.config), parent_field.eval_record_path(self.config))
            for parent_field in self._parent_fields
        ]

        for parent in self.parent_extractor.extract_records(response):
            if not isinstance(parent, MutableMapping):
                continue
            for child in self._children(parent, child_field_path):
                for parent_path, record_path in field_copies:
                    dpath.new(child, record_path, dpath.get(parent, parent_path, default=None))
                yield child

    @staticmethod
    def _children(
        parent: MutableMapping[Any, Any], child_field_path: List[str]
    ) -> Iterable[MutableMapping[Any, Any]]:
        """Yield the elements of the nested collection without materialising them into a new list."""
        extracted = dpath.get(parent, child_field_path, default=None)
        if isinstance(extracted, MutableMapping):
            if extracted:
                # A single object where a collection would also be allowed, the shape a GraphQL
                # drill-down document returns. An empty object yields nothing, matching how
                # `DpathExtractor` treats one.
                yield extracted
        elif isinstance(extracted, list):
            for child in extracted:
                if isinstance(child, MutableMapping):
                    yield child
