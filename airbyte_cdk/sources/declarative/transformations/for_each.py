#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Dict, List, Mapping, Optional, Union, cast

import dpath

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


@dataclass
class ForEach(RecordTransformation):
    """
    Applies a list of nested transformations to every element of a collection nested inside the record,
    instead of applying them to the record as a whole.

    `RecordTransformation.transform` mutates the record in place, so binding the nested transformations to a
    collection element mutates that element inside the parent record. Nothing is copied and nothing needs to
    be written back.

    Behavior of `field_path` resolution:
      * The path entries are interpolated strings, like every other dpath-based component.
      * A `*` wildcard segment is supported. When the path contains a `*`, every value matching the glob is
        treated as its own collection to iterate over.
      * If the path does not resolve, the transformation is a no-op. A collection missing from some records is
        expected and must not fail the sync.
      * If the resolved value is a list, the nested transformations are applied to each element.
      * If the resolved value is an object, it is treated as a collection of one, consistent with how
        `DpathExtractor` treats an object as a single record.
      * If the resolved value is a scalar or `None`, the transformation is a no-op.
      * An empty `field_path` resolves to the record itself, the same way `DpathExtractor` treats an empty
        `field_path`.
      * If an element of the resolved list is not an object (for example a list of strings), an error is
        raised. Such an element cannot be mutated in place, and silently skipping it would hide a manifest
        mistake behind missing data.

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
        self._parameters = parameters
        self._field_path: List[InterpolatedString] = [
            InterpolatedString.create(path, parameters=parameters) for path in self.field_path
        ]

    def transform(
        self,
        record: Dict[str, Any],
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> None:
        effective_config = config if config is not None else self.config
        path = [path.eval(effective_config) for path in self._field_path]

        for collection in self._resolve_collections(record, path):
            for element in self._iterate(collection, path):
                for transformation in self.transformations:
                    transformation.transform(
                        element,
                        config=effective_config,
                        stream_state=stream_state,
                        stream_slice=stream_slice,
                    )

    @staticmethod
    def _resolve_collections(record: Dict[str, Any], path: List[str]) -> List[Any]:
        """
        Resolve `path` to the collections to iterate over. `dpath` returns references to the nested objects,
        never copies, which is what makes the in-place mutation work.
        """
        if not path:
            return [record]
        if "*" in path:
            return list(dpath.values(record, path))
        resolved = dpath.get(record, path, default=None)
        return [] if resolved is None else [resolved]

    def _iterate(self, collection: Any, path: List[str]) -> List[Dict[str, Any]]:
        if isinstance(collection, dict):
            return [collection]
        if not isinstance(collection, list):
            # A scalar cannot hold nested records, so there is nothing to transform.
            return []

        for element in collection:
            if not isinstance(element, dict):
                stream_name = self._parameters.get("name")
                stream_context = f" of stream `{stream_name}`" if stream_name else ""
                raise AirbyteTracedException(
                    message=(
                        f"The ForEach transformation{stream_context} could not be applied because the "
                        f"collection at field_path {path} contains an element of type "
                        f"`{type(element).__name__}` instead of an object. ForEach can only transform "
                        f"collections of objects. Please point `field_path` at a list of objects."
                    ),
                    internal_message=(
                        f"ForEach transformation received a non-object element of type "
                        f"{type(element).__name__} at field_path {path}"
                    ),
                    failure_type=FailureType.config_error,
                )
        return cast(List[Dict[str, Any]], collection)

    def __eq__(self, other: Any) -> bool:
        return bool(self.__dict__ == other.__dict__)
