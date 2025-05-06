#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from airbyte_cdk.sources.declarative.transformations.transformation import RecordTransformation
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class RemapField(RecordTransformation):
    """
    Transformation that remaps a field's value to another value based on a static map.
    """

    map: Mapping[str, Any]
    field_path: str

    def transform(
        self,
        record: Dict[str, Any],
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> None:
        """
        Transforms a record by remapping a field value based on the provided map.
        If the original value is found in the map, it's replaced with the mapped value.
        If the value is not in the map, the field remains unchanged.

        :param record: The input record to be transformed
        :param config: The user-provided configuration as specified by the source's spec
        :param stream_state: The stream state
        :param stream_slice: The stream slice
        """
        # Extract path components
        path_components = self.field_path.split(".")

        # Navigate to the parent object containing the field to remap
        current = record
        for i, component in enumerate(path_components[:-1]):
            if component not in current:
                # Path doesn't exist, so nothing to remap
                return
            current = current[component]

            # If we encounter a non-dict, we can't continue navigating
            if not isinstance(current, dict):
                return

        # The last component is the field name to remap
        field_name = path_components[-1]

        # Check if the field exists and remap its value if it's in the map
        if field_name in current and current[field_name] in self.map:
            current[field_name] = self.map[current[field_name]]
