#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
from dataclasses import dataclass
from typing import Any, List, MutableMapping

import dpath
import dpath.exceptions

from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean
from airbyte_cdk.sources.declarative.transformations.config_transformations.config_transformation import (
    ConfigTransformation,
)
from airbyte_cdk.sources.types import FieldPointer


@dataclass
class ConfigRemoveFields(ConfigTransformation):
    """
    A transformation which removes fields from a config. The fields removed are designated using FieldPointers.
    During transformation, if a field or any of its parents does not exist in the config, no error is thrown.

    If an input field pointer references an item in a list (e.g: ["k", 0] in the object {"k": ["a", "b", "c"]}) then
    the object at that index is set to None rather than being entirely removed from the list.

    It's possible to remove objects nested in lists e.g: removing [".", 0, "k"] from {".": [{"k": "V"}]} results in {".": [{}]}

    Usage syntax:

    ```yaml
        config_transformations:
          - type: RemoveFields
            field_pointers:
              - ["path", "to", "field1"]
              - ["path2"]
            condition: "{{ config.some_flag }}" # Optional condition
    ```

    Attributes:
        field_pointers (List[FieldPointer]): pointers to the fields that should be removed
        condition (str): Optional condition that determines if the fields should be removed
    """

    field_pointers: List[FieldPointer]
    condition: str = ""

    def __post_init__(self) -> None:
        self._filter_interpolator = InterpolatedBoolean(condition=self.condition, parameters={})

    def transform(
        self,
        config: MutableMapping[str, Any],
    ) -> None:
        """
        Transforms a config by removing fields based on the provided field pointers.

        :param config: The user-provided configuration to be transformed
        """
        if self.condition and not self._filter_interpolator.eval(config):
            return

        for pointer in self.field_pointers:
            try:
                dpath.delete(config, pointer)
            except dpath.exceptions.PathNotFound:
                pass
