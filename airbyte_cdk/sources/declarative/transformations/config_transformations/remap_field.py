#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass, field
from typing import Any, List, Mapping, MutableMapping

from airbyte_cdk.sources.declarative.interpolation.interpolated_mapping import InterpolatedMapping
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.transformations.config_transformations.config_transformation import (
    ConfigTransformation,
)


@dataclass
class ConfigRemapField(ConfigTransformation):
    """
    Transformation that remaps a field's value to another value based on a static map.
    """

    map: Mapping[str, Any]
    field_path: List[str]
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.field_path:
            raise Exception("field_path cannot be empty.")
        self._field_path = [
            InterpolatedString.create(path, parameters={}) for path in self.field_path
        ]
        for path_index in range(len(self.field_path)):
            if isinstance(self.field_path[path_index], str):
                self._field_path[path_index] = InterpolatedString.create(
                    self.field_path[path_index], parameters={}
                )
        self._map = InterpolatedMapping(self.map, parameters={})

    def transform(
        self,
        config: MutableMapping[str, Any],
    ) -> None:
        """
        Transforms a config by remapping a field value based on the provided map.
        If the original value is found in the map, it's replaced with the mapped value.
        If the value is not in the map, the field remains unchanged.

        :param config: The user-provided configuration to be transformed
        """
        path_components = [path.eval(config) for path in self._field_path]

        current = config
        for i, component in enumerate(path_components[:-1]):
            if component not in current:
                return
            current = current[component]

            if not isinstance(current, MutableMapping):
                return

        field_name = path_components[-1]

        mapping = self._map.eval(config=self.config or config)

        if field_name in current and current[field_name] in mapping:
            current[field_name] = mapping[current[field_name]]
