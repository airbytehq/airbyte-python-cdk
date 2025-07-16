#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass, field
from typing import Any, List, MutableMapping, Optional, Type, Union

import dpath

from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.transformations.add_fields import (
    AddedFieldDefinition,
    ParsedAddFieldDefinition,
)
from airbyte_cdk.sources.declarative.transformations.config_transformations.config_transformation import (
    ConfigTransformation,
)


@dataclass
class ConfigAddFields(ConfigTransformation):
    """
    Transformation which adds fields to a config. The path of the added field can be nested. Adding nested fields will create all
    necessary parent objects (like mkdir -p).

    This transformation has access to the config being transformed.

    Examples of instantiating this transformation via YAML:
    - type: ConfigAddFields
      fields:
        ### hardcoded constant
        - path: ["path"]
          value: "static_value"

        ### nested path
        - path: ["path", "to", "field"]
          value: "static"

        ### from config
        - path: ["derived_field"]
          value: "{{ config.original_field }}"

        ### by supplying any valid Jinja template directive or expression
        - path: ["two_times_two"]
          value: "{{ 2 * 2 }}"

    Attributes:
        fields (List[AddedFieldDefinition]): A list of transformations (path and corresponding value) that will be added to the config
    """

    fields: List[AddedFieldDefinition]
    condition: str = ""
    _parsed_fields: List[ParsedAddFieldDefinition] = field(
        init=False, repr=False, default_factory=list
    )

    def __post_init__(self) -> None:
        self._filter_interpolator = InterpolatedBoolean(condition=self.condition, parameters={})

        for add_field in self.fields:
            if len(add_field.path) < 1:
                raise ValueError(
                    f"Expected a non-zero-length path for the AddFields transformation {add_field}"
                )

            if not isinstance(add_field.value, InterpolatedString):
                if not isinstance(add_field.value, str):
                    raise ValueError(
                        f"Expected a string value for the AddFields transformation: {add_field}"
                    )
                else:
                    self._parsed_fields.append(
                        ParsedAddFieldDefinition(
                            add_field.path,
                            InterpolatedString.create(add_field.value, parameters={}),
                            value_type=add_field.value_type,
                            parameters={},
                        )
                    )
            else:
                self._parsed_fields.append(
                    ParsedAddFieldDefinition(
                        add_field.path,
                        add_field.value,
                        value_type=add_field.value_type,
                        parameters={},
                    )
                )

    def transform(
        self,
        config: MutableMapping[str, Any],
    ) -> None:
        """
        Transforms a config by adding fields based on the provided field definitions.

        :param config: The user-provided configuration to be transformed
        """
        for parsed_field in self._parsed_fields:
            valid_types = (parsed_field.value_type,) if parsed_field.value_type else None
            value = parsed_field.value.eval(config, valid_types=valid_types)
            if not self.condition or self._filter_interpolator.eval(
                config, value=value, path=parsed_field.path
            ):
                dpath.new(config, parsed_field.path, value)
