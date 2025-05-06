#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any, List, Union

import dpath.util

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.validators.validator import Validator
from airbyte_cdk.sources.declarative.validators.validation_strategy import ValidationStrategy


@dataclass
class DpathValidator(Validator):
    """
    Validator that extracts a value at a specific path in the input data
    and applies a validation strategy to it.
    """

    field_path: List[Union[InterpolatedString, str]]
    strategy: ValidationStrategy

    def __post_init__(self) -> None:
        self._field_path = [
            InterpolatedString.create(path, parameters={}) for path in self.field_path
        ]
        for path_index in range(len(self.field_path)):
            if isinstance(self.field_path[path_index], str):
                self._field_path[path_index] = InterpolatedString.create(
                    self.field_path[path_index], parameters={}
                )

    def validate(self, input_data: dict[str, Any]) -> None:
        """
        Extracts the value at the specified path and applies the validation strategy.

        :param input_data: Dictionary containing the data to validate
        :raises ValueError: If the path doesn't exist or validation fails
        """
        try:
            path = [path.eval(input_data) for path in self._field_path]
            value = dpath.util.get(input_data, path)
            self.strategy.validate(value)
        except KeyError:
            raise ValueError(f"Path '{self.field_path}' not found in the input data")
        except Exception as e:
            raise ValueError(f"Error validating path '{self.field_path}': {e}")
