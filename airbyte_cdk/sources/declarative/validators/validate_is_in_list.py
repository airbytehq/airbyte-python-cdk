#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any

from airbyte_cdk.sources.declarative.validators.validation_strategy import ValidationStrategy


@dataclass
class ValidateIsInList(ValidationStrategy):
    """
    Validates that a value is in a list of supported values.
    """

    supported_values: list[Any]

    def validate(self, value: Any) -> None:
        """
        Checks if the value is in the list of supported values.

        :param value: The value to validate
        :raises ValueError: If the value is not in the list of supported values
        """
        if value not in self.supported_values:
            supported_values_str = ", ".join(str(v) for v in self.supported_values)
            raise ValueError(f"Value '{value}' not in supported values: [{supported_values_str}]")
