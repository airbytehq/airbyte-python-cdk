#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any

from airbyte_cdk.sources.declarative.validators.validation_strategy import ValidationStrategy


@dataclass
class ValidateIsOfType(ValidationStrategy):
    """
    Validates that a value is of a specified type.
    """

    expected_type: Any

    def validate(self, value: Any) -> None:
        """
        Checks if the value is of the expected type.

        :param value: The value to validate
        :raises ValueError: If the value is not of the expected type
        """
        if not isinstance(value, self.expected_type):
            raise ValueError(f"Value '{value}' is not of type {self.expected_type.__name__}")
