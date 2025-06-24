#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any

from airbyte_cdk.sources.declarative.validators.validation_strategy import ValidationStrategy
from airbyte_cdk.sources.declarative.validators.validator import Validator


@dataclass
class PredicateValidator(Validator):
    """
    Validator that applies a validation strategy to a value.
    """

    value: Any
    strategy: ValidationStrategy

    def validate(self, input_data: Any) -> None:
        """
        Applies the validation strategy to the value.

        :raises ValueError: If validation fails
        """
        self.strategy.validate(self.value)
