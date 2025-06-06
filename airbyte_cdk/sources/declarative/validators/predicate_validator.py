#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any

from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean
from airbyte_cdk.sources.declarative.validators.validation_strategy import ValidationStrategy
from airbyte_cdk.sources.types import Config


@dataclass
class PredicateValidator:
    """
    Validator that applies a validation strategy to a value.
    """

    value: Any
    strategy: ValidationStrategy
    config: Config
    condition: str

    def __post_init__(self) -> None:
        self._interpolated_condition = InterpolatedBoolean(condition=self.condition, parameters={})

    def validate(self) -> None:
        """
        Applies the validation strategy to the value.

        :raises ValueError: If validation fails
        """
        if self.condition and not self._interpolated_condition.eval(self.config):
            return

        self.strategy.validate(self.value)
