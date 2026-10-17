#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from airbyte_cdk.sources.declarative.interpolation.jinja import JinjaInterpolation
from airbyte_cdk.sources.declarative.validators.validation_strategy import ValidationStrategy
from airbyte_cdk.sources.declarative.validators.validator import Validator


@dataclass
class PredicateValidator(Validator):
    """
    Validator that applies a validation strategy to a value.

    String values (including strings nested in lists and objects) are interpolated against the
    config passed to `validate`, so the value can be derived from the (already transformed) config.
    Rendered strings are parsed as Python literals when possible, so an expression rendering a list
    reaches the strategy as a list rather than as its string representation.
    """

    value: Any
    strategy: ValidationStrategy

    def __post_init__(self) -> None:
        self._interpolation = JinjaInterpolation()

    def validate(self, input_data: Optional[Mapping[str, Any]] = None) -> None:
        """
        Interpolates the value against the config and applies the validation strategy to it.

        :param input_data: The config the value is interpolated against
        :raises ValueError: If validation fails
        """
        self.strategy.validate(self._interpolate(self.value, input_data or {}))

    def _interpolate(self, value: Any, config: Mapping[str, Any]) -> Any:
        if isinstance(value, str):
            return self._interpolation.eval(value, config)
        if isinstance(value, list):
            return [self._interpolate(item, config) for item in value]
        if isinstance(value, dict):
            return {key: self._interpolate(item, config) for key, item in value.items()}
        return value
