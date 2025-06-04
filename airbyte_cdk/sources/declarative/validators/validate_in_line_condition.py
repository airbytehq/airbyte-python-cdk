#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any

from jinja2.exceptions import TemplateError

from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean
from airbyte_cdk.sources.declarative.validators.validation_strategy import ValidationStrategy
from airbyte_cdk.sources.types import Config


@dataclass
class ValidateInLineCondition(ValidationStrategy):
    """
    Validation strategy that evaluates the argument as an InterpolatedBoolean.
    """

    config: Config

    def validate(self, value: Any) -> None:
        """
        Validates the argument as an InterpolatedBoolean.

        :param value: The value to validate
        :raises ValueError: If the condition is not a string or evaluates to False
        """

        if isinstance(value, str):
            interpolated_condition = InterpolatedBoolean(value, parameters={})
            try:
                result = interpolated_condition.eval(self.config)
            except TemplateError as e:
                raise ValueError(f"Invalid jinja expression: {value}.") from e
            except Exception as e:
                raise ValueError(f"Unexpected error evaluating condition: {value}.") from e

            if not result:
                raise ValueError(f"Condition evaluated to False: {value}.")
        else:
            raise ValueError(f"Invalid condition argument: {value}. Should be a string.")
