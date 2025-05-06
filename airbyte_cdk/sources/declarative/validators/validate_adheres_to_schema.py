#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any

import jsonschema

from airbyte_cdk.sources.declarative.validators.validation_strategy import ValidationStrategy


@dataclass
class ValidateAdheresToSchema(ValidationStrategy):
    """
    Validates that a value adheres to a specified JSON schema.
    """

    schema: dict[str, Any]

    def validate(self, value: Any) -> None:
        """
        Validates the value against the JSON schema.

        :param value: The value to validate
        :raises ValueError: If the value does not adhere to the schema
        """
        try:
            jsonschema.validate(instance=value, schema=self.schema)
        except jsonschema.ValidationError as e:
            raise ValueError(f"JSON schema validation error: {e.message}")
