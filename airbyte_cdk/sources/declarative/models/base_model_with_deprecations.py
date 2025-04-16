# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

# THIS IS A STATIC CLASS MODEL USED TO DISPLAY DEPRECATION WARNINGS
# WHEN DEPRECATED FIELDS ARE ACCESSED

import warnings
from typing import Any, List

from pydantic.v1 import BaseModel

from airbyte_cdk.models import (
    AirbyteLogMessage,
    Level,
)

# format the warning message
warnings.formatwarning = (
    lambda message, category, *args, **kwargs: f"{category.__name__}: {message}"
)

FIELDS_TAG = "__fields__"
DEPRECATED = "deprecated"
DEPRECATION_MESSAGE = "deprecation_message"
DEPRECATION_LOGS_TAG = "_deprecation_logs"


class BaseModelWithDeprecations(BaseModel):
    """
    Pydantic BaseModel that warns when deprecated fields are accessed.
    The deprecation message is stored in the field's extra attributes.
    This class is used to create models that can have deprecated fields
    and show warnings when those fields are accessed or initialized.

    The `_deprecation_logs` attribute is stored in the model itself.
    The collected deprecation warnings are further propagated to the Airbyte log messages,
    during the component creation process, in `model_to_component._collect_model_deprecations()`.

    The component implementation is not responsible for handling the deprecation warnings,
    since the deprecation warnings are already handled in the model itself.
    """

    class Config:
        """
        Allow extra fields in the model. In case the model restricts extra fields.
        """

        extra = "allow"

    def __init__(self, **data: Any) -> None:
        """
        Show warnings for deprecated fields during component initialization.
        """
        # placeholder for deprecation logs
        self._deprecation_logs: List[AirbyteLogMessage] = []

        model_fields = self.__fields__
        for field_name in data:
            if field_name in model_fields:
                is_deprecated_field = model_fields[field_name].field_info.extra.get(
                    DEPRECATED, False
                )
                if is_deprecated_field:
                    deprecation_message = model_fields[field_name].field_info.extra.get(
                        DEPRECATION_MESSAGE, ""
                    )
                    self._deprecated_warning(field_name, deprecation_message)

        # Call the parent constructor
        super().__init__(**data)

    def __getattribute__(self, name: str) -> Any:
        """
        Show warnings for deprecated fields during field usage.
        """

        value = super().__getattribute__(name)
        try:
            model_fields = super().__getattribute__(FIELDS_TAG)
            field_info = model_fields.get(name)
            is_deprecated_field = (
                field_info.field_info.extra.get(DEPRECATED, False) if field_info else False
            )
            if is_deprecated_field:
                deprecation_message = field_info.field_info.extra.get(DEPRECATION_MESSAGE, "")
                self._deprecated_warning(name, deprecation_message)
        except (AttributeError, KeyError):
            pass

        return value

    def _deprecated_warning(self, field_name: str, message: str) -> None:
        """
        Show a warning message for deprecated fields (to stdout).
        Args:
            field_name (str): Name of the deprecated field.
            message (str): Warning message to be displayed.
        """

        message = f"Component type: `{self.__class__.__name__}`. Field '{field_name}' is deprecated. {message}"

        # Emit a warning message for deprecated fields (to stdout) (Python Default behavior)
        warnings.warn(message, DeprecationWarning)

        # Add the deprecation message to the Airbyte log messages,
        # this logs are displayed in the Connector Builder.
        self._deprecation_logs.append(
            AirbyteLogMessage(level=Level.WARN, message=message),
        )
