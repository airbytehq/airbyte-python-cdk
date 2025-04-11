# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

# THIS IS A STATIC CLASS MODEL USED TO DISPLAY DEPRECATION WARNINGS
# WHEN DEPRECATED FIELDS ARE ACCESSED

import warnings
from typing import Any

from pydantic.v1 import BaseModel

from airbyte_cdk.models import (
    AirbyteLogMessage,
    AirbyteMessage,
    Level,
    Type,
)

# format the warning message
warnings.formatwarning = (
    lambda message, category, *args, **kwargs: f"{category.__name__}: {message}"
)

FIELDS_TAG = "__fields__"
DEPRECATED = "deprecated"
DEPRECATION_MESSAGE = "deprecation_message"


class BaseModelWithDeprecations(BaseModel):
    """
    Pydantic BaseModel that warns when deprecated fields are accessed.
    """

    def _deprecated_warning(self, field_name: str, message: str) -> None:
        """
        Show a warning message for deprecated fields (to stdout).
        Args:
            field_name (str): Name of the deprecated field.
            message (str): Warning message to be displayed.
        """

        warnings.warn(
            f"Component type: `{self.__class__.__name__}`. Field '{field_name}' is deprecated. {message}",
            DeprecationWarning,
        )

        # print(
        #     AirbyteMessage(
        #         type=Type.LOG,
        #         log=AirbyteLogMessage(
        #             level=Level.WARN,
        #             message=f"Component type: `{self.__class__.__name__}`. Field '{field_name}' is deprecated. {message}",
        #         ),
        #     )
        # )

    def __init__(self, **data: Any) -> None:
        """
        Show warnings for deprecated fields during component initialization.
        """

        model_fields = self.__fields__

        for field_name in data:
            if field_name in model_fields:
                if model_fields[field_name].field_info.extra.get(DEPRECATED, False):
                    message = model_fields[field_name].field_info.extra.get(DEPRECATION_MESSAGE, "")
                    self._deprecated_warning(field_name, message)

        # Call the parent constructor
        super().__init__(**data)

    def __getattribute__(self, name: str) -> Any:
        """
        Show warnings for deprecated fields during field usage.
        """

        value = super().__getattribute__(name)

        if name == FIELDS_TAG:
            try:
                model_fields = super().__getattribute__(FIELDS_TAG)
                field_info = model_fields.get(name)
                if field_info and field_info.field_info.extra.get(DEPRECATED):
                    self._deprecated_warning(name, field_info)
            except (AttributeError, KeyError):
                pass

        return value
