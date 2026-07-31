#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from typing import Optional

from pydantic.v1 import BaseModel, Field

from airbyte_cdk.utils.oneof_option_config import OneOfOptionConfig


class ExcelFormat(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Excel Format"
        discriminator = "filetype"

    filetype: str = Field(
        "excel",
        const=True,
    )
    sheet_name: Optional[str] = Field(
        None,
        title="Sheet Name",
        description=(
            "The worksheet to read from each workbook. Leave empty to read only the first "
            'worksheet. Enter an exact worksheet name to read that worksheet, or "*" to read '
            "every worksheet in the workbook. Worksheet names are case-sensitive."
        ),
        examples=["Sheet1", "*"],
    )
