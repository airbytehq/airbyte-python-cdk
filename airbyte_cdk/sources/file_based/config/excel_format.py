#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

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
    sheet_name: str = Field(
        default="0",
        title="Sheet Name",
        description='The Excel worksheet to read. Use a sheet name, a zero-indexed sheet position like "0", or "*" to read all sheets.',
    )
