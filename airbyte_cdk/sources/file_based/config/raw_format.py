#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from pydantic.v1 import BaseModel, Field

from airbyte_cdk.utils.oneof_option_config import OneOfOptionConfig


class RawFormat(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Raw Files Format"
        description = "Use this format when you want to copy files without parsing them. Must be used with the 'Copy Raw Files' delivery method."
        discriminator = "filetype"

    filetype: str = Field(
        "raw",
        const=True,
    )
