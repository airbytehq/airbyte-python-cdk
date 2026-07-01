#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import List, Literal, Optional, Union

from pydantic.v1 import BaseModel, Field

from airbyte_cdk.utils.oneof_option_config import OneOfOptionConfig


class LocalProcessingConfigModel(BaseModel):
    mode: Literal["local"] = Field("local", const=True)

    class Config(OneOfOptionConfig):
        title = "Local"
        description = "Process files locally using MarkItDown. This is the default option."
        discriminator = "mode"


class APIParameterConfigModel(BaseModel):
    """Deprecated: API processing is no longer supported. Retained for config backward compatibility."""

    name: str = Field(
        title="Parameter name",
        description="The name of the unstructured API parameter to use",
        examples=["combine_under_n_chars", "languages"],
    )
    value: str = Field(
        title="Value", description="The value of the parameter", examples=["true", "hi_res"]
    )


class APIProcessingConfigModel(BaseModel):
    """Deprecated: API processing is no longer supported. Retained for config backward compatibility."""

    mode: Literal["api"] = Field("api", const=True)

    api_key: str = Field(
        default="",
        always_show=True,
        title="API Key",
        airbyte_secret=True,
        description="Deprecated: API processing is no longer supported.",
    )

    api_url: str = Field(
        default="https://api.unstructured.io",
        title="API URL",
        always_show=True,
        description="Deprecated: API processing is no longer supported.",
        examples=["https://api.unstructured.com"],
    )

    parameters: Optional[List[APIParameterConfigModel]] = Field(
        default=[],
        always_show=True,
        title="Additional URL Parameters",
        description="Deprecated: API processing is no longer supported.",
    )

    class Config(OneOfOptionConfig):
        title = "via API"
        description = "Deprecated: API processing is no longer supported. All processing is now done locally using MarkItDown."
        discriminator = "mode"


class UnstructuredFormat(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Document Format"
        description = "Extract text from document formats (.pdf, .docx, .md, .pptx) and emit as one record per file."
        discriminator = "filetype"

    filetype: str = Field(
        "unstructured",
        const=True,
    )

    skip_unprocessable_files: bool = Field(
        default=True,
        title="Skip Unprocessable Files",
        description="If true, skip files that cannot be parsed and pass the error message along as the _ab_source_file_parse_error field. If false, fail the sync.",
        always_show=True,
    )

    strategy: str = Field(
        always_show=True,
        order=0,
        default="auto",
        title="Parsing Strategy",
        enum=["auto", "fast", "ocr_only", "hi_res"],
        description="Deprecated: This field is ignored. All parsing is now handled by MarkItDown.",
    )

    processing: Union[
        LocalProcessingConfigModel,
        APIProcessingConfigModel,
    ] = Field(
        default=LocalProcessingConfigModel(mode="local"),
        title="Processing",
        description="Deprecated: All processing is now done locally using MarkItDown.",
        discriminator="mode",
        type="object",
    )
