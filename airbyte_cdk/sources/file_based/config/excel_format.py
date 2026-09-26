#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic.v1 import BaseModel, Field, root_validator

from airbyte_cdk.utils.oneof_option_config import OneOfOptionConfig


class SheetsToSync(str, Enum):
    FIRST_SHEET_ONLY = "first_sheet_only"
    ALL_SHEETS = "all_sheets"


class ExcelFormat(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Excel Format"
        discriminator = "filetype"

    filetype: str = Field(
        "excel",
        const=True,
    )
    sheets_to_sync: SheetsToSync = Field(
        default=SheetsToSync.FIRST_SHEET_ONLY,
        title="Sheets to Sync",
        description=(
            "Controls which sheets (tabs) of the workbook are synced. "
            "First Sheet Only reads only the first sheet. "
            "All Sheets reads every sheet and adds _ab_sheet_name to each record."
        ),
        order=1,
    )
    sheet_names: Optional[List[str]] = Field(
        default=None,
        title="Sheet Names",
        description=(
            "Optional list of sheet names to sync. When set, only these sheets are read and "
            "_ab_sheet_name is added to each record."
        ),
        order=2,
    )

    @root_validator
    def validate_sheet_selection(
        cls,
        values: Dict[str, Any],  # noqa: N805  # Pydantic validators use cls, not self
    ) -> Dict[str, Any]:
        if values.get("sheet_names") == []:
            raise ValueError("Sheet Names cannot be empty.")
        return values
