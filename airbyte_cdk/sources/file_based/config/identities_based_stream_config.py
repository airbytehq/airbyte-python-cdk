from typing import Literal

from pydantic.v1 import BaseModel, Field


class IdentitiesStreamConfig(BaseModel):
    name: Literal["identities"] = Field("identities", const=True, airbyte_hidden=True)
    domain: str = Field(title="Domain", description="The domain of the identities.")
