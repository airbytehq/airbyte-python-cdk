# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from typing import Any

from serpyco_rs import CustomType, Serializer

from .airbyte_protocol import (  # type: ignore[attr-defined] # all classes are imported to airbyte_protocol via *
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
)


class AirbyteStateBlobType(CustomType[AirbyteStateBlob, dict[str, Any]]):
    def serialize(self, value: AirbyteStateBlob) -> dict[str, Any]:
        # cant use orjson.dumps() directly because private attributes are excluded, e.g. "__ab_full_refresh_sync_complete"
        return {k: v for k, v in value.__dict__.items()}

    def deserialize(self, value: dict[str, Any]) -> AirbyteStateBlob:
        return AirbyteStateBlob(value)

    def get_json_schema(self) -> dict[str, Any]:
        return {"type": "object"}


def custom_type_resolver(t: type) -> CustomType[AirbyteStateBlob, dict[str, Any]] | None:
    return AirbyteStateBlobType() if t is AirbyteStateBlob else None


AirbyteStreamStateSerializer = Serializer(
    AirbyteStreamState, omit_none=True, custom_type_resolver=custom_type_resolver
)
AirbyteStateMessageSerializer = Serializer(
    AirbyteStateMessage, omit_none=True, custom_type_resolver=custom_type_resolver
)
AirbyteMessageSerializer = Serializer(
    AirbyteMessage, omit_none=True, custom_type_resolver=custom_type_resolver
)
ConfiguredAirbyteCatalogSerializer = Serializer(ConfiguredAirbyteCatalog, omit_none=True)
ConfiguredAirbyteStreamSerializer = Serializer(ConfiguredAirbyteStream, omit_none=True)
ConnectorSpecificationSerializer = Serializer(ConnectorSpecification, omit_none=True)
