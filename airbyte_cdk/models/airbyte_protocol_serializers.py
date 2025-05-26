# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from typing import Any, Dict

from airbyte_protocol_dataclasses.models import (  # type: ignore[attr-defined] # all classes are imported to airbyte_protocol via *
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
    DestinationOperation,
)
from serpyco_rs import CustomType, Serializer


class AirbyteStateBlobType(CustomType[AirbyteStateBlob, Dict[str, Any]]):
    def serialize(self, value: AirbyteStateBlob) -> Dict[str, Any]:
        # cant use orjson.dumps() directly because private attributes are excluded, e.g. "__ab_full_refresh_sync_complete"
        return {k: v for k, v in value.__dict__.items()}

    def deserialize(self, value: Dict[str, Any]) -> AirbyteStateBlob:
        return AirbyteStateBlob(value)

    def get_json_schema(self) -> Dict[str, Any]:
        return {"type": "object"}


class DestinationOperationType(CustomType[DestinationOperation, Dict[str, Any]]):
    def serialize(self, value: DestinationOperation) -> Dict[str, Any]:
        # one field is named `schema` in the DestinationOperation which renders it as `schema_`. We need to reserialize this properly
        return {"schema" if k == "schema_" else k: v for k, v in value.__dict__.items()}

    def deserialize(self, value: Dict[str, Any]) -> DestinationOperation:
        return DestinationOperation(value)

    def get_json_schema(self) -> Dict[str, Any]:
        return {"type": "object"}


def custom_type_resolver(t: type) -> CustomType[Any, Dict[str, Any]] | None:
    if t is AirbyteStateBlob:
        return AirbyteStateBlobType()
    elif t is DestinationOperation:
        return DestinationOperationType()
    return None


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
