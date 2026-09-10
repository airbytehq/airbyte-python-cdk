# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from typing import Any, Dict, cast

from serpyco_rs import CustomType, Serializer

from .airbyte_protocol import (  # type: ignore[attr-defined] # all classes are imported to airbyte_protocol via *
    AirbyteCatalog,
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStream,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
)


class AirbyteStateBlobType(CustomType[AirbyteStateBlob, Dict[str, Any]]):
    def serialize(self, value: AirbyteStateBlob) -> Dict[str, Any]:
        # cant use orjson.dumps() directly because private attributes are excluded, e.g. "__ab_full_refresh_sync_complete"
        return {k: v for k, v in value.__dict__.items()}

    def deserialize(self, value: Dict[str, Any]) -> AirbyteStateBlob:
        return AirbyteStateBlob(value)

    def get_json_schema(self) -> Dict[str, Any]:
        return {"type": "object"}


def _blob_custom_type_resolver(
    t: type,
) -> CustomType[AirbyteStateBlob, Dict[str, Any]] | None:
    return AirbyteStateBlobType() if t is AirbyteStateBlob else None


class AirbyteStateMessageType(CustomType[AirbyteStateMessage, Dict[str, Any]]):
    _KNOWN_PROPERTIES = {
        "type",
        "stream",
        "global",
        "data",
        "sourceStats",
        "destinationStats",
    }

    def __init__(self) -> None:
        self._inner = Serializer(
            AirbyteStateMessage,
            omit_none=True,
            custom_type_resolver=_blob_custom_type_resolver,
        )

    def serialize(self, value: AirbyteStateMessage) -> Dict[str, Any]:
        result = cast(Dict[str, Any], self._inner.dump(value))
        result.update(
            {
                key: property_value
                for key, property_value in getattr(value, "additional_properties", {}).items()
                if key not in self._KNOWN_PROPERTIES
            }
        )
        return result

    def deserialize(self, value: Dict[str, Any]) -> AirbyteStateMessage:
        message = self._inner.load(value)
        additional_properties = {
            key: property_value
            for key, property_value in value.items()
            if key not in self._KNOWN_PROPERTIES
        }
        if additional_properties:
            message.additional_properties = additional_properties  # type: ignore[attr-defined]
        return message

    def get_json_schema(self) -> Dict[str, Any]:
        return self._inner.get_json_schema()


def custom_type_resolver(t: type) -> CustomType[Any, Any] | None:
    if t is AirbyteStateMessage:
        return AirbyteStateMessageType()
    return _blob_custom_type_resolver(t)


AirbyteCatalogSerializer = Serializer(AirbyteCatalog, omit_none=True)
AirbyteStreamSerializer = Serializer(AirbyteStream, omit_none=True)
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
