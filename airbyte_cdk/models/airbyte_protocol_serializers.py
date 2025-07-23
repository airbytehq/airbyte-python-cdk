# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
import sys
from typing import Any, Dict, Type, TypeVar

import orjson
from pydantic import ValidationError

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

USE_RUST_BACKEND = sys.platform != "emscripten"
"""When run in WASM, use the pure Python backend for serpyco."""


T = TypeVar("T")

class CustomSerializer:
    """Custom serializer that mimics serpyco-rs Serializer API"""

    def __init__(
        self,
        model_class: Type[T],
        omit_none: bool = False,
        custom_type_resolver: Callable | None = None,
    ):
        self.model_class = model_class
        self.omit_none = omit_none
        self.custom_type_resolver = custom_type_resolver

    def dump(self, obj: T) -> Dict[str, Any]:
        """Convert dataclass to dictionary, omitting None values if configured"""
        if hasattr(obj, "__dict__"):
            result = {}
            for key, value in obj.__dict__.items():
                if self.omit_none and value is None:
                    continue
                # Handle custom types like AirbyteStateBlob
                if self.custom_type_resolver and hasattr(value, "__class__"):
                    custom_handler = self.custom_type_resolver(value.__class__)
                    if custom_handler:
                        value = custom_handler.serialize(value)
                # Recursively handle nested objects
                if hasattr(value, "__dict__"):
                    value = self._serialize_nested(value)
                elif isinstance(value, list):
                    value = [
                        self._serialize_nested(item) if hasattr(item, "__dict__") else item
                        for item in value
                    ]
                result[key] = value
            return result
        return obj.__dict__ if hasattr(obj, "__dict__") else {}

    def load(self, data: Dict[str, Any]) -> T:
        """Convert dictionary to dataclass instance"""
        # Handle custom types
        return self.model_class(**data)

    def _serialize_nested(self, obj: Any) -> Any:
        """Helper to serialize nested objects"""
        if hasattr(obj, "__dict__"):
            result = {}
            for key, value in obj.__dict__.items():
                if self.omit_none and value is None:
                    continue
                result[key] = value
            return result
        return obj


if USE_RUST_BACKEND:
    from serpyco_rs import CustomType, Serializer  # type: ignore[import]

SERIALIZER = Serializer if USE_RUST_BACKEND else CustomSerializer

# Making this a no-op for now:
custom_type_resolver = None

# No idea why this is here. Commenting out for now.
# def custom_type_resolver(t: type) -> AirbyteStateBlobType | None:
#     return AirbyteStateBlobType() if t is AirbyteStateBlob else None
#
# class AirbyteStateBlobType(CustomType[AirbyteStateBlob, Dict[str, Any]]):
#     def serialize(self, value: AirbyteStateBlob) -> Dict[str, Any]:
#         # cant use orjson.dumps() directly because private attributes are excluded, e.g. "__ab_full_refresh_sync_complete"
#         return {k: v for k, v in value.__dict__.items()}

#     def deserialize(self, value: Dict[str, Any]) -> AirbyteStateBlob:
#         return AirbyteStateBlob(value)

#     def get_json_schema(self) -> Dict[str, Any]:
#         return {"type": "object"}

# Create serializer instances maintaining the same API
AirbyteCatalogSerializer = SERIALIZER(AirbyteCatalog, omit_none=True)
AirbyteStreamSerializer = SERIALIZER(AirbyteStream, omit_none=True)
AirbyteStreamStateSerializer = SERIALIZER(
    AirbyteStreamState, omit_none=True, custom_type_resolver=custom_type_resolver
)
AirbyteStateMessageSerializer = SERIALIZER(
    AirbyteStateMessage, omit_none=True, custom_type_resolver=custom_type_resolver
)
AirbyteMessageSerializer = SERIALIZER(
    AirbyteMessage, omit_none=True, custom_type_resolver=custom_type_resolver
)
ConfiguredAirbyteCatalogSerializer = SERIALIZER(ConfiguredAirbyteCatalog, omit_none=True)
ConfiguredAirbyteStreamSerializer = SERIALIZER(ConfiguredAirbyteStream, omit_none=True)
ConnectorSpecificationSerializer = SERIALIZER(ConnectorSpecification, omit_none=True)
