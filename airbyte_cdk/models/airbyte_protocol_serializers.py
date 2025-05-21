# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""This module is deprecated and exists only for legacy compatibility.

Instead of importing from this module, callers should import from
`airbyte_cdk.models.airbyte_protocol` directly.

The dedicated SerDes classes are _also_ deprecated. Instead, use these methods:
- `from_dict()`
- `from_json()`
- `to_dict()`
- `to_json()`
"""

from airbyte_cdk.models.airbyte_protocol import *  # type: ignore[attr-defined]

# Deprecated. Declared here for legacy compatibility:
AirbyteStreamStateSerializer = AirbyteStreamState._serializer  # type: ignore
AirbyteStateMessageSerializer = AirbyteStateMessage._serializer  # type: ignore
AirbyteMessageSerializer = AirbyteMessage._serializer  # type: ignore
ConfiguredAirbyteCatalogSerializer = ConfiguredAirbyteCatalog._serializer  # type: ignore
ConfiguredAirbyteStreamSerializer = ConfiguredAirbyteStream._serializer  # type: ignore
ConnectorSpecificationSerializer = ConnectorSpecification._serializer  # type: ignore
