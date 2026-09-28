# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from typing import List, Optional, Set

import pytest
from airbyte_protocol_dataclasses.models import (
    AirbyteStream,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)

from airbyte_cdk.sources.declarative.requesters.query_properties.property_selector import (
    JsonSchemaPropertySelector,
)
from airbyte_cdk.sources.declarative.transformations import RecordTransformation, RemoveFields
from airbyte_cdk.sources.declarative.transformations.keys_replace_transformation import (
    KeysReplaceTransformation,
)

CONFIG = {}


def _create_configured_airbyte_stream(configured_properties: List[str]):
    return ConfiguredAirbyteStream(
        sync_mode=SyncMode.full_refresh,
        destination_sync_mode=DestinationSyncMode.append,
        stream=AirbyteStream(
            name="players",
            namespace=None,
            json_schema={
                "properties": {
                    property: {"type": ["null", "string"]} for property in configured_properties
                }
            },
            supported_sync_modes=[SyncMode.full_refresh],
        ),
    )


@pytest.mark.parametrize(
    "configured_stream, transformations, expected_properties",
    [
        pytest.param(
            # Test case 1: configured stream with 5 fields, RemoveFields and KeysReplaceTransformation
            _create_configured_airbyte_stream(
                ["id", "first_name", "last_name", "number", "team", "properties_statistics"]
            ),
            [
                RemoveFields(field_pointers=[["id"]], parameters={}),
                KeysReplaceTransformation(old="properties_", new="", parameters={}),
            ],
            {"first_name", "last_name", "number", "team", "statistics"},
            id="test_select_properties_with_transformations",
        ),
        pytest.param(
            None,
            [],
            None,
            id="configured_stream_is_none",
        ),
        pytest.param(
            # Test case 3: configured stream has no properties
            ConfiguredAirbyteStream(
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
                stream=AirbyteStream(
                    name="players",
                    namespace=None,
                    json_schema={},
                    supported_sync_modes=[SyncMode.full_refresh],
                ),
            ),
            [],
            set(),
            id="configured_stream_no_properties_key_in_json_schema",
        ),
    ],
)
def test_select_properties(
    configured_stream: Optional[ConfiguredAirbyteStream],
    transformations: List[RecordTransformation],
    expected_properties: Optional[Set[str]],
):
    json_schema_property_selector = JsonSchemaPropertySelector(
        configured_stream=configured_stream,
        properties_transformations=transformations,
        config=CONFIG,
        parameters={},
    )

    selected_properties = json_schema_property_selector.select()

    assert selected_properties == expected_properties
