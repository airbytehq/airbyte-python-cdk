# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from typing import List
from unittest.mock import Mock

from airbyte_protocol_dataclasses.models import (
    AirbyteStream,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)

from airbyte_cdk.sources.declarative.requesters.query_properties import (
    PropertiesFromEndpoint,
    QueryProperties,
)
from airbyte_cdk.sources.declarative.requesters.query_properties.property_chunking import (
    PropertyChunking,
    PropertyLimitType,
)
from airbyte_cdk.sources.declarative.requesters.query_properties.property_selector import (
    JsonSchemaPropertySelector,
)
from airbyte_cdk.sources.declarative.requesters.query_properties.strategies import GroupByKey
from airbyte_cdk.sources.declarative.transformations import RemoveFields
from airbyte_cdk.sources.types import StreamSlice

CONFIG = {}


def _create_configured_airbyte_stream(configured_properties: List[str]):
    return ConfiguredAirbyteStream(
        sync_mode=SyncMode.full_refresh,
        destination_sync_mode=DestinationSyncMode.append,
        stream=AirbyteStream(
            name="nonary_game_01",
            namespace=None,
            json_schema={
                "properties": {
                    property: {"type": ["null", "string"]} for property in configured_properties
                }
            },
            supported_sync_modes=[SyncMode.full_refresh],
        ),
    )


def test_get_request_property_chunks_static_list_with_chunking_property_selection():
    configured_airbyte_stream = _create_configured_airbyte_stream(
        ["santa", "clover", "junpei", "june", "remove_me"]
    )

    stream_slice = StreamSlice(cursor_slice={}, partition={})

    query_properties = QueryProperties(
        property_list=[
            "ace",
            "snake",
            "santa",
            "clover",
            "junpei",
            "june",
            "seven",
            "lotus",
            "nine",
            "remove_me",
        ],
        always_include_properties=None,
        property_chunking=PropertyChunking(
            property_limit_type=PropertyLimitType.property_count,
            property_limit=3,
            record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
            config=CONFIG,
            parameters={},
        ),
        property_selector=JsonSchemaPropertySelector(
            configured_stream=configured_airbyte_stream,
            properties_transformations=[
                RemoveFields(field_pointers=[["remove_me"]], parameters={}),
            ],
            config=CONFIG,
            parameters={},
        ),
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 2
    assert property_chunks[0] == ["santa", "clover", "junpei"]
    assert property_chunks[1] == ["june"]


def test_get_request_property_chunks_static_list_with_always_include_properties():
    configured_airbyte_stream = _create_configured_airbyte_stream(
        [
            "ace",
            "snake",
            "santa",
            "clover",
            "junpei",
            "june",
            "seven",
            "lotus",
            "nine",
        ]
    )

    stream_slice = StreamSlice(cursor_slice={}, partition={})

    query_properties = QueryProperties(
        property_list=[
            "ace",
            "snake",
            "santa",
            "clover",
            "junpei",
            "june",
            "seven",
            "lotus",
            "nine",
        ],
        always_include_properties=["zero"],
        property_chunking=PropertyChunking(
            property_limit_type=PropertyLimitType.property_count,
            property_limit=3,
            record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
            config=CONFIG,
            parameters={},
        ),
        property_selector=JsonSchemaPropertySelector(
            configured_stream=configured_airbyte_stream,
            properties_transformations=[
                RemoveFields(field_pointers=[["remove_me"]], parameters={}),
            ],
            config=CONFIG,
            parameters={},
        ),
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 3
    assert property_chunks[0] == ["zero", "ace", "snake", "santa"]
    assert property_chunks[1] == ["zero", "clover", "junpei", "june"]
    assert property_chunks[2] == ["zero", "seven", "lotus", "nine"]


def test_get_request_no_property_chunking_selected_properties_always_include_properties():
    configured_airbyte_stream = _create_configured_airbyte_stream(
        ["santa", "clover", "junpei", "june", "remove_me"]
    )

    stream_slice = StreamSlice(cursor_slice={}, partition={})

    query_properties = QueryProperties(
        property_list=[
            "ace",
            "snake",
            "santa",
            "clover",
            "junpei",
            "june",
            "seven",
            "lotus",
            "nine",
        ],
        always_include_properties=["zero"],
        property_chunking=None,
        property_selector=JsonSchemaPropertySelector(
            configured_stream=configured_airbyte_stream,
            properties_transformations=[
                RemoveFields(field_pointers=[["remove_me"]], parameters={}),
            ],
            config=CONFIG,
            parameters={},
        ),
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 1
    assert property_chunks[0] == ["zero", "santa", "clover", "junpei", "june"]


def test_get_request_no_property_chunking_always_include_properties():
    configured_airbyte_stream = _create_configured_airbyte_stream(
        [
            "ace",
            "snake",
            "santa",
            "clover",
            "junpei",
            "june",
            "seven",
            "lotus",
            "nine",
        ]
    )

    stream_slice = StreamSlice(cursor_slice={}, partition={})

    query_properties = QueryProperties(
        property_list=[
            "ace",
            "snake",
            "santa",
            "clover",
            "junpei",
            "june",
            "seven",
            "lotus",
            "nine",
        ],
        always_include_properties=["zero"],
        property_chunking=None,
        property_selector=JsonSchemaPropertySelector(
            configured_stream=configured_airbyte_stream,
            properties_transformations=[
                RemoveFields(field_pointers=[["remove_me"]], parameters={}),
            ],
            config=CONFIG,
            parameters={},
        ),
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 1
    assert property_chunks[0] == [
        "zero",
        "ace",
        "snake",
        "santa",
        "clover",
        "junpei",
        "june",
        "seven",
        "lotus",
        "nine",
    ]


def test_get_request_property_chunks_dynamic_endpoint():
    configured_airbyte_stream = _create_configured_airbyte_stream(
        ["alice", "clover", "dio", "k", "luna", "phi", "quark", "sigma", "tenmyouji"]
    )

    stream_slice = StreamSlice(cursor_slice={}, partition={})

    properties_from_endpoint_mock = Mock(spec=PropertiesFromEndpoint)
    properties_from_endpoint_mock.get_properties_from_endpoint.return_value = iter(
        ["alice", "clover", "dio", "k", "luna", "phi", "quark", "sigma", "tenmyouji"]
    )

    query_properties = QueryProperties(
        property_list=properties_from_endpoint_mock,
        always_include_properties=None,
        property_chunking=PropertyChunking(
            property_limit_type=PropertyLimitType.property_count,
            property_limit=5,
            record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
            config=CONFIG,
            parameters={},
        ),
        property_selector=JsonSchemaPropertySelector(
            configured_stream=configured_airbyte_stream,
            properties_transformations=[
                RemoveFields(field_pointers=[["remove_me"]], parameters={}),
            ],
            config=CONFIG,
            parameters={},
        ),
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 2
    assert property_chunks[0] == ["alice", "clover", "dio", "k", "luna"]
    assert property_chunks[1] == ["phi", "quark", "sigma", "tenmyouji"]


def test_get_request_property_chunks_with_configured_catalog_static_list():
    stream_slice = StreamSlice(cursor_slice={}, partition={})
    # Simulate configured_airbyte_stream whose json_schema only enables 'luna', 'phi', 'sigma'
    configured_airbyte_stream = _create_configured_airbyte_stream(
        ["santa", "clover", "junpei", "june"]
    )

    query_properties = QueryProperties(
        property_list=[
            "ace",
            "snake",
            "santa",
            "clover",
            "junpei",
            "june",
            "seven",
            "lotus",
            "nine",
        ],
        always_include_properties=None,
        property_chunking=PropertyChunking(
            property_limit_type=PropertyLimitType.property_count,
            property_limit=3,
            record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
            config=CONFIG,
            parameters={},
        ),
        property_selector=JsonSchemaPropertySelector(
            configured_stream=configured_airbyte_stream,
            properties_transformations=[
                RemoveFields(field_pointers=[["remove_me"]], parameters={}),
            ],
            config=CONFIG,
            parameters={},
        ),
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 2
    assert property_chunks[0] == ["santa", "clover", "junpei"]
    assert property_chunks[1] == ["june"]


def test_get_request_property_chunks_with_configured_catalog_dynamic_endpoint():
    configured_airbyte_stream = _create_configured_airbyte_stream(
        ["luna", "phi", "sigma", "remove_me"]
    )

    stream_slice = StreamSlice(cursor_slice={}, partition={})

    properties_from_endpoint_mock = Mock(spec=PropertiesFromEndpoint)
    properties_from_endpoint_mock.get_properties_from_endpoint.return_value = iter(
        ["alice", "clover", "dio", "k", "luna", "phi", "quark", "sigma", "tenmyouji"]
    )

    query_properties = QueryProperties(
        property_list=properties_from_endpoint_mock,
        always_include_properties=None,
        property_chunking=PropertyChunking(
            property_limit_type=PropertyLimitType.property_count,
            property_limit=5,
            record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
            config=CONFIG,
            parameters={},
        ),
        property_selector=JsonSchemaPropertySelector(
            configured_stream=configured_airbyte_stream,
            properties_transformations=[
                RemoveFields(field_pointers=[["remove_me"]], parameters={}),
            ],
            config=CONFIG,
            parameters={},
        ),
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 1
    assert property_chunks[0] == ["luna", "phi", "sigma"]


def test_get_request_property_no_property_selection():
    stream_slice = StreamSlice(cursor_slice={}, partition={})

    query_properties = QueryProperties(
        property_list=[
            "ace",
            "snake",
            "santa",
            "clover",
            "junpei",
            "june",
            "seven",
            "lotus",
            "nine",
        ],
        always_include_properties=None,
        property_chunking=PropertyChunking(
            property_limit_type=PropertyLimitType.property_count,
            property_limit=3,
            record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
            config=CONFIG,
            parameters={},
        ),
        property_selector=None,
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 3
    assert property_chunks[0] == ["ace", "snake", "santa"]
    assert property_chunks[1] == ["clover", "junpei", "june"]
    assert property_chunks[2] == ["seven", "lotus", "nine"]
