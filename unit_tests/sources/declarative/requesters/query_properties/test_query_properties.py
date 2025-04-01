# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from unittest.mock import Mock

import pytest

from airbyte_cdk.sources.declarative.requesters.query_properties import (
    GroupByKey,
    PropertiesFromEndpoint,
    QueryProperties,
)
from airbyte_cdk.sources.declarative.requesters.query_properties.property_chunking import (
    PropertyChunking,
    PropertyLimitType,
)
from airbyte_cdk.sources.types import StreamSlice

CONFIG = {}


def test_get_request_property_chunks_static_list_with_chunking():
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
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 3
    assert property_chunks[0] == ["ace", "snake", "santa"]
    assert property_chunks[1] == ["clover", "junpei", "june"]
    assert property_chunks[2] == ["seven", "lotus", "nine"]


def test_get_request_property_chunks_static_list_with_always_include_properties():
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
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 3
    assert property_chunks[0] == ["zero", "ace", "snake", "santa"]
    assert property_chunks[1] == ["zero", "clover", "junpei", "june"]
    assert property_chunks[2] == ["zero", "seven", "lotus", "nine"]


def test_get_request_property_chunks_dynamic_endpoint():
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
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(query_properties.get_request_property_chunks(stream_slice=stream_slice))

    assert len(property_chunks) == 2
    assert property_chunks[0] == ["alice", "clover", "dio", "k", "luna"]
    assert property_chunks[1] == ["phi", "quark", "sigma", "tenmyouji"]


@pytest.mark.parametrize(
    "property_limit,expected_has_multiple_chunks",
    [
        pytest.param(
            5,
            True,
            id="test_has_multiple_chunks",
        ),
        pytest.param(
            10,
            False,
            id="test_has_multiple_chunks",
        ),
    ],
)
def test_has_multiple_chunks(property_limit, expected_has_multiple_chunks):
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
            property_limit=property_limit,
            record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
            config=CONFIG,
            parameters={},
        ),
        config=CONFIG,
        parameters={},
    )

    assert (
        query_properties.has_multiple_chunks(stream_slice=stream_slice)
        == expected_has_multiple_chunks
    )
