# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
from typing import Set

import pytest

from airbyte_cdk.sources.declarative.requesters.query_properties import PropertyChunking
from airbyte_cdk.sources.declarative.requesters.query_properties.property_chunking import (
    PropertyLimitType,
)
from airbyte_cdk.sources.declarative.requesters.query_properties.strategies import GroupByKey
from airbyte_cdk.sources.types import Record

CONFIG = {}


@pytest.mark.parametrize(
    "property_fields,always_include_properties,property_limit_type,property_limit,expected_property_chunks",
    [
        pytest.param(
            ["rick", "chelsea", "victoria", "tim", "saxon", "lochlan", "piper"],
            None,
            PropertyLimitType.property_count,
            2,
            [["rick", "chelsea"], ["victoria", "tim"], ["saxon", "lochlan"], ["piper"]],
            id="test_property_chunking",
        ),
        pytest.param(
            ["rick", "chelsea", "victoria", "tim"],
            ["mook", "gaitok"],
            PropertyLimitType.property_count,
            2,
            [["mook", "gaitok", "rick", "chelsea"], ["mook", "gaitok", "victoria", "tim"]],
            id="test_property_chunking_with_always_include_fields",
        ),
        pytest.param(
            ["rick", "chelsea", "victoria", "tim", "saxon", "lochlan", "piper"],
            None,
            PropertyLimitType.property_count,
            None,
            [["rick", "chelsea", "victoria", "tim", "saxon", "lochlan", "piper"]],
            id="test_property_chunking_no_limit",
        ),
        pytest.param(
            ["kate", "laurie", "jaclyn"],
            None,
            PropertyLimitType.characters,
            20,
            [["kate", "laurie"], ["jaclyn"]],
            id="test_property_chunking_limit_characters",
        ),
        pytest.param(
            ["laurie", "jaclyn", "kaitlin"],
            None,
            PropertyLimitType.characters,
            17,  # laurie%2Cjaclyn%2C == 18, so this will create separate chunks
            [["laurie"], ["jaclyn"], ["kaitlin"]],
            id="test_property_chunking_includes_extra_delimiter",
        ),
        pytest.param(
            [],
            None,
            PropertyLimitType.property_count,
            5,
            [[]],
            id="test_property_chunking_no_properties",
        ),
    ],
)
def test_get_request_property_chunks(
    property_fields,
    always_include_properties,
    property_limit_type,
    property_limit,
    expected_property_chunks,
):
    configured_properties = set(property_fields)
    property_fields = property_fields
    property_chunking = PropertyChunking(
        property_limit_type=property_limit_type,
        property_limit=property_limit,
        record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(
        property_chunking.get_request_property_chunks(
            property_fields=property_fields,
            always_include_properties=always_include_properties,
            configured_properties=configured_properties,
        )
    )

    assert len(property_chunks) == len(expected_property_chunks)
    for i, expected_property_chunk in enumerate(expected_property_chunks):
        assert property_chunks[i] == expected_property_chunk


def test_get_request_property_chunks_empty_configured_properties():
    expected_property_chunks = [["white", "lotus"]]

    always_include_properties = ["white", "lotus"]
    property_fields = ["maui", "taormina", "koh_samui", "saint_jean_cap_ferrat"]
    configured_properties: Set[str] = set()
    property_chunking = PropertyChunking(
        property_limit_type=PropertyLimitType.property_count,
        property_limit=3,
        record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
        config=CONFIG,
        parameters={},
    )
    property_chunks = list(
        property_chunking.get_request_property_chunks(
            property_fields=property_fields,
            always_include_properties=always_include_properties,
            configured_properties=configured_properties,
        )
    )
    assert property_chunks == expected_property_chunks


def test_get_request_property_chunks_none_configured_properties():
    expected_property_chunks = [
        ["white", "lotus", "maui", "taormina"],
        ["white", "lotus", "koh_samui", "saint_jean_cap_ferrat"],
    ]

    always_include_properties = ["white", "lotus"]
    property_fields = ["maui", "taormina", "koh_samui", "saint_jean_cap_ferrat"]
    property_chunking = PropertyChunking(
        property_limit_type=PropertyLimitType.property_count,
        property_limit=2,
        record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
        config=CONFIG,
        parameters={},
    )
    property_chunks = list(
        property_chunking.get_request_property_chunks(
            property_fields=property_fields,
            always_include_properties=always_include_properties,
            configured_properties=None,
        )
    )
    assert property_chunks == expected_property_chunks


def test_get_merge_key():
    record = Record(stream_name="test", data={"id": "0"})
    property_chunking = PropertyChunking(
        property_limit_type=PropertyLimitType.property_count,
        property_limit=10,
        record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
        config=CONFIG,
        parameters={},
    )

    merge_key = property_chunking.get_merge_key(record=record)
    assert merge_key == "0"


def test_given_single_property_chunk_when_get_request_property_chunks_then_always_include_properties_are_not_added_to_input_list():
    """
    This test is used to validate that we don't manipulate the in-memory values from get_request_property_chunks
    """
    property_chunking = PropertyChunking(
        property_limit_type=PropertyLimitType.property_count,
        property_limit=None,
        record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
        config=CONFIG,
        parameters={},
    )

    property_fields = ["rick", "chelsea", "victoria", "tim", "saxon", "lochlan", "piper"]
    list(
        property_chunking.get_request_property_chunks(
            property_fields=property_fields,
            always_include_properties=["id"],
            configured_properties=None
        )
    )

    assert property_fields == ["rick", "chelsea", "victoria", "tim", "saxon", "lochlan", "piper"]
