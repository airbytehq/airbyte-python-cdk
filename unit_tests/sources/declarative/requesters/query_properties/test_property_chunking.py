# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

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
            15,
            [["kate", "laurie"], ["jaclyn"]],
            id="test_property_chunking_limit_characters",
        ),
        pytest.param(
            ["laurie", "jaclyn", "kaitlin"],
            None,
            PropertyLimitType.characters,
            12,
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
    property_fields = iter(property_fields)
    property_chunking = PropertyChunking(
        property_limit_type=property_limit_type,
        property_limit=property_limit,
        record_merge_strategy=GroupByKey(key="id", config=CONFIG, parameters={}),
        config=CONFIG,
        parameters={},
    )

    property_chunks = list(
        property_chunking.get_request_property_chunks(
            property_fields=property_fields, always_include_properties=always_include_properties
        )
    )

    assert len(property_chunks) == len(expected_property_chunks)
    for i, expected_property_chunk in enumerate(expected_property_chunks):
        assert property_chunks[i] == expected_property_chunk


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
