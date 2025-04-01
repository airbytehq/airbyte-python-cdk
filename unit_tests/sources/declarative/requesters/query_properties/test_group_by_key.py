# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import pytest

from airbyte_cdk.sources.declarative.requesters.query_properties.strategies import GroupByKey
from airbyte_cdk.sources.types import Record


@pytest.mark.parametrize(
    "key,record,expected_merge_key",
    [
        pytest.param(
            ["id"],
            Record(
                stream_name="test",
                data={"id": "0", "first_name": "Belinda", "last_name": "Lindsey"},
            ),
            "0",
            id="test_get_merge_key_single",
        ),
        pytest.param(
            ["last_name", "first_name"],
            Record(
                stream_name="test", data={"id": "1", "first_name": "Zion", "last_name": "Lindsey"}
            ),
            "Lindsey,Zion",
            id="test_get_merge_key_single_multiple",
        ),
        pytest.param(
            [""],
            Record(stream_name="test", data={}),
            None,
            id="test_get_merge_key_not_present",
        ),
    ],
)
def test_get_merge_key(key, record, expected_merge_key):
    group_by_key = GroupByKey(key=key, config={}, parameters={})

    merge_key = group_by_key.get_group_key(record=record)
    assert merge_key == expected_merge_key
