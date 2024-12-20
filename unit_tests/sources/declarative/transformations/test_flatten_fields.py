#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.transformations.flatten_fields import (
    FlattenFields,
)


@pytest.mark.parametrize(
    "input_record, expected_output",
    [
        ({"FirstName": "John", "LastName": "Doe"}, {"FirstName": "John", "LastName": "Doe"}),
        ({"123Number": 123, "456Another123": 456}, {"123Number": 123, "456Another123": 456}),
        (
            {
                "NestedRecord": {"FirstName": "John", "LastName": "Doe"},
                "456Another123": 456,
            },
            {
                "FirstName": "John",
                "LastName": "Doe",
                "456Another123": 456,
            },
        ),
        (
            {"ListExample": [{"A": "a"}, {"A": "b"}]},
            {"ListExample.0.A": "a", "ListExample.1.A": "b"},
        ),
        (
            {
                "MixedCase123": {
                    "Nested": [{"Key": {"Value": "test1"}}, {"Key": {"Value": "test2"}}]
                },
                "SimpleKey": "SimpleValue",
            },
            {
                "Nested.0.Key.Value": "test1",
                "Nested.1.Key.Value": "test2",
                "SimpleKey": "SimpleValue",
            },
        ),
        (
            {"List": ["Item1", "Item2", "Item3"]},
            {"List.0": "Item1", "List.1": "Item2", "List.2": "Item3"},
        ),
    ],
)
def test_flatten_fields(input_record, expected_output):
    flattener = FlattenFields()
    flattener.transform(input_record)
    assert input_record == expected_output
