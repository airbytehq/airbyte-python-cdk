#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.transformations.flatten_fields import (
    FlattenFields,
)


@pytest.mark.parametrize(
    "flatten_lists, input_record, expected_output",
    [
        (True, {"FirstName": "John", "LastName": "Doe"}, {"FirstName": "John", "LastName": "Doe"}),
        (True, {"123Number": 123, "456Another123": 456}, {"123Number": 123, "456Another123": 456}),
        (
            True,
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
            True,
            {"ListExample": [{"A": "a"}, {"A": "b"}]},
            {"ListExample.0.A": "a", "ListExample.1.A": "b"},
        ),
        (
            True,
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
            True,
            {"List": ["Item1", "Item2", "Item3"]},
            {"List.0": "Item1", "List.1": "Item2", "List.2": "Item3"},
        ),
        (
            False,
            {"List": ["Item1", "Item2", "Item3"]},
            {"List": ["Item1", "Item2", "Item3"]},
        ),
        (
            False,
            {
                "RootField": {
                    "NestedList": [{"Key": {"Value": "test1"}}, {"Key": {"Value": "test2"}}]
                },
                "SimpleKey": "SimpleValue",
            },
            {
                "NestedList": [{"Key": {"Value": "test1"}}, {"Key": {"Value": "test2"}}],
                "SimpleKey": "SimpleValue",
            },
        ),
        (
            False,
            {
                "RootField": {"List": [{"Key": {"Value": "test1"}}, {"Key": {"Value": "test2"}}]},
                "List": [1, 3, 6],
                "SimpleKey": "SimpleValue",
            },
            {
                "RootField.List": [{"Key": {"Value": "test1"}}, {"Key": {"Value": "test2"}}],
                "List": [1, 3, 6],
                "SimpleKey": "SimpleValue",
            },
        ),
    ],
)
def test_flatten_fields(flatten_lists, input_record, expected_output):
    flattener = FlattenFields(flatten_lists=flatten_lists)
    flattener.transform(input_record)
    assert input_record == expected_output
