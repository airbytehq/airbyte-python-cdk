#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.extractors.record_extractor import SERVICE_KEY_PREFIX
from airbyte_cdk.sources.declarative.transformations.flatten_fields import (
    FlattenFields,
)

_FLATTEN_LISTS = True
_DO_NOT_FLATTEN_LISTS = False


@pytest.mark.parametrize(
    "flatten_lists, input_record, expected_output",
    [
        pytest.param(
            _FLATTEN_LISTS,
            {"FirstName": "John", "LastName": "Doe"},
            {"FirstName": "John", "LastName": "Doe"},
            id="flatten simple record with string values",
        ),
        pytest.param(
            _FLATTEN_LISTS,
            {"123Number": 123, "456Another123": 456},
            {"123Number": 123, "456Another123": 456},
            id="flatten simple record with int values",
        ),
        pytest.param(
            _FLATTEN_LISTS,
            {
                "NestedRecord": {"FirstName": "John", "LastName": "Doe"},
                "456Another123": 456,
            },
            {
                "FirstName": "John",
                "LastName": "Doe",
                "456Another123": 456,
            },
            id="flatten record with nested dict",
        ),
        pytest.param(
            _FLATTEN_LISTS,
            {"ListExample": [{"A": "a"}, {"A": "b"}]},
            {"ListExample.0.A": "a", "ListExample.1.A": "b"},
            id="flatten record with list values of dict items",
        ),
        pytest.param(
            _FLATTEN_LISTS,
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
            id="flatten record with nested dict of both list and string values",
        ),
        pytest.param(
            _FLATTEN_LISTS,
            {"List": ["Item1", "Item2", "Item3"]},
            {"List.0": "Item1", "List.1": "Item2", "List.2": "Item3"},
            id="flatten record with list of str values",
        ),
        pytest.param(
            _DO_NOT_FLATTEN_LISTS,
            {"List": ["Item1", "Item2", "Item3"]},
            {"List": ["Item1", "Item2", "Item3"]},
            id="flatten record with dict of list values, flatten_lists=False",
        ),
        pytest.param(
            _DO_NOT_FLATTEN_LISTS,
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
            id="flatten record with dict of list values and simple key, flatten_lists=False",
        ),
        pytest.param(
            _DO_NOT_FLATTEN_LISTS,
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
            id="flatten record with dict of list values and simple key with duplicated keys, flatten_lists=False",
        ),
        (
            {SERVICE_KEY_PREFIX + "name": "xyz", "List": ["Item1", "Item2", "Item3"]},
            {
                SERVICE_KEY_PREFIX + "name": "xyz",
                "List.0": "Item1",
                "List.1": "Item2",
                "List.2": "Item3",
            },
        ),
        (
            {SERVICE_KEY_PREFIX + "name": {"k", "xyz"}, "List": ["Item1", "Item2", "Item3"]},
            {
                SERVICE_KEY_PREFIX + "name": {"k", "xyz"},
                "List.0": "Item1",
                "List.1": "Item2",
                "List.2": "Item3",
            },
        ),
    ],
)
def test_flatten_fields(flatten_lists, input_record, expected_output):
    flattener = FlattenFields(flatten_lists=flatten_lists)
    flattener.transform(input_record)
    assert input_record == expected_output
