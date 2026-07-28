#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import dpath
import pytest

from airbyte_cdk.sources.declarative.interpolation.interpolated_nested_mapping import (
    InterpolatedNestedMapping,
)


@pytest.mark.parametrize(
    "test_name, path, expected_value",
    [
        ("test_field_value", "nested/field", "value"),
        ("test_number", "nested/number", 100),
        ("test_interpolated_number", "nested/nested_array/1/value", 5),
        ("test_interpolated_boolean", "nested/nested_array/2/value", True),
        ("test_field_to_interpolate_from_config", "nested/config_value", "VALUE_FROM_CONFIG"),
        ("test_field_to_interpolate_from_kwargs", "nested/kwargs_value", "VALUE_FROM_KWARGS"),
        (
            "test_field_to_interpolate_from_parameters",
            "nested/parameters_value",
            "VALUE_FROM_PARAMETERS",
        ),
        ("test_key_is_interpolated", "nested/nested_array/0/key", "VALUE"),
    ],
)
def test(test_name, path, expected_value):
    d = {
        "nested": {
            "field": "value",
            "number": 100,
            "nested_array": [
                {"{{ parameters.k }}": "VALUE"},
                {"value": "{{ config['num_value'] | int + 2 }}"},
                {"value": "{{ True }}"},
            ],
            "config_value": "{{ config['c'] }}",
            "parameters_value": "{{ parameters['b'] }}",
            "kwargs_value": "{{ kwargs['a'] }}",
        }
    }

    config = {"c": "VALUE_FROM_CONFIG", "num_value": 3}
    kwargs = {"a": "VALUE_FROM_KWARGS"}
    mapping = InterpolatedNestedMapping(
        mapping=d, parameters={"b": "VALUE_FROM_PARAMETERS", "k": "key"}
    )

    interpolated = mapping.eval(config, **{"kwargs": kwargs})

    assert dpath.get(interpolated, path) == expected_value


@pytest.mark.parametrize(
    "mapping, path, expected_value",
    [
        pytest.param(
            {"clientId": "478"},
            "clientId",
            "478",
            id="static_digit_only_string",
        ),
        pytest.param(
            {"value": "0012"},
            "value",
            "0012",
            id="static_leading_zero_string",
        ),
        pytest.param(
            {"value": "None"},
            "value",
            "None",
            id="static_none_string",
        ),
        pytest.param(
            {"478": "value"},
            "478",
            "value",
            id="static_digit_only_key",
        ),
        pytest.param(
            {"nested": {"clientId": "478"}},
            "nested/clientId",
            "478",
            id="static_string_nested_in_dict",
        ),
        pytest.param(
            {"nested": {"values": ["478", "0012", "None"]}},
            "nested/values",
            ["478", "0012", "None"],
            id="static_strings_nested_in_list",
        ),
        pytest.param(
            {"value": "{{ 1 + 1 }}"},
            "value",
            2,
            id="jinja_value_still_coerces",
        ),
    ],
)
def test_static_strings_are_preserved_and_jinja_values_are_interpolated(
    mapping, path, expected_value
):
    interpolated = InterpolatedNestedMapping(mapping=mapping, parameters={}).eval({})

    assert dpath.get(interpolated, path) == expected_value
