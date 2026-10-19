#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean

config = {
    "parent": {"key_with_true": True},
    "string_key": "compare_me",
    "zero_value": 0,
    "empty_array": [],
    "non_empty_array": [1],
    "empty_dict": {},
    "empty_tuple": (),
    "none_value": None,
}


@pytest.mark.parametrize(
    "test_name, template, expected_result",
    [
        ("test_interpolated_true_value", "{{ config['parent']['key_with_true'] }}", True),
        ("test_interpolated_true_comparison", "{{ config['string_key'] == \"compare_me\" }}", True),
        (
            "test_interpolated_false_condition",
            "{{ config['string_key'] == \"witness_me\" }}",
            False,
        ),
        ("test_path_has_value_returns_true", "{{ config['string_key'] }}", True),
        ("test_zero_is_false", "{{ config['zero_value'] }}", False),
        ("test_empty_array_is_false", "{{ config['empty_array'] }}", False),
        ("test_empty_dict_is_false", "{{ config['empty_dict'] }}", False),
        ("test_empty_tuple_is_false", "{{ config['empty_tuple'] }}", False),
        ("test_none_literal_is_false", "{{ none }}", False),
        ("test_get_missing_key_is_false", "{{ config.get('missing_key') }}", False),
        ("test_explicit_null_value_is_false", "{{ config['none_value'] }}", False),
        (
            "test_and_chain_short_circuits_to_none_is_false",
            "{{ config['string_key'] and config.get('missing_key') }}",
            False,
        ),
        (
            "test_or_chain_short_circuits_to_none_is_false",
            "{{ config.get('missing_key') or config['none_value'] }}",
            False,
        ),
        (
            "test_or_chain_with_truthy_value_is_true",
            "{{ config.get('missing_key') or config['string_key'] }}",
            True,
        ),
        ("test_lowercase_false", '{{ "false" }}', False),
        ("test_False", "{{ False }}", False),
        ("test_True", "{{ True }}", True),
        ("test_value_in_array", "{{ 1 in config['non_empty_array'] }}", True),
        ("test_value_not_in_array", "{{ 2 in config['non_empty_array'] }}", False),
        (
            "test_interpolation_using_parameters",
            "{{ parameters['from_parameters'] == \"come_find_me\" }}",
            True,
        ),
    ],
)
def test_interpolated_boolean(test_name, template, expected_result):
    interpolated_bool = InterpolatedBoolean(
        condition=template, parameters={"from_parameters": "come_find_me"}
    )
    assert interpolated_bool.eval(config) == expected_result
