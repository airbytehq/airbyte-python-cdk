#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString

config = {"field": "value"}
parameters = {"hello": "world"}
kwargs = {"c": "airbyte"}

JINJA_FLOAT_PARSING = "{{ record['some_calculation'] | float if record.get('some_calculation') is not none and record.get('some_calculation') != '' else none }}"
JINJA_FLOAT_PARSING_MULTILINE = (
    "{% set v = record.get('some_calculation') %}\n"
    "{{ v | replace('%', '') | float if v is not none and v != '' else None }}"
)


@pytest.mark.parametrize(
    "test_name, input_string, expected_value",
    [
        ("test_static_value", "HELLO WORLD", "HELLO WORLD"),
        ("test_eval_from_parameters", "{{ parameters['hello'] }}", "world"),
        ("test_eval_from_config", "{{ config['field'] }}", "value"),
        ("test_eval_from_kwargs", "{{ kwargs['c'] }}", "airbyte"),
        ("test_eval_from_kwargs", "{{ kwargs['c'] }}", "airbyte"),
    ],
)
def test_interpolated_string(test_name, input_string, expected_value):
    s = InterpolatedString.create(input_string, parameters=parameters)
    assert s.eval(config, **{"kwargs": kwargs}) == expected_value


@pytest.mark.parametrize(
    "test_name, input_string, record_value, expected_value",
    [
        ("test_non_zero_string_value", JINJA_FLOAT_PARSING, "4.4", 4.4),
        ("test_non_zero_float_value", JINJA_FLOAT_PARSING, 4.4, 4.4),
        ("test_empty_string", JINJA_FLOAT_PARSING, "", None),
        ("test_none_value", JINJA_FLOAT_PARSING, None, None),
        ("test_zero_string_value", JINJA_FLOAT_PARSING, "0.0", 0.0),
        ("test_zero_2f_string_value", JINJA_FLOAT_PARSING, "0.00", 0.0),
        ("test_float_value", JINJA_FLOAT_PARSING, 0.0, 0.0),
        ("test_multiline_non_zero_string_value", JINJA_FLOAT_PARSING_MULTILINE, "4.4", 4.4),
        ("test_multiline_non_zero_float_value", JINJA_FLOAT_PARSING_MULTILINE, 4.4, 4.4),
        ("test_multiline_empty_string", JINJA_FLOAT_PARSING_MULTILINE, "", None),
        ("test_multiline_none_value", JINJA_FLOAT_PARSING_MULTILINE, None, None),
        ("test_multiline_zero_string_value", JINJA_FLOAT_PARSING_MULTILINE, "0.0", 0.0),
        ("test_multiline_zero_2f_string_value", JINJA_FLOAT_PARSING_MULTILINE, "0.00", 0.0),
        ("test_multiline_float_value", JINJA_FLOAT_PARSING_MULTILINE, 0.0, 0.0),
    ],
)
def test_parsing_record_data(test_name, input_string, record_value, expected_value):
    """
    This is very similar to what happens in stream transformations.
    """
    s = InterpolatedString.create(input_string, parameters=parameters)
    record = {"some_calculation": record_value}
    val = s.eval(config, **{"record": record})
    assert val == expected_value
    if expected_value is None:
        assert val is None, f"Expected None for value {record_value} in test {test_name}"
    else:
        assert float == type(val), (
            f"Expected float, got {type(val)} for value {val} in test {test_name}"
        )
