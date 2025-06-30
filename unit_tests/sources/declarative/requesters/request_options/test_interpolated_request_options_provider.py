#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    RequestBodyGraphQL,
    RequestBodyGraphQlQuery,
    RequestBodyJsonObject,
    RequestBodyPlainText,
    RequestBodyUrlEncodedForm,
)
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_request_options_provider import (
    InterpolatedRequestOptionsProvider,
)
from airbyte_cdk.sources.types import StreamSlice

state = {"date": "2021-01-01"}
stream_slice = {"start_date": "2020-01-01"}
next_page_token = {"offset": 12345, "page": 27}
config = {"option": "OPTION"}


@pytest.mark.parametrize(
    "test_name, input_request_params, expected_request_params",
    [
        (
            "test_static_param",
            {"a_static_request_param": "a_static_value"},
            {"a_static_request_param": "a_static_value"},
        ),
        (
            "test_value_depends_on_stream_interval",
            {"read_from_stream_interval": "{{ stream_interval['start_date'] }}"},
            {"read_from_stream_interval": "2020-01-01"},
        ),
        (
            "test_value_depends_on_stream_slice",
            {"read_from_slice": "{{ stream_slice['start_date'] }}"},
            {"read_from_slice": "2020-01-01"},
        ),
        (
            "test_value_depends_on_next_page_token",
            {"read_from_token": "{{ next_page_token['offset'] }}"},
            {"read_from_token": "12345"},
        ),
        (
            "test_value_depends_on_config",
            {"read_from_config": "{{ config['option'] }}"},
            {"read_from_config": "OPTION"},
        ),
        (
            "test_parameter_is_interpolated",
            {
                "{{ stream_interval['start_date'] }} - {{stream_slice['start_date']}} - {{next_page_token['offset']}} - {{config['option']}}": "ABC"
            },
            {"2020-01-01 - 2020-01-01 - 12345 - OPTION": "ABC"},
        ),
        ("test_boolean_false_value", {"boolean_false": "{{ False }}"}, {"boolean_false": "False"}),
        ("test_integer_falsy_value", {"integer_falsy": "{{ 0 }}"}, {"integer_falsy": "0"}),
        ("test_number_falsy_value", {"number_falsy": "{{ 0.0 }}"}, {"number_falsy": "0.0"}),
        ("test_string_falsy_value", {"string_falsy": "{{ '' }}"}, {}),
        ("test_none_value", {"none_value": "{{ None }}"}, {"none_value": "None"}),
    ],
)
def test_interpolated_request_params(test_name, input_request_params, expected_request_params):
    provider = InterpolatedRequestOptionsProvider(
        config=config, request_parameters=input_request_params, parameters={}
    )

    actual_request_params = provider.get_request_params(
        stream_state=state, stream_slice=stream_slice, next_page_token=next_page_token
    )

    assert actual_request_params == expected_request_params


@pytest.mark.parametrize(
    "test_name, input_request_json, expected_request_json",
    [
        (
            "test_static_json",
            {"a_static_request_param": "a_static_value"},
            {"a_static_request_param": "a_static_value"},
        ),
        (
            "test_value_depends_on_stream_slice",
            {"read_from_slice": "{{ stream_slice['start_date'] }}"},
            {"read_from_slice": "2020-01-01"},
        ),
        (
            "test_value_depends_on_next_page_token",
            {"read_from_token": "{{ next_page_token['offset'] }}"},
            {"read_from_token": 12345},
        ),
        (
            "test_value_depends_on_config",
            {"read_from_config": "{{ config['option'] }}"},
            {"read_from_config": "OPTION"},
        ),
        (
            "test_interpolated_keys",
            {"{{ stream_interval['start_date'] }}": 123, "{{ config['option'] }}": "ABC"},
            {"2020-01-01": 123, "OPTION": "ABC"},
        ),
        ("test_boolean_false_value", {"boolean_false": "{{ False }}"}, {"boolean_false": False}),
        ("test_integer_falsy_value", {"integer_falsy": "{{ 0 }}"}, {"integer_falsy": 0}),
        ("test_number_falsy_value", {"number_falsy": "{{ 0.0 }}"}, {"number_falsy": 0.0}),
        ("test_string_falsy_value", {"string_falsy": "{{ '' }}"}, {}),
        ("test_none_value", {"none_value": "{{ None }}"}, {}),
        (
            "test_string",
            """{"nested": { "key": "{{ config['option'] }}" }}""",
            {"nested": {"key": "OPTION"}},
        ),
        (
            "test_nested_objects",
            {"nested": {"key": "{{ config['option'] }}"}},
            {"nested": {"key": "OPTION"}},
        ),
        (
            "test_nested_objects_interpolated keys",
            {"nested": {"{{ stream_interval['start_date'] }}": "{{ config['option'] }}"}},
            {"nested": {"2020-01-01": "OPTION"}},
        ),
    ],
)
def test_interpolated_request_json(test_name, input_request_json, expected_request_json):
    provider = InterpolatedRequestOptionsProvider(
        config=config, request_body_json=input_request_json, parameters={}
    )

    actual_request_json = provider.get_request_body_json(
        stream_state=state, stream_slice=stream_slice, next_page_token=next_page_token
    )

    assert actual_request_json == expected_request_json


@pytest.mark.parametrize(
    "test_name, input_request_json, expected_request_json",
    [
        (
            "test_static_json",
            RequestBodyJsonObject(
                type="RequestBodyJsonObject", value={"a_static_request_param": "a_static_value"}
            ),
            {"a_static_request_param": "a_static_value"},
        ),
        (
            "test_value_depends_on_stream_slice",
            RequestBodyJsonObject(
                type="RequestBodyJsonObject",
                value={"read_from_slice": "{{ stream_slice['start_date'] }}"},
            ),
            {"read_from_slice": "2020-01-01"},
        ),
        (
            "test_value_depends_on_next_page_token",
            RequestBodyJsonObject(
                type="RequestBodyJsonObject",
                value={"read_from_token": "{{ next_page_token['offset'] }}"},
            ),
            {"read_from_token": 12345},
        ),
        (
            "test_value_depends_on_config",
            RequestBodyJsonObject(
                type="RequestBodyJsonObject", value={"read_from_config": "{{ config['option'] }}"}
            ),
            {"read_from_config": "OPTION"},
        ),
        (
            "test_interpolated_keys",
            RequestBodyJsonObject(
                type="RequestBodyJsonObject",
                value={"{{ stream_interval['start_date'] }}": 123, "{{ config['option'] }}": "ABC"},
            ),
            {"2020-01-01": 123, "OPTION": "ABC"},
        ),
        (
            "test_boolean_false_value",
            RequestBodyJsonObject(
                type="RequestBodyJsonObject", value={"boolean_false": "{{ False }}"}
            ),
            {"boolean_false": False},
        ),
        (
            "test_integer_falsy_value",
            RequestBodyJsonObject(
                type="RequestBodyJsonObject",
                value={"integer_falsy": "{{ 0 }}"},
            ),
            {"integer_falsy": 0},
        ),
        (
            "test_number_falsy_value",
            RequestBodyJsonObject(
                type="RequestBodyJsonObject", value={"number_falsy": "{{ 0.0 }}"}
            ),
            {"number_falsy": 0.0},
        ),
        (
            "test_string_falsy_value",
            RequestBodyJsonObject(type="RequestBodyJsonObject", value={"string_falsy": "{{ '' }}"}),
            {},
        ),
        (
            "test_none_value",
            RequestBodyJsonObject(type="RequestBodyJsonObject", value={"none_value": "{{ None }}"}),
            {},
        ),
        (
            "test_string",
            RequestBodyPlainText(
                type="RequestBodyPlainText",
                value="""{"nested": { "key": "{{ config['option'] }}" }}""",
            ),
            {"nested": {"key": "OPTION"}},
        ),
        (
            "test_nested_objects",
            RequestBodyJsonObject(
                type="RequestBodyJsonObject", value={"nested": {"key": "{{ config['option'] }}"}}
            ),
            {"nested": {"key": "OPTION"}},
        ),
        (
            "test_nested_objects_interpolated keys",
            RequestBodyJsonObject(
                type="RequestBodyJsonObject",
                value={"nested": {"{{ stream_interval['start_date'] }}": "{{ config['option'] }}"}},
            ),
            {"nested": {"2020-01-01": "OPTION"}},
        ),
        (
            "test_graphql_query",
            RequestBodyGraphQL(
                type="RequestBodyGraphQL",
                value=RequestBodyGraphQlQuery(
                    query="query { {{ config['option'] }} }"
                ),
            ),
            {"query": "query { OPTION }"},
        ),
    ],
)
def test_interpolated_request_json_using_request_body(
    test_name, input_request_json, expected_request_json
) -> None:
    provider = InterpolatedRequestOptionsProvider(
        config=config,
        request_body=input_request_json,
        parameters={},
    )
    actual_request_json = provider.get_request_body_json(
        stream_state=state, stream_slice=stream_slice, next_page_token=next_page_token
    )

    assert actual_request_json == expected_request_json


@pytest.mark.parametrize(
    "test_name, input_request_data, expected_request_data",
    [
        (
            "test_static_map_data",
            {"a_static_request_param": "a_static_value"},
            {"a_static_request_param": "a_static_value"},
        ),
        (
            "test_map_depends_on_stream_slice",
            {"read_from_slice": "{{ stream_slice['start_date'] }}"},
            {"read_from_slice": "2020-01-01"},
        ),
        (
            "test_map_depends_on_config",
            {"read_from_config": "{{ config['option'] }}"},
            {"read_from_config": "OPTION"},
        ),
        ("test_defaults_to_empty_dict", None, {}),
        (
            "test_interpolated_keys",
            {"{{ stream_interval['start_date'] }} - {{ next_page_token['offset'] }}": "ABC"},
            {"2020-01-01 - 12345": "ABC"},
        ),
    ],
)
def test_interpolated_request_data(test_name, input_request_data, expected_request_data):
    provider = InterpolatedRequestOptionsProvider(
        config=config, request_body_data=input_request_data, parameters={}
    )

    actual_request_data = provider.get_request_body_data(
        stream_state=state, stream_slice=stream_slice, next_page_token=next_page_token
    )

    assert actual_request_data == expected_request_data


@pytest.mark.parametrize(
    "test_name, input_request_data, expected_request_data",
    [
        (
            "test_static_map_data",
            RequestBodyUrlEncodedForm(
                type="RequestBodyUrlEncodedForm",
                value={"a_static_request_param": "a_static_value"},
            ),
            {"a_static_request_param": "a_static_value"},
        ),
        (
            "test_map_depends_on_stream_slice",
            RequestBodyUrlEncodedForm(
                type="RequestBodyUrlEncodedForm",
                value={"read_from_slice": "{{ stream_slice['start_date'] }}"},
            ),
            {"read_from_slice": "2020-01-01"},
        ),
        (
            "test_map_depends_on_config",
            RequestBodyUrlEncodedForm(
                type="RequestBodyUrlEncodedForm",
                value={"read_from_config": "{{ config['option'] }}"},
            ),
            {"read_from_config": "OPTION"},
        ),
        (
            "test_defaults_to_empty_dict",
            RequestBodyUrlEncodedForm(
                type="RequestBodyUrlEncodedForm",
                value={},
            ),
            {},
        ),
        (
            "test_interpolated_keys",
            RequestBodyUrlEncodedForm(
                type="RequestBodyUrlEncodedForm",
                value={
                    "{{ stream_interval['start_date'] }} - {{ next_page_token['offset'] }}": "ABC"
                },
            ),
            {"2020-01-01 - 12345": "ABC"},
        ),
    ],
)
def test_interpolated_request_data_using_request_body(
    test_name, input_request_data, expected_request_data
) -> None:
    provider = InterpolatedRequestOptionsProvider(
        config=config,
        request_body=input_request_data,
        parameters={},
    )

    actual_request_data = provider.get_request_body_data(
        stream_state=state, stream_slice=stream_slice, next_page_token=next_page_token
    )

    assert actual_request_data == expected_request_data


def test_error_on_create_for_both_request_json_and_data():
    request_json = {"body_key": "{{ stream_slice['start_date'] }}"}
    request_data = "interpolate_me=5&invalid={{ config['option'] }}"
    with pytest.raises(ValueError):
        InterpolatedRequestOptionsProvider(
            config=config,
            request_body_json=request_json,
            request_body_data=request_data,
            parameters={},
        )


@pytest.mark.parametrize(
    "incoming_stream_slice,expected_query_params,expected_error",
    [
        pytest.param(
            StreamSlice(
                cursor_slice={}, partition={}, extra_fields={"query_properties": ["id", "name"]}
            ),
            {"predicate": "OPTION", "properties": "id,name"},
            None,
            id="test_include_query_properties",
        ),
        pytest.param(None, None, ValueError, id="test_raise_error_on_no_stream_slice"),
        pytest.param(
            StreamSlice(cursor_slice={}, partition={}, extra_fields={}),
            None,
            ValueError,
            id="test_raise_error_on_no_query_properties",
        ),
        pytest.param(
            StreamSlice(cursor_slice={}, partition={}, extra_fields={"query_properties": None}),
            None,
            ValueError,
            id="test_raise_error_on_query_properties_is_none",
        ),
        pytest.param(
            StreamSlice(cursor_slice={}, partition={}, extra_fields={"query_properties": 404}),
            None,
            ValueError,
            id="test_raise_error_on_query_properties_is_not_a_list_of_properties",
        ),
    ],
)
def test_property_error_on_invalid_stream_slice(
    incoming_stream_slice, expected_query_params, expected_error
):
    request_options_provider = InterpolatedRequestOptionsProvider(
        request_parameters={"predicate": "{{ config['option'] }}"},
        query_properties_key="properties",
        config=config,
        parameters={},
    )
    if expected_error:
        with pytest.raises(expected_error):
            request_options_provider.get_request_params(stream_slice=incoming_stream_slice)
    else:
        request_parameters = request_options_provider.get_request_params(
            stream_slice=incoming_stream_slice
        )
        assert request_parameters == expected_query_params
