import pytest

from airbyte_cdk.utils.mapping_helpers import (
    RequestOption,
    RequestOptionType,
    _validate_component_request_option_paths,
    combine_mappings,
)


@pytest.mark.parametrize(
    "test_name, mappings, expected_result",
    [
        ("empty_mappings", [], {}),
        ("single_mapping", [{"a": 1}], {"a": 1}),
        ("handle_none_values", [{"a": 1}, None, {"b": 2}], {"a": 1, "b": 2}),
    ],
)
def test_basic_functionality(test_name, mappings, expected_result):
    """Test basic mapping operations that work the same regardless of request type"""
    assert combine_mappings(mappings) == expected_result


@pytest.mark.parametrize(
    "test_name, mappings, expected_result, expected_error",
    [
        (
            "combine_with_string",
            [{"a": 1}, "option"],
            None,
            "Cannot combine multiple options if one is a string",
        ),
        (
            "multiple_strings",
            ["option1", "option2"],
            None,
            "Cannot combine multiple string options",
        ),
        ("string_with_empty_mapping", ["option", {}], "option", None),
    ],
)
def test_string_handling(test_name, mappings, expected_result, expected_error):
    """Test string handling behavior which is independent of request type"""
    if expected_error:
        with pytest.raises(ValueError, match=expected_error):
            combine_mappings(mappings)
    else:
        assert combine_mappings(mappings) == expected_result


@pytest.mark.parametrize(
    "test_name, mappings, expected_error",
    [
        ("duplicate_keys_same_value", [{"a": 1}, {"a": 1}], "duplicate keys detected"),
        ("duplicate_keys_different_value", [{"a": 1}, {"a": 2}], "duplicate keys detected"),
        (
            "nested_structure_not_allowed",
            [{"a": {"b": 1}}, {"a": {"c": 2}}],
            "duplicate keys detected",
        ),
        ("any_nesting_not_allowed", [{"a": {"b": 1}}, {"a": {"d": 2}}], "duplicate keys detected"),
    ],
)
def test_non_body_json_requests(test_name, mappings, expected_error):
    """Test strict validation for non-body-json requests (headers, params, body_data)"""
    with pytest.raises(ValueError, match=expected_error):
        combine_mappings(mappings, allow_same_value_merge=False)


@pytest.mark.parametrize(
    "test_name, mappings, expected_result, expected_error",
    [
        (
            "simple_nested_merge",
            [{"a": {"b": 1}}, {"c": {"d": 2}}],
            {"a": {"b": 1}, "c": {"d": 2}},
            None,
        ),
        (
            "deep_nested_merge",
            [{"a": {"b": {"c": 1}}}, {"d": {"e": {"f": 2}}}],
            {"a": {"b": {"c": 1}}, "d": {"e": {"f": 2}}},
            None,
        ),
        (
            "nested_merge_same_level",
            [
                {"data": {"user": {"id": 1}, "status": "active"}},
                {"data": {"user": {"name": "test"}, "type": "admin"}},
            ],
            {
                "data": {
                    "user": {"id": 1, "name": "test"},
                    "status": "active",
                    "type": "admin",
                },
            },
            None,
        ),
        (
            "nested_conflict",
            [{"a": {"b": 1}}, {"a": {"b": 2}}],
            None,
            "duplicate keys detected",
        ),
        (
            "type_conflict",
            [{"a": 1}, {"a": {"b": 2}}],
            None,
            "duplicate keys detected",
        ),
    ],
)
def test_body_json_requests(test_name, mappings, expected_result, expected_error):
    """Test nested structure support for body_json requests"""
    if expected_error:
        with pytest.raises(ValueError, match=expected_error):
            combine_mappings(mappings, allow_same_value_merge=True)
    else:
        assert combine_mappings(mappings, allow_same_value_merge=True) == expected_result


@pytest.fixture
def mock_config() -> dict[str, str]:
    return {"test": "config"}


@pytest.mark.parametrize(
    "test_name, option1, option2, should_raise",
    [
        (
            "different_fields",
            RequestOption(
                field_name="field1", inject_into=RequestOptionType.body_json, parameters={}
            ),
            RequestOption(
                field_name="field2", inject_into=RequestOptionType.body_json, parameters={}
            ),
            False,
        ),
        (
            "same_field_name_header",
            RequestOption(field_name="field", inject_into=RequestOptionType.header, parameters={}),
            RequestOption(field_name="field", inject_into=RequestOptionType.header, parameters={}),
            True,
        ),
        (
            "different_nested_paths",
            RequestOption(
                field_path=["data", "query1", "limit"],
                inject_into=RequestOptionType.body_json,
                parameters={},
            ),
            RequestOption(
                field_path=["data", "query2", "limit"],
                inject_into=RequestOptionType.body_json,
                parameters={},
            ),
            False,
        ),
        (
            "same_nested_paths",
            RequestOption(
                field_path=["data", "query", "limit"],
                inject_into=RequestOptionType.body_json,
                parameters={},
            ),
            RequestOption(
                field_path=["data", "query", "limit"],
                inject_into=RequestOptionType.body_json,
                parameters={},
            ),
            True,
        ),
        (
            "different_inject_types",
            RequestOption(field_name="field", inject_into=RequestOptionType.header, parameters={}),
            RequestOption(
                field_name="field", inject_into=RequestOptionType.body_json, parameters={}
            ),
            False,
        ),
    ],
)
def test_request_option_validation(test_name, option1, option2, should_raise, mock_config):
    """Test various combinations of request option validation"""
    if should_raise:
        with pytest.raises(ValueError, match="duplicate keys detected"):
            _validate_component_request_option_paths(mock_config, option1, option2)
    else:
        _validate_component_request_option_paths(mock_config, option1, option2)


@pytest.mark.parametrize(
    "test_name, options",
    [
        (
            "none_options",
            [
                None,
                RequestOption(
                    field_name="field", inject_into=RequestOptionType.header, parameters={}
                ),
                None,
            ],
        ),
        (
            "single_option",
            [
                RequestOption(
                    field_name="field", inject_into=RequestOptionType.header, parameters={}
                )
            ],
        ),
        ("all_none", [None, None, None]),
        ("empty_list", []),
    ],
)
def test_edge_cases(test_name, options, mock_config):
    """Test edge cases like None values and single options"""
    _validate_component_request_option_paths(mock_config, *options)
