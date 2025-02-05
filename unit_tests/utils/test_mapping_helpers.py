import pytest

from airbyte_cdk.utils.mapping_helpers import combine_mappings


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
        ("duplicate_keys_same_value", [{"a": 1}, {"a": 1}], "Duplicate keys found"),
        ("duplicate_keys_different_value", [{"a": 1}, {"a": 2}], "Duplicate keys found"),
        (
            "nested_structure_not_allowed",
            [{"a": {"b": 1}}, {"a": {"c": 2}}],
            "Duplicate keys found",
        ),
        ("any_nesting_not_allowed", [{"a": {"b": 1}}, {"a": {"d": 2}}], "Duplicate keys found"),
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
            "Duplicate keys found",
        ),
        (
            "type_conflict",
            [{"a": 1}, {"a": {"b": 2}}],
            None,
            "Duplicate keys found",
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
