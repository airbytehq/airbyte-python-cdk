#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json

import pytest

from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer

SIMPLE_SCHEMA = {"type": "object", "properties": {"value": {"type": "string"}}}
COMPLEX_SCHEMA = {
    "type": "object",
    "properties": {
        "value": {"type": "boolean", "format": "even", "is_positive": True},
        "prop": {"type": "string"},
        "prop_with_null": {"type": ["string", "null"]},
        "number_prop": {"type": "number"},
        "int_prop": {"type": ["integer", "null"]},
        "too_many_types": {"type": ["boolean", "null", "string"]},
        "def": {
            "type": "object",
            "properties": {"dd": {"$ref": "#/definitions/my_type"}},
        },
        "array": {"type": "array", "items": {"$ref": "#/definitions/str_type"}},
        "nested": {"$ref": "#/definitions/nested_type"},
        "list_of_lists": {
            "type": "array",
            "items": {"type": "array", "items": {"type": "string"}},
        },
    },
    "definitions": {
        "str_type": {"type": "string"},
        "nested_type": {"type": "object", "properties": {"a": {"type": "string"}}},
    },
}
VERY_NESTED_SCHEMA = {
    "type": ["null", "object"],
    "properties": {
        "very_nested_value": {
            "type": ["null", "object"],
            "properties": {
                "very_nested_value": {
                    "type": ["null", "object"],
                    "properties": {
                        "very_nested_value": {
                            "type": ["null", "object"],
                            "properties": {
                                "very_nested_value": {
                                    "type": ["null", "object"],
                                    "properties": {
                                        "very_nested_value": {"type": ["null", "number"]}
                                    },
                                }
                            },
                        }
                    },
                }
            },
        }
    },
}


@pytest.mark.parametrize(
    "schema, actual, expected, expected_warns",
    [
        (SIMPLE_SCHEMA, {"value": 12}, {"value": "12"}, None),
        (
            SIMPLE_SCHEMA,
            {"value": 12, "unexpected_value": "unexpected"},
            {"value": "12", "unexpected_value": "unexpected"},
            None,
        ),
        (
            COMPLEX_SCHEMA,
            {"value": 1, "array": ["111", 111, {1: 111}]},
            {"value": True, "array": ["111", "111", "{1: 111}"]},
            None,
        ),
        (
            COMPLEX_SCHEMA,
            {"value": 1, "list_of_lists": [["111"], [111], [11], [{1: 1}]]},
            {"value": True, "list_of_lists": [["111"], ["111"], ["11"], ["{1: 1}"]]},
            None,
        ),
        (
            COMPLEX_SCHEMA,
            {"value": 1, "nested": {"a": [1, 2, 3]}},
            {"value": True, "nested": {"a": "[1, 2, 3]"}},
            None,
        ),
        (
            COMPLEX_SCHEMA,
            {"value": "false", "nested": {"a": [1, 2, 3]}},
            {"value": False, "nested": {"a": "[1, 2, 3]"}},
            None,
        ),
        (COMPLEX_SCHEMA, {}, {}, None),
        (COMPLEX_SCHEMA, {"int_prop": "12"}, {"int_prop": 12}, None),
        # Skip invalid formattted field and process other fields.
        (
            COMPLEX_SCHEMA,
            {"prop": 12, "number_prop": "aa12", "array": [12]},
            {"prop": "12", "number_prop": "aa12", "array": ["12"]},
            "Failed to transform value from type 'string' to type 'number' at path: 'number_prop'",
        ),
        # Field too_many_types have ambigious type, skip formatting
        (
            COMPLEX_SCHEMA,
            {"prop": 12, "too_many_types": 1212, "array": [12]},
            {"prop": "12", "too_many_types": 1212, "array": ["12"]},
            "Failed to transform value from type 'integer' to type '['boolean', 'null', 'string']' at path: 'too_many_types'",
        ),
        # Test null field
        (COMPLEX_SCHEMA, {"prop": None, "array": [12]}, {"prop": "None", "array": ["12"]}, None),
        # If field can be null do not convert
        (
            COMPLEX_SCHEMA,
            {"prop_with_null": None, "array": [12]},
            {"prop_with_null": None, "array": ["12"]},
            None,
        ),
        (
            VERY_NESTED_SCHEMA,
            {
                "very_nested_value": {
                    "very_nested_value": {
                        "very_nested_value": {"very_nested_value": {"very_nested_value": "2"}}
                    }
                }
            },
            {
                "very_nested_value": {
                    "very_nested_value": {
                        "very_nested_value": {"very_nested_value": {"very_nested_value": 2.0}}
                    }
                }
            },
            None,
        ),
        (
            VERY_NESTED_SCHEMA,
            {"very_nested_value": {"very_nested_value": None}},
            {"very_nested_value": {"very_nested_value": None}},
            None,
        ),
        # Object without properties
        ({"type": "object"}, {"value": 12}, {"value": 12}, None),
        (
            # Array without items
            {"type": "object", "properties": {"value": {"type": "array"}}},
            {"value": [12]},
            {"value": [12]},
            None,
        ),
        (
            # Array without items and value is not an array
            {"type": "object", "properties": {"value": {"type": "array"}}},
            {"value": "12"},
            {"value": ["12"]},
            None,
        ),
        (
            {"type": "object", "properties": {"value": {"type": "array"}}},
            {"value": 12},
            {"value": [12]},
            None,
        ),
        (
            {"type": "object", "properties": {"value": {"type": "array"}}},
            {"value": None},
            {"value": [None]},
            None,
        ),
        (
            {"type": "object", "properties": {"value": {"type": ["null", "array"]}}},
            {"value": None},
            {"value": None},
            None,
        ),
        (
            {
                "type": "object",
                "properties": {"value": {"type": "array", "items": {"type": ["string"]}}},
            },
            {"value": 10},
            {"value": ["10"]},
            None,
        ),
        (
            {
                "type": "object",
                "properties": {"value": {"type": "array", "items": {"type": ["object"]}}},
            },
            {"value": "string"},
            {"value": "string"},
            "Failed to transform value from type 'string' to type 'array' at path: 'value'",
        ),
        (
            {
                "type": "object",
                "properties": {"value": {"type": "array", "items": {"type": ["string"]}}},
            },
            {"value": {"key": "value"}},
            {"value": {"key": "value"}},
            "Failed to transform value from type '{'key': 'string'}' to type 'array' at path: 'value'",
        ),
        (
            # Schema root object is not an object, no convertion should happen
            {"type": "integer"},
            {"value": "12"},
            {"value": "12"},
            "Failed to transform value from type '{'value': 'string'}' to type 'integer' at path: ''",
        ),
        (
            # More than one type except null, no conversion should happen
            {"type": "object", "properties": {"value": {"type": ["string", "boolean", "null"]}}},
            {"value": 12},
            {"value": 12},
            "Failed to transform value from type 'integer' to type '['string', 'boolean', 'null']' at path: 'value'",
        ),
        (
            # Oneof not suported, no conversion for one_of_value should happen
            {
                "type": "object",
                "properties": {
                    "one_of_value": {"oneOf": ["string", "boolean", "null"]},
                    "value_2": {"type": "string"},
                },
            },
            {"one_of_value": 12, "value_2": 12},
            {"one_of_value": 12, "value_2": "12"},
            None,
        ),
        (
            # Case for #7076 issue (Facebook marketing: print tons of WARN message)
            {
                "properties": {
                    "cpc": {"type": ["null", "number"]},
                },
            },
            {"cpc": "6.6666"},
            {"cpc": 6.6666},
            None,
        ),
        (
            {
                "type": "object",
                "properties": {"value": {"type": "array", "items": {"type": "string"}}},
            },
            {"value": {"key": "value"}},
            {"value": {"key": "value"}},
            "Failed to transform value from type '{'key': 'string'}' to type 'array' at path: 'value'",
        ),
        (
            {
                "type": "object",
                "properties": {
                    "value1": {"type": "object", "properties": {"value2": {"type": "string"}}}
                },
            },
            {"value1": "value2"},
            {"value1": "value2"},
            "Failed to transform value from type 'string' to type 'object' at path: 'value1'",
        ),
        (
            {
                "type": "object",
                "properties": {"value": {"type": "array", "items": {"type": "object"}}},
            },
            {"value": ["one", "two"]},
            {"value": ["one", "two"]},
            "Failed to transform value from type 'string' to type 'object' at path: 'value.0'",
        ),
        (
            {"type": "string"},
            None,
            None,
            "Failed to transform value from type 'null' to type 'string' at path: ''",
        ),
        (
            {"type": "string"},
            {"a": {"b": {"c": {"d": {"e": "deep value"}}}}},
            {"a": {"b": {"c": {"d": {"e": "deep value"}}}}},
            "Failed to transform value from type '{'a': {'b': {'c': 'object'}}}' to type 'string' at path: ''",
        ),
    ],
    ids=[
        "simple_number_to_string",
        "preserve_unexpected_fields",
        "array_with_mixed_types",
        "nested_list_conversion",
        "array_in_nested_object",
        "string_to_boolean_nested",
        "empty_object",
        "string_to_integer",
        "skip_invalid_number_format",
        "skip_ambiguous_types",
        "null_to_string",
        "preserve_null_when_allowed",
        "very_nested_object_conversion",
        "null_in_nested_structure",
        "object_without_properties",
        "array_without_items",
        "non_array_to_array",
        "number_to_array",
        "null_to_array",
        "null_preserved_for_nullable_array",
        "number_to_string_array",
        "string_fails_object_array",
        "object_fails_array_with_string_array_items",
        "non_object_root_schema",
        "multiple_allowed_types",
        "oneof_not_supported",
        "facebook_cpc_number_conversion",
        "object_fails_array_with_string_item",
        "string_fails_object_conversion",
        "string_fails_object_in_array",
        "null_input_data",
        "max_nesting_depth_protection",
    ],
)
def test_transform(schema, actual, expected, expected_warns, caplog):
    t = TypeTransformer(TransformConfig.DefaultSchemaNormalization)
    t.transform(actual, schema)
    assert json.dumps(actual) == json.dumps(expected)
    if expected_warns:
        record = caplog.records[0]
        assert record.name == "airbyte"
        assert record.levelname == "WARNING"
        assert record.message == expected_warns
    else:
        assert len(caplog.records) == 0


def test_transform_wrong_config():
    with pytest.raises(Exception, match="NoTransform option cannot be combined with other flags."):
        TypeTransformer(TransformConfig.NoTransform | TransformConfig.DefaultSchemaNormalization)

    with pytest.raises(
        Exception,
        match="Please set TransformConfig.CustomSchemaNormalization config before registering custom normalizer",
    ):

        class NotAStream:
            transformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

            @transformer.registerCustomTransform
            def transform_cb(instance, schema):
                pass


def test_custom_transform():
    class NotAStream:
        transformer = TypeTransformer(TransformConfig.CustomSchemaNormalization)

        @transformer.registerCustomTransform
        def transform_cb(instance, schema):
            # Check no default conversion applied
            assert instance == 12
            assert schema == SIMPLE_SCHEMA["properties"]["value"]
            return "transformed"

    s = NotAStream()
    obj = {"value": 12}
    s.transformer.transform(obj, SIMPLE_SCHEMA)
    assert obj == {"value": "transformed"}


def test_custom_transform_with_default_normalization():
    class NotAStream:
        transformer = TypeTransformer(
            TransformConfig.CustomSchemaNormalization | TransformConfig.DefaultSchemaNormalization
        )

        @transformer.registerCustomTransform
        def transform_cb(instance, schema):
            # Check default conversion applied
            assert instance == "12"
            assert schema == SIMPLE_SCHEMA["properties"]["value"]
            return "transformed"

    s = NotAStream()
    obj = {"value": 12}
    s.transformer.transform(obj, SIMPLE_SCHEMA)
    assert obj == {"value": "transformed"}
