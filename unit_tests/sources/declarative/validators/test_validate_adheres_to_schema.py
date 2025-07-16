from unittest import TestCase

import jsonschema
import pytest

from airbyte_cdk.sources.declarative.validators.validate_adheres_to_schema import (
    ValidateAdheresToSchema,
)


class TestValidateAdheresToSchema(TestCase):
    def test_given_valid_input_matching_schema_when_validate_then_succeeds(self):
        schema = {
            "type": "object",
            "required": ["id", "name"],
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"},
                "age": {"type": "integer", "minimum": 0},
            },
        }

        validator = ValidateAdheresToSchema(schema=schema)

        valid_data = {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30}

        validator.validate(valid_data)

    def test_given_missing_required_field_when_validate_then_raises_error(self):
        schema = {
            "type": "object",
            "required": ["id", "name"],
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"},
                "age": {"type": "integer", "minimum": 0},
            },
        }

        validator = ValidateAdheresToSchema(schema=schema)

        # Data missing the required 'name' field
        invalid_data = {"id": 1, "email": "john@example.com", "age": 30}

        with pytest.raises(ValueError) as exc_info:
            validator.validate(invalid_data)

        assert "required" in str(exc_info.value)
        assert "name" in str(exc_info.value)

    def test_given_incorrect_type_when_validate_then_raises_error(self):
        schema = {
            "type": "object",
            "required": ["id", "name"],
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"},
                "age": {"type": "integer", "minimum": 0},
            },
        }

        validator = ValidateAdheresToSchema(schema=schema)

        invalid_data = {
            "id": "one",  # Should be an integer
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30,
        }

        with pytest.raises(ValueError) as exc_info:
            validator.validate(invalid_data)

        assert "type" in str(exc_info.value)
        assert "integer" in str(exc_info.value)

    def test_given_invalid_schema_when_validate_then_raises_error(self):
        invalid_schema = {"type": "object", "properties": {"id": {"type": "invalid_type"}}}

        validator = ValidateAdheresToSchema(schema=invalid_schema)
        data = {"id": 123}

        with pytest.raises(jsonschema.exceptions.SchemaError) as exc_info:
            validator.validate(data)

        assert "invalid_type" in str(exc_info.value)

    def test_given_null_value_when_validate_then_succeeds_if_nullable(self):
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "optional_field": {"type": ["string", "null"]},
            },
            "required": ["id", "name"],
        }

        validator = ValidateAdheresToSchema(schema=schema)

        data_with_null = {"id": 1, "name": "Test User", "optional_field": None}

        validator.validate(data_with_null)

        data_without_field = {"id": 1, "name": "Test User"}

        validator.validate(data_without_field)

    def test_given_empty_schema_when_validate_then_succeeds(self):
        empty_schema = {}

        validator = ValidateAdheresToSchema(schema=empty_schema)

        validator.validate({"complex": "object"})

    def test_given_json_string_when_validate_then_succeeds(self):
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
            },
        }

        validator = ValidateAdheresToSchema(schema=schema)

        validator.validate('{"id": 1, "name": "John Doe"}')
