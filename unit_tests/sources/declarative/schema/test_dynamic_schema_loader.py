#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.schema import DynamicSchemaLoader, SchemaTypeIdentifier


@pytest.fixture
def mock_retriever():
    retriever = MagicMock()
    retriever.read_records.return_value = [
        {
            "schema": [
                {"field1": {"key": "name", "type": "string"}},
                {"field2": {"key": "age", "type": "integer"}},
                {"field3": {"key": "active", "type": "boolean"}},
            ]
        }
    ]
    return retriever


@pytest.fixture
def mock_schema_type_identifier():
    return SchemaTypeIdentifier(
        schema_pointer=["schema"],
        key_pointer=["key"],
        type_pointer=["type"],
        types_map=[],
        parameters={},
    )


@pytest.fixture
def dynamic_schema_loader(mock_retriever, mock_schema_type_identifier):
    config = MagicMock()
    parameters = {}
    return DynamicSchemaLoader(
        retriever=mock_retriever,
        config=config,
        parameters=parameters,
        schema_type_identifier=mock_schema_type_identifier,
    )


@pytest.mark.parametrize(
    "retriever_data, expected_schema",
    [
        (
            # Test case: All fields with valid types
            iter(
                [
                    {
                        "schema": [
                            {"key": "name", "type": "string"},
                            {"key": "age", "type": "integer"},
                        ]
                    }
                ]
            ),
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "name": {"type": ["null", "string"]},
                    "age": {"type": ["null", "integer"]},
                },
            },
        ),
        (
            # Test case: Fields with missing type default to "string"
            iter(
                [
                    {
                        "schema": [
                            {"key": "name"},
                            {"key": "email", "type": "string"},
                        ]
                    }
                ]
            ),
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "name": {"type": ["null", "string"]},
                    "email": {"type": ["null", "string"]},
                },
            },
        ),
        (
            # Test case: Fields with nested types
            iter(
                [
                    {
                        "schema": [
                            {"key": "address", "type": ["string", "integer"]},
                        ]
                    }
                ]
            ),
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "address": {
                        "oneOf": [{"type": ["null", "string"]}, {"type": ["null", "integer"]}]
                    },
                },
            },
        ),
        (
            # Test case: Empty record set
            iter([]),
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {},
            },
        ),
    ],
)
def test_dynamic_schema_loader(dynamic_schema_loader, retriever_data, expected_schema):
    dynamic_schema_loader.retriever.read_records = MagicMock(return_value=retriever_data)

    schema = dynamic_schema_loader.get_json_schema()

    # Validate the generated schema
    assert schema == expected_schema


def test_dynamic_schema_loader_invalid_key(dynamic_schema_loader):
    # Test case: Invalid key type
    dynamic_schema_loader.retriever.read_records.return_value = iter(
        [{"schema": [{"field1": {"key": 123, "type": "string"}}]}]
    )

    with pytest.raises(ValueError, match="Expected key to be a string"):
        dynamic_schema_loader.get_json_schema()


def test_dynamic_schema_loader_invalid_type(dynamic_schema_loader):
    # Test case: Invalid type
    dynamic_schema_loader.retriever.read_records.return_value = iter(
        [{"schema": [{"field1": {"key": "name", "type": "invalid_type"}}]}]
    )

    with pytest.raises(ValueError, match="Expected key to be a string. Got None"):
        dynamic_schema_loader.get_json_schema()
