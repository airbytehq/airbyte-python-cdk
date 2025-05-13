#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.schema import (
    DynamicSchemaLoader,
    InlineSchemaLoader,
    SchemaTypeIdentifier,
)
from airbyte_cdk.sources.declarative.schema.composite_schema_loader import CompositeSchemaLoader


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
        types_mapping=[],
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


def test_composite_schema_loader_with_dynamic(dynamic_schema_loader):
    expected_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": ["null", "object"],
        "properties": {
            "id": {"description": "Unique identifier for the form.", "type": ["null", "string"]},
            "name": {"description": "Name of the record.", "type": ["null", "string"]},
            "updatedAt": {
                "description": "Date and time when the record was last updated.",
                "type": ["null", "string"],
                "format": "date-time",
            },
            "nestedDisplayOptions": {
                "description": "Display options for the form.",
                "type": ["null", "object"],
                "properties": {
                    "theme": {
                        "description": "Theme setting for the form.",
                        "type": ["null", "string"],
                    },
                    "style": {
                        "description": "Style settings for the form.",
                        "type": ["null", "object"],
                        "properties": {
                            "fontFamily": {
                                "description": "Font family style.",
                                "type": ["null", "string"],
                            },
                        },
                    },
                },
            },
            "dynamic_property_one": {"type": ["null", "string"]},
            "dynamic_property_two": {"type": ["null", "integer"]},
        },
    }

    static_schema_loader = InlineSchemaLoader(
        schema={
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": ["null", "object"],
            "properties": {
                "id": {
                    "description": "Unique identifier for the form.",
                    "type": ["null", "string"],
                },
                "name": {"description": "Name of the record.", "type": ["null", "string"]},
                "updatedAt": {
                    "description": "Date and time when the record was last updated.",
                    "type": ["null", "string"],
                    "format": "date-time",
                },
                "nestedDisplayOptions": {
                    "description": "Display options for the form.",
                    "type": ["null", "object"],
                    "properties": {
                        "theme": {
                            "description": "Theme setting for the form.",
                            "type": ["null", "string"],
                        },
                        "style": {
                            "description": "Style settings for the form.",
                            "type": ["null", "object"],
                            "properties": {
                                "fontFamily": {
                                    "description": "Font family style.",
                                    "type": ["null", "string"],
                                },
                            },
                        },
                    },
                },
            },
        },
        parameters={},
    )

    dynamic_schema_loader.retriever.read_records = MagicMock(
        return_value=iter(
            [
                {
                    "schema": [
                        {"key": "dynamic_property_one", "type": "string"},
                        {"key": "dynamic_property_two", "type": "integer"},
                    ]
                }
            ]
        )
    )

    composite_schema = CompositeSchemaLoader(
        schema_loaders=[static_schema_loader, dynamic_schema_loader], parameters={}
    )
    schema = composite_schema.get_json_schema()

    assert schema == expected_schema


def test_subsequent_schemas_do_not_overwrite_previous():
    expected_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": ["null", "object"],
        "properties": {
            "id": {"type": ["null", "string"]},
            "name": {"type": ["null", "string"]},
            "updatedAt": {"type": ["null", "string"], "format": "date-time"},
            "organization": {
                "type": ["null", "string"],
            },
        },
    }

    first = InlineSchemaLoader(
        schema={
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": ["null", "object"],
            "properties": {
                "id": {"type": ["null", "string"]},
                "name": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
            },
        },
        parameters={},
    )

    second = InlineSchemaLoader(
        schema={
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": ["null", "object"],
            "properties": {
                "updatedAt": {"type": ["null", "string"], "format": "date"},
                "organization": {
                    "type": ["null", "string"],
                },
            },
        },
        parameters={},
    )

    third = InlineSchemaLoader(
        schema={
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": ["null", "object"],
            "properties": {
                "id": {
                    "type": ["null", "integer"],
                }
            },
        },
        parameters={},
    )

    composite_schema = CompositeSchemaLoader(schema_loaders=[first, second, third], parameters={})
    schema = composite_schema.get_json_schema()

    assert schema == expected_schema
