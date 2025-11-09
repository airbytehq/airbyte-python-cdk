#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.schema import InferredSchemaLoader


@pytest.fixture
def mock_retriever():
    """Create a mock retriever that returns sample records."""
    retriever = MagicMock()
    retriever.stream_slices.return_value = iter([None])
    retriever.read_records.return_value = iter(
        [
            {"id": 1, "name": "Alice", "age": 30, "active": True},
            {"id": 2, "name": "Bob", "age": 25, "active": False},
            {"id": 3, "name": "Charlie", "age": 35, "active": True},
        ]
    )
    return retriever


@pytest.fixture
def inferred_schema_loader(mock_retriever):
    """Create an InferredSchemaLoader with a mock retriever."""
    config = MagicMock()
    parameters = {"name": "users"}
    return InferredSchemaLoader(
        retriever=mock_retriever,
        config=config,
        parameters=parameters,
        record_sample_size=3,
        stream_name="users",
    )


def test_inferred_schema_loader_basic(inferred_schema_loader):
    """Test that InferredSchemaLoader correctly infers schema from sample records."""
    schema = inferred_schema_loader.get_json_schema()

    assert "$schema" in schema
    assert schema["type"] == "object"
    assert "properties" in schema

    assert "id" in schema["properties"]
    assert "name" in schema["properties"]
    assert "age" in schema["properties"]
    assert "active" in schema["properties"]

    assert "number" in schema["properties"]["id"]["type"]
    assert "string" in schema["properties"]["name"]["type"]
    assert "number" in schema["properties"]["age"]["type"]
    assert "boolean" in schema["properties"]["active"]["type"]


def test_inferred_schema_loader_empty_records():
    """Test that InferredSchemaLoader returns empty schema when no records are available."""
    retriever = MagicMock()
    retriever.stream_slices.return_value = iter([None])
    retriever.read_records.return_value = iter([])

    config = MagicMock()
    parameters = {"name": "users"}
    loader = InferredSchemaLoader(
        retriever=retriever,
        config=config,
        parameters=parameters,
        record_sample_size=100,
        stream_name="users",
    )

    schema = loader.get_json_schema()

    assert schema == {}


def test_inferred_schema_loader_respects_sample_size():
    """Test that InferredSchemaLoader respects the record_sample_size parameter."""
    retriever = MagicMock()
    records = [{"id": i, "name": f"User{i}"} for i in range(10)]
    retriever.stream_slices.return_value = iter([None])
    retriever.read_records.return_value = iter(records)

    config = MagicMock()
    parameters = {"name": "users"}
    loader = InferredSchemaLoader(
        retriever=retriever,
        config=config,
        parameters=parameters,
        record_sample_size=5,
        stream_name="users",
    )

    schema = loader.get_json_schema()

    assert "properties" in schema
    assert "id" in schema["properties"]
    assert "name" in schema["properties"]


def test_inferred_schema_loader_handles_errors():
    """Test that InferredSchemaLoader propagates errors from the retriever."""
    retriever = MagicMock()
    retriever.stream_slices.return_value = iter([None])
    retriever.read_records.side_effect = Exception("API Error")

    config = MagicMock()
    parameters = {"name": "users"}
    loader = InferredSchemaLoader(
        retriever=retriever,
        config=config,
        parameters=parameters,
        record_sample_size=100,
        stream_name="users",
    )

    with pytest.raises(Exception, match="API Error"):
        loader.get_json_schema()


def test_inferred_schema_loader_with_nested_objects():
    """Test that InferredSchemaLoader handles nested objects correctly."""
    retriever = MagicMock()
    retriever.stream_slices.return_value = iter([None])
    retriever.read_records.return_value = iter(
        [
            {
                "id": 1,
                "name": "Alice",
                "address": {"street": "123 Main St", "city": "Springfield", "zip": "12345"},
            },
            {
                "id": 2,
                "name": "Bob",
                "address": {"street": "456 Oak Ave", "city": "Shelbyville", "zip": "67890"},
            },
        ]
    )

    config = MagicMock()
    parameters = {"name": "users"}
    loader = InferredSchemaLoader(
        retriever=retriever,
        config=config,
        parameters=parameters,
        record_sample_size=2,
        stream_name="users",
    )

    schema = loader.get_json_schema()

    assert "properties" in schema
    assert "address" in schema["properties"]
    assert "object" in schema["properties"]["address"]["type"]


def test_inferred_schema_loader_with_arrays():
    """Test that InferredSchemaLoader handles arrays correctly."""
    retriever = MagicMock()
    retriever.stream_slices.return_value = iter([None])
    retriever.read_records.return_value = iter(
        [
            {"id": 1, "name": "Alice", "tags": ["admin", "user"]},
            {"id": 2, "name": "Bob", "tags": ["user", "guest"]},
        ]
    )

    config = MagicMock()
    parameters = {"name": "users"}
    loader = InferredSchemaLoader(
        retriever=retriever,
        config=config,
        parameters=parameters,
        record_sample_size=2,
        stream_name="users",
    )

    schema = loader.get_json_schema()

    assert "properties" in schema
    assert "tags" in schema["properties"]
    assert "array" in schema["properties"]["tags"]["type"]
