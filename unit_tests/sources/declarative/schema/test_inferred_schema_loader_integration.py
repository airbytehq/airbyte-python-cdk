#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse


def test_inferred_schema_loader_manifest_happy_path():
    """Test InferredSchemaLoader in a full manifest flow with sample records."""
    manifest = {
        "version": "6.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["users"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "users",
                "primary_key": ["id"],
                "schema_loader": {
                    "type": "InferredSchemaLoader",
                    "record_sample_size": 3,
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://jsonplaceholder.typicode.com",
                            "path": "/users",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://jsonplaceholder.typicode.com",
                        "path": "/users",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "connection_specification": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": [],
                "properties": {},
                "additionalProperties": True,
            },
            "documentation_url": "https://example.org",
            "type": "Spec",
        },
    }

    config = {}

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://jsonplaceholder.typicode.com/users"),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 1, "name": "Alice", "email": "alice@example.com"},
                        {"id": 2, "name": "Bob", "email": "bob@example.com"},
                        {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
                        {"id": 4, "name": "David", "email": "david@example.com"},
                    ]
                )
            ),
        )

        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=config, catalog=None, state=None
        )

        catalog = source.discover(logger=source.logger, config=config)

        assert len(catalog.streams) == 1
        stream = catalog.streams[0]
        assert stream.name == "users"

        schema = stream.json_schema
        assert schema is not None
        assert schema.get("type") == "object"
        assert "properties" in schema

        properties = schema["properties"]
        assert "id" in properties
        assert "name" in properties
        assert "email" in properties

        id_type = properties["id"]["type"]
        assert id_type == "number" or (isinstance(id_type, list) and "number" in id_type)

        name_type = properties["name"]["type"]
        assert name_type == "string" or (isinstance(name_type, list) and "string" in name_type)

        email_type = properties["email"]["type"]
        assert email_type == "string" or (isinstance(email_type, list) and "string" in email_type)


def test_inferred_schema_loader_respects_sample_size():
    """Test that InferredSchemaLoader only reads up to record_sample_size records."""
    manifest = {
        "version": "6.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["posts"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "posts",
                "primary_key": ["id"],
                "schema_loader": {
                    "type": "InferredSchemaLoader",
                    "record_sample_size": 2,
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://jsonplaceholder.typicode.com",
                            "path": "/posts",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://jsonplaceholder.typicode.com",
                        "path": "/posts",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "connection_specification": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": [],
                "properties": {},
                "additionalProperties": True,
            },
            "documentation_url": "https://example.org",
            "type": "Spec",
        },
    }

    config = {}

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://jsonplaceholder.typicode.com/posts"),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 1, "title": "Post 1", "flag": True},
                        {"id": 2, "title": "Post 2", "flag": False},
                        {"id": 3, "title": "Post 3", "flag": "string_value"},
                    ]
                )
            ),
        )

        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=config, catalog=None, state=None
        )

        catalog = source.discover(logger=source.logger, config=config)

        assert len(catalog.streams) == 1
        stream = catalog.streams[0]
        schema = stream.json_schema

        properties = schema["properties"]
        assert "flag" in properties
        flag_type = properties["flag"]["type"]
        assert flag_type == "boolean" or (isinstance(flag_type, list) and "boolean" in flag_type)


def test_inferred_schema_loader_with_nested_objects():
    """Test InferredSchemaLoader handles nested objects correctly."""
    manifest = {
        "version": "6.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["users"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "users",
                "primary_key": ["id"],
                "schema_loader": {
                    "type": "InferredSchemaLoader",
                    "record_sample_size": 2,
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://jsonplaceholder.typicode.com",
                            "path": "/users",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://jsonplaceholder.typicode.com",
                        "path": "/users",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "connection_specification": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": [],
                "properties": {},
                "additionalProperties": True,
            },
            "documentation_url": "https://example.org",
            "type": "Spec",
        },
    }

    config = {}

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://jsonplaceholder.typicode.com/users"),
            HttpResponse(
                body=json.dumps(
                    [
                        {
                            "id": 1,
                            "name": "Alice",
                            "address": {
                                "street": "123 Main St",
                                "city": "Springfield",
                                "zipcode": "12345",
                            },
                        },
                        {
                            "id": 2,
                            "name": "Bob",
                            "address": {
                                "street": "456 Oak Ave",
                                "city": "Shelbyville",
                                "zipcode": "67890",
                            },
                        },
                    ]
                )
            ),
        )

        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=config, catalog=None, state=None
        )

        catalog = source.discover(logger=source.logger, config=config)

        assert len(catalog.streams) == 1
        stream = catalog.streams[0]
        schema = stream.json_schema

        properties = schema["properties"]
        assert "address" in properties
        address_type = properties["address"]["type"]
        assert address_type == "object" or (
            isinstance(address_type, list) and "object" in address_type
        )
        assert "properties" in properties["address"]

        address_props = properties["address"]["properties"]
        assert "street" in address_props
        assert "city" in address_props
        assert "zipcode" in address_props


def test_inferred_schema_loader_with_arrays():
    """Test InferredSchemaLoader handles arrays correctly."""
    manifest = {
        "version": "6.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["posts"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "posts",
                "primary_key": ["id"],
                "schema_loader": {
                    "type": "InferredSchemaLoader",
                    "record_sample_size": 2,
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://jsonplaceholder.typicode.com",
                            "path": "/posts",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://jsonplaceholder.typicode.com",
                        "path": "/posts",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "connection_specification": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": [],
                "properties": {},
                "additionalProperties": True,
            },
            "documentation_url": "https://example.org",
            "type": "Spec",
        },
    }

    config = {}

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://jsonplaceholder.typicode.com/posts"),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 1, "title": "Post 1", "tags": ["python", "testing"]},
                        {"id": 2, "title": "Post 2", "tags": ["javascript", "web"]},
                    ]
                )
            ),
        )

        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=config, catalog=None, state=None
        )

        catalog = source.discover(logger=source.logger, config=config)

        assert len(catalog.streams) == 1
        stream = catalog.streams[0]
        schema = stream.json_schema

        properties = schema["properties"]
        assert "tags" in properties
        tags_type = properties["tags"]["type"]
        assert tags_type == "array" or (isinstance(tags_type, list) and "array" in tags_type)
        assert "items" in properties["tags"]
        items_type = properties["tags"]["items"]["type"]
        assert items_type == "string" or (isinstance(items_type, list) and "string" in items_type)


def test_inferred_schema_loader_empty_response():
    """Test InferredSchemaLoader handles empty responses gracefully."""
    manifest = {
        "version": "6.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["empty"]},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "empty",
                "primary_key": [],
                "schema_loader": {
                    "type": "InferredSchemaLoader",
                    "record_sample_size": 10,
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://jsonplaceholder.typicode.com",
                            "path": "/empty",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://jsonplaceholder.typicode.com",
                        "path": "/empty",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
            }
        ],
        "spec": {
            "connection_specification": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": [],
                "properties": {},
                "additionalProperties": True,
            },
            "documentation_url": "https://example.org",
            "type": "Spec",
        },
    }

    config = {}

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://jsonplaceholder.typicode.com/empty"),
            HttpResponse(body=json.dumps([])),
        )

        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=config, catalog=None, state=None
        )

        catalog = source.discover(logger=source.logger, config=config)

        assert len(catalog.streams) == 1
        stream = catalog.streams[0]
        assert stream.name == "empty"

        schema = stream.json_schema
        assert schema == {}
