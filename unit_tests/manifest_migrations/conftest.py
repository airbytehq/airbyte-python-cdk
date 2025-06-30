#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Dict

import pytest

from airbyte_cdk.manifest_migrations.manifest_migration import ManifestMigration


@pytest.fixture
def manifest_with_url_base_to_migrate_to_url() -> Dict[str, Any]:
    return {
        "version": "6.48.3",
        "type": "DeclarativeSource",
        "check": {
            "type": "CheckStream",
            "stream_names": ["A"],
        },
        "definitions": {
            "streams": {
                "A": {
                    "type": "DeclarativeStream",
                    "name": "A",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_A",
                            "path": "/path_to_A",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/A"},
                    },
                },
                "B": {
                    "type": "DeclarativeStream",
                    "name": "B",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_A",
                            "path": "path_to_A",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/B"},
                    },
                },
                "C": {
                    "type": "DeclarativeStream",
                    "name": "C",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_B",
                            "path": "path_to_B",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/C"},
                    },
                },
                "D": {
                    "type": "DeclarativeStream",
                    "name": "D",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_B",
                            # ! the double-slash is intentional here for the test.
                            "path": "//path_to_B",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/D"},
                    },
                },
                "E": {
                    "type": "DeclarativeStream",
                    "name": "E",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_B",
                            "path": "/path_to_B",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/E"},
                    },
                },
            },
            # both requesters have duplicated `url_base`,
            # which should be migrated to `url` in the new format
            # and the `url_base` and `path` key should be removed
            "requester_A": {
                "type": "HttpRequester",
                "url_base": "https://example.com/v1/",
            },
            "requester_B": {
                "type": "HttpRequester",
                "url_base": "https://example.com/v2/",
            },
        },
        "streams": [
            {"$ref": "#/definitions/streams/A"},
            {"$ref": "#/definitions/streams/B"},
            {"$ref": "#/definitions/streams/C"},
            {"$ref": "#/definitions/streams/D"},
            {"$ref": "#/definitions/streams/E"},
        ],
        "schemas": {
            "A": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "field_a1": {
                        "type": "string",
                    },
                },
            },
            "B": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "field_b1": {
                        "type": "string",
                    },
                },
            },
            "C": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "field_c1": {
                        "type": "string",
                    },
                },
            },
            "D": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "field_d1": {
                        "type": "string",
                    },
                },
            },
            "E": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "field_e1": {
                        "type": "string",
                    },
                },
            },
        },
    }


@pytest.fixture
def expected_manifest_with_url_base_migrated_to_url() -> Dict[str, Any]:
    return {
        "version": "6.48.3",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["A"]},
        "definitions": {
            "streams": {
                "A": {
                    "type": "DeclarativeStream",
                    "name": "A",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "http_method": "GET",
                            "url": "https://example.com/v1/path_to_A",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "additionalProperties": True,
                            "properties": {"field_a1": {"type": "string"}},
                        },
                    },
                },
                "B": {
                    "type": "DeclarativeStream",
                    "name": "B",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "http_method": "GET",
                            "url": "https://example.com/v1/path_to_A",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "additionalProperties": True,
                            "properties": {"field_b1": {"type": "string"}},
                        },
                    },
                },
                "C": {
                    "type": "DeclarativeStream",
                    "name": "C",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "http_method": "GET",
                            "url": "https://example.com/v2/path_to_B",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "additionalProperties": True,
                            "properties": {"field_c1": {"type": "string"}},
                        },
                    },
                },
                "D": {
                    "type": "DeclarativeStream",
                    "name": "D",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "http_method": "GET",
                            "url": "https://example.com/v2/path_to_B",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "additionalProperties": True,
                            "properties": {"field_d1": {"type": "string"}},
                        },
                    },
                },
                "E": {
                    "type": "DeclarativeStream",
                    "name": "E",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "http_method": "GET",
                            "url": "https://example.com/v2/path_to_B",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "additionalProperties": True,
                            "properties": {"field_e1": {"type": "string"}},
                        },
                    },
                },
            },
            "requester_A": {"type": "HttpRequester", "url": "https://example.com/v1/"},
            "requester_B": {"type": "HttpRequester", "url": "https://example.com/v2/"},
        },
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "A",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v1/path_to_A",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_a1": {"type": "string"}},
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "B",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v1/path_to_A",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_b1": {"type": "string"}},
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "C",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v2/path_to_B",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_c1": {"type": "string"}},
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "D",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v2/path_to_B",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_d1": {"type": "string"}},
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "E",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v2/path_to_B",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_e1": {"type": "string"}},
                    },
                },
            },
        ],
        "schemas": {
            "A": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_a1": {"type": "string"}},
            },
            "B": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_b1": {"type": "string"}},
            },
            "C": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_c1": {"type": "string"}},
            },
            "D": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_d1": {"type": "string"}},
            },
            "E": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_e1": {"type": "string"}},
            },
        },
        "metadata": {
            "applied_migrations": [
                {
                    "from_version": "6.48.3",
                    "to_version": ">=6.48.2,<6.50.0",
                    "migration": "HttpRequesterUrlBaseToUrl",
                    "migrated_at": "2025-04-01T00:00:00+00:00",  # time freezed in the test
                },
                {
                    "from_version": "6.48.3",
                    "to_version": ">=6.48.2,<6.50.0",
                    "migration": "HttpRequesterPathToUrl",
                    "migrated_at": "2025-04-01T00:00:00+00:00",  # time freezed in the test
                },
            ]
        },
    }


@pytest.fixture
def manifest_with_migrated_url_base_and_path_is_joined_to_url() -> Dict[str, Any]:
    return {
        "version": "6.48.3",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["A"]},
        "definitions": {},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "A",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v1/path_to_A",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_a1": {"type": "string"}},
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "B",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v2/path_to_B",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_b1": {"type": "string"}},
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "C",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v2/path_to_B",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_c1": {"type": "string"}},
                    },
                },
            },
        ],
        "schemas": {
            "A": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_a1": {"type": "string"}},
            },
            "B": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_b1": {"type": "string"}},
            },
            "C": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_c1": {"type": "string"}},
            },
        },
    }


@pytest.fixture
def manifest_with_request_body_json_and_data_to_migrate_to_request_body() -> Dict[str, Any]:
    return {
        "version": "0.0.0",
        "type": "DeclarativeSource",
        "check": {
            "type": "CheckStream",
            "stream_names": ["A"],
        },
        "definitions": {
            "streams": {
                "A": {
                    "type": "DeclarativeStream",
                    "name": "A",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_A",
                            "path": "/path_to_A",
                            "http_method": "GET",
                            # this requester has a `request_body_data` key,
                            # to be migrated to the `request_body` key
                            "request_body_data": {
                                "test_key": "{{ config['config_key'] }}",
                                "test_key_2": "test_value_2",
                                "test_key_3": 123,
                            },
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/A"},
                    },
                },
                "B": {
                    "type": "DeclarativeStream",
                    "name": "B",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_A",
                            "path": "path_to_A",
                            "http_method": "GET",
                            # this requester has a `request_body_data` key,
                            # to be migrated to the `request_body` key
                            "request_body_data": "&test_key=TestValue&test_key_2=test_value_2",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/B"},
                    },
                },
                "C": {
                    "type": "DeclarativeStream",
                    "name": "C",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_B",
                            "path": "path_to_B",
                            "http_method": "GET",
                            # the `request_body_json` is expected to be migrated to the `request_body` key
                            "request_body_json": {
                                "reportType": "test_report",
                                "groupBy": "GROUP",
                                "metrics": "{{ ','.join( ['a-b','cd','e-f-g-h'] ) }}",
                            },
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/C"},
                    },
                },
                "D": {
                    "type": "DeclarativeStream",
                    "name": "D",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_B",
                            # ! the double-slash is intentional here for the test.
                            "path": "//path_to_B",
                            "http_method": "GET",
                            # the `request_body_json` is expected to be migrated to the `request_body` key
                            "request_body_json": """{"nested": { "key": "{{ config['option'] }}" }}""",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/D"},
                    },
                },
                "E": {
                    "type": "DeclarativeStream",
                    "name": "E",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_B",
                            "path": "/path_to_B",
                            "http_method": "GET",
                            # the `request_body_json` is expected to be migrated to the `request_body` key,
                            # this example holds the GraphQL query object.
                            "request_body_json": {
                                "query": "query { {{ config['query_field'] }} { {{ config['query_value'] }} }}",
                                "variables": {"arg1": "test"},
                            },
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/E"},
                    },
                },
            },
            # both requesters have duplicated `url_base`,
            # which should be migrated to `url` in the new format
            # and the `url_base` and `path` key should be removed
            "requester_A": {
                "type": "HttpRequester",
                "url_base": "https://example.com/v1/",
            },
            "requester_B": {
                "type": "HttpRequester",
                "url_base": "https://example.com/v2/",
            },
        },
        "streams": [
            {"$ref": "#/definitions/streams/A"},
            {"$ref": "#/definitions/streams/B"},
            {"$ref": "#/definitions/streams/C"},
            {"$ref": "#/definitions/streams/D"},
            {"$ref": "#/definitions/streams/E"},
        ],
        "schemas": {
            "A": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "field_a1": {
                        "type": "string",
                    },
                },
            },
            "B": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "field_b1": {
                        "type": "string",
                    },
                },
            },
            "C": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "field_c1": {
                        "type": "string",
                    },
                },
            },
            "D": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "field_d1": {
                        "type": "string",
                    },
                },
            },
            "E": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "field_e1": {
                        "type": "string",
                    },
                },
            },
        },
    }


@pytest.fixture
def expected_manifest_with_migrated_to_request_body() -> Dict[str, Any]:
    return {
        "version": "0.0.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["A"]},
        "definitions": {
            "streams": {
                "A": {
                    "type": "DeclarativeStream",
                    "name": "A",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "http_method": "GET",
                            "url": "https://example.com/v1/path_to_A",
                            "request_body": {
                                "type": "RequestBodyUrlEncodedForm",
                                "value": {
                                    "test_key": "{{ config['config_key'] }}",
                                    "test_key_2": "test_value_2",
                                    "test_key_3": 123,
                                },
                            },
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "additionalProperties": True,
                            "properties": {"field_a1": {"type": "string"}},
                        },
                    },
                },
                "B": {
                    "type": "DeclarativeStream",
                    "name": "B",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "http_method": "GET",
                            "url": "https://example.com/v1/path_to_A",
                            "request_body": {
                                "type": "RequestBodyUrlEncodedForm",
                                "value": "&test_key=TestValue&test_key_2=test_value_2",
                            },
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "additionalProperties": True,
                            "properties": {"field_b1": {"type": "string"}},
                        },
                    },
                },
                "C": {
                    "type": "DeclarativeStream",
                    "name": "C",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "http_method": "GET",
                            "url": "https://example.com/v2/path_to_B",
                            "request_body": {
                                "type": "RequestBodyJsonObject",
                                "value": {
                                    "reportType": "test_report",
                                    "groupBy": "GROUP",
                                    "metrics": "{{ ','.join( ['a-b','cd','e-f-g-h'] ) }}",
                                },
                            },
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "additionalProperties": True,
                            "properties": {"field_c1": {"type": "string"}},
                        },
                    },
                },
                "D": {
                    "type": "DeclarativeStream",
                    "name": "D",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "http_method": "GET",
                            "url": "https://example.com/v2/path_to_B",
                            "request_body": {
                                "type": "RequestBodyPlainText",
                                "value": '{"nested": { "key": "{{ config[\'option\'] }}" }}',
                            },
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "additionalProperties": True,
                            "properties": {"field_d1": {"type": "string"}},
                        },
                    },
                },
                "E": {
                    "type": "DeclarativeStream",
                    "name": "E",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "http_method": "GET",
                            "url": "https://example.com/v2/path_to_B",
                            "request_body": {
                                "type": "RequestBodyGraphQL",
                                "value": {
                                    "query": "query { {{ config['query_field'] }} { {{ config['query_value'] }} }}",
                                    "variables": {"arg1": "test"},
                                },
                            },
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "additionalProperties": True,
                            "properties": {"field_e1": {"type": "string"}},
                        },
                    },
                },
            },
            "requester_A": {"type": "HttpRequester", "url": "https://example.com/v1/"},
            "requester_B": {"type": "HttpRequester", "url": "https://example.com/v2/"},
        },
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "A",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v1/path_to_A",
                        "request_body": {
                            "type": "RequestBodyUrlEncodedForm",
                            "value": {
                                "test_key": "{{ config['config_key'] }}",
                                "test_key_2": "test_value_2",
                                "test_key_3": 123,
                            },
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_a1": {"type": "string"}},
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "B",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v1/path_to_A",
                        "request_body": {
                            "type": "RequestBodyUrlEncodedForm",
                            "value": "&test_key=TestValue&test_key_2=test_value_2",
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_b1": {"type": "string"}},
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "C",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v2/path_to_B",
                        "request_body": {
                            "type": "RequestBodyJsonObject",
                            "value": {
                                "reportType": "test_report",
                                "groupBy": "GROUP",
                                "metrics": "{{ ','.join( ['a-b','cd','e-f-g-h'] ) }}",
                            },
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_c1": {"type": "string"}},
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "D",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v2/path_to_B",
                        "request_body": {
                            "type": "RequestBodyPlainText",
                            "value": '{"nested": { "key": "{{ config[\'option\'] }}" }}',
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_d1": {"type": "string"}},
                    },
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "E",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "http_method": "GET",
                        "url": "https://example.com/v2/path_to_B",
                        "request_body": {
                            "type": "RequestBodyGraphQL",
                            "value": {
                                "query": "query { {{ config['query_field'] }} { {{ config['query_value'] }} }}",
                                "variables": {"arg1": "test"},
                            },
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {"field_e1": {"type": "string"}},
                    },
                },
            },
        ],
        "schemas": {
            "A": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_a1": {"type": "string"}},
            },
            "B": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_b1": {"type": "string"}},
            },
            "C": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_c1": {"type": "string"}},
            },
            "D": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_d1": {"type": "string"}},
            },
            "E": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {"field_e1": {"type": "string"}},
            },
        },
        "metadata": {
            "applied_migrations": [
                {
                    "from_version": "0.0.0",
                    "to_version": "*",
                    "migration": "HttpRequesterUrlBaseToUrl",
                    "migrated_at": "2025-04-01T00:00:00+00:00",
                },
                {
                    "from_version": "0.0.0",
                    "to_version": "*",
                    "migration": "HttpRequesterPathToUrl",
                    "migrated_at": "2025-04-01T00:00:00+00:00",
                },
                {
                    "from_version": "0.0.0",
                    "to_version": "*",
                    "migration": "HttpRequesterRequestBodyJsonDataToRequestBody",
                    "migrated_at": "2025-04-01T00:00:00+00:00",
                },
            ]
        },
    }


class DummyMigration(ManifestMigration):
    def _process_manifest(self, manifest):
        self.is_migrated = False

    def should_migrate(self, manifest):
        return True

    def validate(self, manifest):
        return True

    def migrate(self, manifest):
        pass
