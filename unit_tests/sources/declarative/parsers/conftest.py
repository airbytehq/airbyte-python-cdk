#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Dict

import pytest
import yaml


@pytest.fixture
def manifest_with_multiple_url_base() -> Dict[str, Any]:
    return {
        "type": "DeclarativeSource",
        "definitions": {
            "streams": {
                "A": {
                    "type": "DeclarativeStream",
                    "name": "A",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_A",
                            "path": "A",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                        "decoder": {"type": "JsonDecoder"},
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
                            "$ref": "#/definitions/requester_B",
                            "path": "B",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                        "decoder": {"type": "JsonDecoder"},
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
                            "$ref": "#/definitions/requester_A",
                            "path": "C",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                        "decoder": {"type": "JsonDecoder"},
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
                            "path": "D",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                        "decoder": {"type": "JsonDecoder"},
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
                            "path": "E",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                        "decoder": {"type": "JsonDecoder"},
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/E"},
                    },
                },
            },
            # dummy requesters to be resolved and deduplicated
            # to the linked `url_base` in the `definitions.linked` section
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
                "properties": {},
            },
            "B": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "C": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "D": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "E": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
        },
    }


@pytest.fixture
def expected_manifest_with_multiple_url_base_normalized() -> Dict[str, Any]:
    return {
        "type": "DeclarativeSource",
        "definitions": {"linked": {"HttpRequester": {"url_base": "https://example.com/v2/"}}},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "A",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.com/v1/",
                        "path": "A",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "decoder": {"type": "JsonDecoder"},
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/A"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "B",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": {"$ref": "#/definitions/linked/HttpRequester/url_base"},
                        "path": "B",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "decoder": {"type": "JsonDecoder"},
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/B"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "C",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.com/v1/",
                        "path": "C",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "decoder": {"type": "JsonDecoder"},
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/C"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "D",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": {"$ref": "#/definitions/linked/HttpRequester/url_base"},
                        "path": "D",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "decoder": {"type": "JsonDecoder"},
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/D"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "E",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": {"$ref": "#/definitions/linked/HttpRequester/url_base"},
                        "path": "E",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "decoder": {"type": "JsonDecoder"},
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/E"},
                },
            },
        ],
        "schemas": {
            "A": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "B": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "C": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "D": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "E": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
        },
    }


@pytest.fixture
def manifest_with_url_base_linked_definition() -> Dict[str, Any]:
    return {
        "type": "DeclarativeSource",
        "definitions": {
            "linked": {"HttpRequester": {"url_base": "https://example.com/v2/"}},
            "streams": {
                "A": {
                    "type": "DeclarativeStream",
                    "name": "A",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$ref": "#/definitions/requester_A",
                            "path": "A",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                        "decoder": {"type": "JsonDecoder"},
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
                            "$ref": "#/definitions/requester_B",
                            "path": "B",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                        "decoder": {"type": "JsonDecoder"},
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
                            "$ref": "#/definitions/requester_A",
                            "path": "C",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                        "decoder": {"type": "JsonDecoder"},
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
                            "path": "D",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                        "decoder": {"type": "JsonDecoder"},
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
                            "path": "E",
                            "http_method": "GET",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                        "decoder": {"type": "JsonDecoder"},
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {"$ref": "#/schemas/E"},
                    },
                },
            },
            # dummy requesters to be resolved and deduplicated
            # to the linked `url_base` in the `definitions.linked` section
            "requester_A": {
                "type": "HttpRequester",
                "url_base": "https://example.com/v1/",
            },
            "requester_B": {
                "type": "HttpRequester",
                "url_base": {"$ref": "#/definitions/linked/HttpRequester/url_base"},
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
                "properties": {},
            },
            "B": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "C": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "D": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "E": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
        },
    }


@pytest.fixture
def expected_manifest_with_url_base_linked_definition_normalized() -> Dict[str, Any]:
    return {
        "type": "DeclarativeSource",
        "definitions": {"linked": {"HttpRequester": {"url_base": "https://example.com/v2/"}}},
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "A",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.com/v1/",
                        "path": "A",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "decoder": {"type": "JsonDecoder"},
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/A"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "B",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": {"$ref": "#/definitions/linked/HttpRequester/url_base"},
                        "path": "B",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "decoder": {"type": "JsonDecoder"},
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/B"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "C",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://example.com/v1/",
                        "path": "C",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "decoder": {"type": "JsonDecoder"},
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/C"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "D",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": {"$ref": "#/definitions/linked/HttpRequester/url_base"},
                        "path": "D",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "decoder": {"type": "JsonDecoder"},
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/D"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "E",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": {"$ref": "#/definitions/linked/HttpRequester/url_base"},
                        "path": "E",
                        "http_method": "GET",
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "decoder": {"type": "JsonDecoder"},
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/E"},
                },
            },
        ],
        "schemas": {
            "A": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "B": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "C": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "D": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
            "E": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {},
            },
        },
    }


@pytest.fixture
def manifest_with_linked_definitions_url_base_authenticator_abnormal_schemas() -> Dict[str, Any]:
    with open(
        "unit_tests/sources/declarative/parsers/resources/abnormal_schemas_manifest.yaml",
        "r",
    ) as file:
        return dict(yaml.safe_load(file))


@pytest.fixture
def expected_manifest_with_linked_definitions_url_base_authenticator_normalized() -> Dict[str, Any]:
    return {
        "version": "6.44.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["pokemon"]},
        "definitions": {
            "linked": {
                "HttpRequester": {
                    "url_base": "https://pokeapi.co/api/v1/",
                    "authenticator": {
                        "type": "ApiKeyAuthenticator",
                        "api_token": '{{ config["api_key"] }}',
                        "inject_into": {
                            "type": "RequestOption",
                            "field_name": "API_KEY",
                            "inject_into": "header",
                        },
                    },
                }
            }
        },
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "pokemon",
                "retriever": {
                    "type": "SimpleRetriever",
                    "decoder": {"type": "JsonDecoder"},
                    "requester": {
                        "type": "HttpRequester",
                        "path": "pokemon",
                        "url_base": {
                            "$ref": "#/definitions/linked/HttpRequester/url_base",
                        },
                        "http_method": "GET",
                        "authenticator": {
                            "$ref": "#/definitions/linked/HttpRequester/authenticator",
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/pokemon"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "trainers",
                "retriever": {
                    "type": "SimpleRetriever",
                    "decoder": {"type": "JsonDecoder"},
                    "requester": {
                        "type": "HttpRequester",
                        "path": "pokemon",
                        "url_base": {
                            "$ref": "#/definitions/linked/HttpRequester/url_base",
                        },
                        "http_method": "GET",
                        "authenticator": {
                            "$ref": "#/definitions/linked/HttpRequester/authenticator",
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/trainers"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "items",
                "retriever": {
                    "type": "SimpleRetriever",
                    "decoder": {"type": "JsonDecoder"},
                    "requester": {
                        "type": "HttpRequester",
                        "path": "pokemon",
                        "url_base": "https://pokeapi.co/api/v2/",
                        "http_method": "GET",
                        "authenticator": {
                            "$ref": "#/definitions/linked/HttpRequester/authenticator"
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/items"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "location",
                "retriever": {
                    "type": "SimpleRetriever",
                    "decoder": {"type": "JsonDecoder"},
                    "requester": {
                        "type": "HttpRequester",
                        "path": "location",
                        "url_base": "https://pokeapi.co/api/v2/",
                        "http_method": "GET",
                        "authenticator": {
                            "$ref": "#/definitions/linked/HttpRequester/authenticator"
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/location"},
                },
            },
            {
                "type": "DeclarativeStream",
                "name": "berries",
                "retriever": {
                    "type": "SimpleRetriever",
                    "decoder": {"type": "JsonDecoder"},
                    "requester": {
                        "type": "HttpRequester",
                        "path": "berries",
                        "url_base": "https://pokeapi.co/api/v2/",
                        "http_method": "GET",
                        "authenticator": {
                            "$ref": "#/definitions/linked/HttpRequester/authenticator"
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {"$ref": "#/schemas/berries"},
                },
            },
        ],
        "spec": {
            "type": "Spec",
            "connection_specification": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "required": ["api_key"],
                "properties": {
                    "api_key": {
                        "type": "string",
                        "order": 0,
                        "title": "API Key",
                        "airbyte_secret": True,
                    }
                },
                "additionalProperties": True,
            },
        },
        "metadata": {
            "assist": {},
            "testedStreams": {
                "berries": {"streamHash": None},
                "pokemon": {"streamHash": None},
                "location": {"streamHash": None},
                "trainers": {"streamHash": "ca4ee51a2aaa2a53b9c0b91881a84ad621da575f"},
                "items": {"streamHash": "12e624ecf47c6357c74c27d6a65c72e437b1534a"},
            },
            "autoImportSchema": {"berries": True, "pokemon": True, "location": True},
        },
        "schemas": {
            "berries": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "properties": {
                    "name": {"type": "string"},
                    "berry_type": {"type": "integer"},
                },
                "additionalProperties": True,
            },
            "pokemon": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "properties": {
                    "name": {"type": "string"},
                    "pokemon_type": {"type": "integer"},
                },
            },
            "location": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "properties": {
                    "name": {"type": "string"},
                    "location_type": {"type": "string"},
                },
            },
            "trainers": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "properties": {
                    "name": {"type": "string"},
                    "pokemon_type": {"type": "integer"},
                },
            },
            "items": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "properties": {
                    "name": {"type": "string"},
                    "pokemon_type": {"type": "integer"},
                },
            },
        },
    }
