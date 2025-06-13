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
                "properties": {
                    "a": 1
                },
            },
            "B": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "b": 2
                },
            },
            "C": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "c": 3
                },
            },
            "D": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "d": 4
                },
            },
            "E": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "properties": {
                    "e": 5
                },
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
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {
                            "a": 1
                        },
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
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {
                            "b": 2
                        },
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
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {
                            "c": 3
                        },
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
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {
                            "d": 4
                        },
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
                    "schema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                        "properties": {
                            "e": 5
                        }
                    }
                },
            },
        ],
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
            },
        ],
    }
