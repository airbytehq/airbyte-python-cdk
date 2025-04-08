#
#

from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource


def test_check_config_against_spec_with_dynamic_schema_loader():
    """Test that check_config_against_spec is False when DynamicSchemaLoader is used."""
    source_config = {
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream"},
        "streams": [
            {
                "name": "test_stream",
                "schema_loader": {
                    "type": "DynamicSchemaLoader",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {"url_base": "https://example.com", "http_method": "GET"},
                        "record_selector": {"extractor": {"field_path": []}},
                    },
                    "schema_type_identifier": {
                        "key_pointer": ["name"],
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {"url_base": "https://example.com", "http_method": "GET"},
                    "record_selector": {"extractor": {"field_path": []}},
                },
            }
        ],
        "version": "0.1.0",
    }

    source = ManifestDeclarativeSource(source_config=source_config)

    assert source.check_config_against_spec is False


def test_check_config_against_spec_without_dynamic_schema_loader():
    """Test that check_config_against_spec is True when DynamicSchemaLoader is not used."""
    source_config = {
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream"},
        "streams": [
            {
                "name": "test_stream",
                "schema_loader": {"type": "InlineSchemaLoader", "schema": {}},
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {"url_base": "https://example.com", "http_method": "GET"},
                    "record_selector": {"extractor": {"field_path": []}},
                },
            }
        ],
        "version": "0.1.0",
    }

    source = ManifestDeclarativeSource(source_config=source_config)

    assert source.check_config_against_spec is True
