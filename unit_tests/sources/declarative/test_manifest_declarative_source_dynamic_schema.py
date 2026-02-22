#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

"""Tests for the ManifestDeclarativeSource with DynamicSchemaLoader."""

from unittest.mock import MagicMock, patch

import pytest

from airbyte_cdk.legacy.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)
from airbyte_cdk.models import AirbyteCatalog


def test_check_config_during_discover_with_dynamic_schema_loader():
    """Test that check_config_during_discover is True when DynamicSchemaLoader is used."""
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

    assert source.check_config_during_discover is True
    assert source.check_config_against_spec is True


def test_check_config_during_discover_without_dynamic_schema_loader():
    """Test that check_config_during_discover is False when DynamicSchemaLoader is not used."""
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

    assert source.check_config_during_discover is False
    assert source.check_config_against_spec is True


@patch(
    "airbyte_cdk.legacy.sources.declarative.manifest_declarative_source.ManifestDeclarativeSource.streams"
)
def test_discover_with_dynamic_schema_loader_no_config(mock_streams):
    """Test that discovery works without config when DynamicSchemaLoader is used."""
    mock_stream = MagicMock()
    mock_stream.name = "test_dynamic_stream"

    mock_airbyte_stream = MagicMock()
    type(mock_airbyte_stream).name = "test_dynamic_stream"
    mock_stream.as_airbyte_stream.return_value = mock_airbyte_stream

    mock_streams.return_value = [mock_stream]

    source_config = {
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream"},
        "streams": [
            {
                "name": "test_dynamic_stream",
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

    assert source.check_config_during_discover is True
    assert source.check_config_against_spec is True

    logger = MagicMock()
    catalog = source.discover(logger, {})

    assert isinstance(catalog, AirbyteCatalog)
    assert len(catalog.streams) == 1
    assert catalog.streams[0].name == "test_dynamic_stream"


@patch(
    "airbyte_cdk.legacy.sources.declarative.manifest_declarative_source.ManifestDeclarativeSource.streams"
)
def test_discover_without_dynamic_schema_loader_no_config(mock_streams):
    """Test that discovery validates config when DynamicSchemaLoader is not used."""
    mock_stream = MagicMock()
    mock_stream.name = "test_static_stream"

    mock_airbyte_stream = MagicMock()
    type(mock_airbyte_stream).name = "test_static_stream"
    mock_stream.as_airbyte_stream.return_value = mock_airbyte_stream

    mock_streams.return_value = [mock_stream]

    source_config = {
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream"},
        "streams": [
            {
                "name": "test_static_stream",
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

    assert source.check_config_during_discover is False
    assert source.check_config_against_spec is True

    logger = MagicMock()
    catalog = source.discover(logger, {})

    assert isinstance(catalog, AirbyteCatalog)
    assert len(catalog.streams) == 1
    assert catalog.streams[0].name == "test_static_stream"

    assert source.check_config_during_discover is False
    assert source.check_config_against_spec is True
