from unittest.mock import Mock

import pytest

from airbyte_cdk.models import AirbyteStream, ConfiguredAirbyteCatalog, ConfiguredAirbyteStream
from airbyte_cdk.sql.shared.catalog_providers import CatalogProvider


class TestCatalogProvider:
    """Test cases for CatalogProvider.get_primary_keys() method."""

    def test_get_primary_keys_uses_configured_primary_key_when_set(self):
        """Test that configured primary_key is used when set."""
        stream = AirbyteStream(
            name="test_stream",
            json_schema={"type": "object", "properties": {"id": {"type": "string"}}},
            supported_sync_modes=["full_refresh"],
            source_defined_primary_key=[["source_id"]],
        )
        configured_stream = ConfiguredAirbyteStream(
            stream=stream,
            sync_mode="full_refresh",
            destination_sync_mode="overwrite",
            primary_key=[["configured_id"]],
        )
        catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

        provider = CatalogProvider(catalog)
        result = provider.get_primary_keys("test_stream")

        assert result == ["configured_id"]

    def test_get_primary_keys_falls_back_to_source_defined_when_configured_empty(self):
        """Test that source_defined_primary_key is used when primary_key is empty."""
        stream = AirbyteStream(
            name="test_stream",
            json_schema={"type": "object", "properties": {"id": {"type": "string"}}},
            supported_sync_modes=["full_refresh"],
            source_defined_primary_key=[["source_id"]],
        )
        configured_stream = ConfiguredAirbyteStream(
            stream=stream,
            sync_mode="full_refresh",
            destination_sync_mode="overwrite",
            primary_key=[],  # Empty configured primary key
        )
        catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

        provider = CatalogProvider(catalog)
        result = provider.get_primary_keys("test_stream")

        assert result == ["source_id"]

    def test_get_primary_keys_falls_back_to_source_defined_when_configured_none(self):
        """Test that source_defined_primary_key is used when primary_key is None."""
        stream = AirbyteStream(
            name="test_stream",
            json_schema={"type": "object", "properties": {"id": {"type": "string"}}},
            supported_sync_modes=["full_refresh"],
            source_defined_primary_key=[["source_id"]],
        )
        configured_stream = ConfiguredAirbyteStream(
            stream=stream,
            sync_mode="full_refresh",
            destination_sync_mode="overwrite",
            primary_key=None,  # None configured primary key
        )
        catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

        provider = CatalogProvider(catalog)
        result = provider.get_primary_keys("test_stream")

        assert result == ["source_id"]

    def test_get_primary_keys_returns_empty_when_both_empty(self):
        """Test that empty list is returned when both primary keys are empty."""
        stream = AirbyteStream(
            name="test_stream",
            json_schema={"type": "object", "properties": {"id": {"type": "string"}}},
            supported_sync_modes=["full_refresh"],
            source_defined_primary_key=[],  # Empty source-defined primary key
        )
        configured_stream = ConfiguredAirbyteStream(
            stream=stream,
            sync_mode="full_refresh",
            destination_sync_mode="overwrite",
            primary_key=[],  # Empty configured primary key
        )
        catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

        provider = CatalogProvider(catalog)
        result = provider.get_primary_keys("test_stream")

        assert result == []

    def test_get_primary_keys_returns_empty_when_both_none(self):
        """Test that empty list is returned when both primary keys are None."""
        stream = AirbyteStream(
            name="test_stream",
            json_schema={"type": "object", "properties": {"id": {"type": "string"}}},
            supported_sync_modes=["full_refresh"],
            source_defined_primary_key=None,  # None source-defined primary key
        )
        configured_stream = ConfiguredAirbyteStream(
            stream=stream,
            sync_mode="full_refresh",
            destination_sync_mode="overwrite",
            primary_key=None,  # None configured primary key
        )
        catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

        provider = CatalogProvider(catalog)
        result = provider.get_primary_keys("test_stream")

        assert result == []

    def test_get_primary_keys_handles_composite_keys_from_source_defined(self):
        """Test that composite primary keys work correctly with source-defined fallback."""
        stream = AirbyteStream(
            name="test_stream",
            json_schema={
                "type": "object",
                "properties": {"id1": {"type": "string"}, "id2": {"type": "string"}},
            },
            supported_sync_modes=["full_refresh"],
            source_defined_primary_key=[["id1"], ["id2"]],  # Composite primary key
        )
        configured_stream = ConfiguredAirbyteStream(
            stream=stream,
            sync_mode="full_refresh",
            destination_sync_mode="overwrite",
            primary_key=[],  # Empty configured primary key
        )
        catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

        provider = CatalogProvider(catalog)
        result = provider.get_primary_keys("test_stream")

        assert result == ["id1", "id2"]

    def test_get_primary_keys_normalizes_case_for_source_defined(self):
        """Test that primary keys from source-defined are normalized to lowercase."""
        stream = AirbyteStream(
            name="test_stream",
            json_schema={"type": "object", "properties": {"ID": {"type": "string"}}},
            supported_sync_modes=["full_refresh"],
            source_defined_primary_key=[["ID"]],  # Uppercase primary key
        )
        configured_stream = ConfiguredAirbyteStream(
            stream=stream,
            sync_mode="full_refresh",
            destination_sync_mode="overwrite",
            primary_key=[],  # Empty configured primary key
        )
        catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

        provider = CatalogProvider(catalog)
        result = provider.get_primary_keys("test_stream")

        assert result == ["id"]
