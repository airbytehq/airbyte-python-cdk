from unittest.mock import Mock

import pytest

from airbyte_cdk.models import AirbyteStream, ConfiguredAirbyteCatalog, ConfiguredAirbyteStream
from airbyte_cdk.sql.shared.catalog_providers import CatalogProvider


class TestCatalogProvider:
    """Test cases for CatalogProvider.get_primary_keys() method."""

    @pytest.mark.parametrize(
        "configured_primary_key,source_defined_primary_key,expected_result,test_description",
        [
            (["configured_id"], ["source_id"], ["source_id"], "prioritizes source when both set"),
            ([], ["source_id"], ["source_id"], "uses source when configured empty"),
            (None, ["source_id"], ["source_id"], "uses source when configured None"),
            (
                ["configured_id"],
                [],
                ["configured_id"],
                "falls back to configured when source empty",
            ),
            (
                ["configured_id"],
                None,
                ["configured_id"],
                "falls back to configured when source None",
            ),
            ([], [], None, "returns None when both empty"),
            (None, None, None, "returns None when both None"),
            ([], ["id1", "id2"], ["id1", "id2"], "handles composite keys from source"),
        ],
    )
    def test_get_primary_keys_parametrized(
        self, configured_primary_key, source_defined_primary_key, expected_result, test_description
    ):
        """Test primary key fallback logic with various input combinations."""
        configured_pk_wrapped = (
            None
            if configured_primary_key is None
            else [[pk] for pk in configured_primary_key]
            if configured_primary_key
            else []
        )
        source_pk_wrapped = (
            None
            if source_defined_primary_key is None
            else [[pk] for pk in source_defined_primary_key]
            if source_defined_primary_key
            else []
        )

        stream = AirbyteStream(
            name="test_stream",
            json_schema={
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "id1": {"type": "string"},
                    "id2": {"type": "string"},
                },
            },
            supported_sync_modes=["full_refresh"],
            source_defined_primary_key=source_pk_wrapped,
        )
        configured_stream = ConfiguredAirbyteStream(
            stream=stream,
            sync_mode="full_refresh",
            destination_sync_mode="overwrite",
            primary_key=configured_pk_wrapped,
        )
        catalog = ConfiguredAirbyteCatalog(streams=[configured_stream])

        provider = CatalogProvider(catalog)
        result = provider.get_primary_keys("test_stream")

        assert result == expected_result
