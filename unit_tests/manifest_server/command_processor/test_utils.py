from unittest.mock import Mock, patch

from airbyte_cdk.manifest_server.command_processor.utils import (
    SHOULD_MIGRATE_KEY,
    SHOULD_NORMALIZE_KEY,
    build_catalog,
    build_source,
)


class TestManifestUtils:
    """Test cases for the utils module."""

    def test_build_catalog_creates_correct_structure(self):
        """Test that build_catalog creates a properly structured ConfiguredAirbyteCatalog."""
        stream_name = "test_stream"
        catalog = build_catalog(stream_name)

        # Verify catalog structure
        assert len(catalog.streams) == 1

        configured_stream = catalog.streams[0]
        assert configured_stream.stream.name == stream_name
        assert configured_stream.stream.json_schema == {}

        # Verify sync modes
        from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode, SyncMode

        assert SyncMode.full_refresh in configured_stream.stream.supported_sync_modes
        assert SyncMode.incremental in configured_stream.stream.supported_sync_modes
        assert configured_stream.sync_mode == SyncMode.incremental
        assert configured_stream.destination_sync_mode == DestinationSyncMode.overwrite

    @patch("airbyte_cdk.manifest_server.command_processor.utils.ConcurrentDeclarativeSource")
    def test_build_source_with_normalize_flag(self, mock_source_class):
        """Test build_source when normalize flag is set."""
        mock_source = Mock()
        mock_source_class.return_value = mock_source

        manifest = {"streams": [{"name": "test_stream"}], SHOULD_NORMALIZE_KEY: True}
        config = {"api_key": "test_key"}
        catalog = build_catalog("test_stream")
        state = []

        build_source(manifest, catalog, config, state)

        # Verify normalize_manifest is True
        call_args = mock_source_class.call_args[1]
        assert call_args["normalize_manifest"] is True
        assert call_args["migrate_manifest"] is False

    @patch("airbyte_cdk.manifest_server.command_processor.utils.ConcurrentDeclarativeSource")
    def test_build_source_with_migrate_flag(self, mock_source_class):
        """Test build_source when migrate flag is set."""
        mock_source = Mock()
        mock_source_class.return_value = mock_source

        manifest = {"streams": [{"name": "test_stream"}], SHOULD_MIGRATE_KEY: True}
        config = {"api_key": "test_key"}
        catalog = build_catalog("test_stream")
        state = []

        build_source(manifest, catalog, config, state)

        # Verify migrate_manifest is True
        call_args = mock_source_class.call_args[1]
        assert call_args["normalize_manifest"] is False
        assert call_args["migrate_manifest"] is True
