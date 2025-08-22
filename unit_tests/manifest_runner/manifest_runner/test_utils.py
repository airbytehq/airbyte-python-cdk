from unittest.mock import Mock, patch

from airbyte_cdk.manifest_runner.manifest_runner.utils import (
    SHOULD_MIGRATE_KEY,
    SHOULD_NORMALIZE_KEY,
    build_catalog,
    build_source,
)


class TestManifestUtils:
    """Test cases for the manifest_runner utils module."""

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

    @patch("airbyte_cdk.manifest_runner.manifest_runner.utils.ManifestDeclarativeSource")
    @patch("airbyte_cdk.manifest_runner.manifest_runner.utils.ModelToComponentFactory")
    def test_build_source_creates_manifest_declarative_source(
        self, mock_component_factory_class, mock_source_class
    ):
        """Test that build_source creates a ManifestDeclarativeSource with correct parameters."""
        # Setup mocks
        mock_component_factory = Mock()
        mock_component_factory_class.return_value = mock_component_factory
        mock_source = Mock()
        mock_source_class.return_value = mock_source

        # Test with complex manifest and config structures
        manifest = {
            "version": "0.1.0",
            "definitions": {"selector": {"extractor": {"field_path": ["data"]}}},
            "streams": [
                {
                    "name": "users",
                    "primary_key": "id",
                    "retriever": {
                        "requester": {
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        }
                    },
                }
            ],
            "check": {"stream_names": ["users"]},
        }

        config = {
            "api_key": "sk-test-123",
            "base_url": "https://api.example.com",
            "timeout": 30,
        }

        # Call build_source
        result = build_source(manifest, config)

        # Verify ModelToComponentFactory was created with correct parameters
        mock_component_factory_class.assert_called_once_with(
            emit_connector_builder_messages=True,
            limit_pages_fetched_per_slice=None,
            limit_slices_fetched=None,
            disable_retries=True,
            disable_cache=True,
        )

        # Verify ManifestDeclarativeSource was created with correct parameters
        mock_source_class.assert_called_once_with(
            source_config=manifest,
            config=config,
            normalize_manifest=False,  # Default when flag not set
            migrate_manifest=False,  # Default when flag not set
            emit_connector_builder_messages=True,
            component_factory=mock_component_factory,
        )

        assert result == mock_source

    @patch("airbyte_cdk.manifest_runner.manifest_runner.utils.ManifestDeclarativeSource")
    @patch("airbyte_cdk.manifest_runner.manifest_runner.utils.ModelToComponentFactory")
    def test_build_source_with_normalize_flag(
        self, mock_component_factory_class, mock_source_class
    ):
        """Test build_source when normalize flag is set."""
        mock_component_factory = Mock()
        mock_component_factory_class.return_value = mock_component_factory
        mock_source = Mock()
        mock_source_class.return_value = mock_source

        manifest = {"streams": [{"name": "test_stream"}], SHOULD_NORMALIZE_KEY: True}
        config = {"api_key": "test_key"}

        build_source(manifest, config)

        # Verify normalize_manifest is True
        call_args = mock_source_class.call_args[1]
        assert call_args["normalize_manifest"] is True
        assert call_args["migrate_manifest"] is False

    @patch("airbyte_cdk.manifest_runner.manifest_runner.utils.ManifestDeclarativeSource")
    @patch("airbyte_cdk.manifest_runner.manifest_runner.utils.ModelToComponentFactory")
    def test_build_source_with_migrate_flag(self, mock_component_factory_class, mock_source_class):
        """Test build_source when migrate flag is set."""
        mock_component_factory = Mock()
        mock_component_factory_class.return_value = mock_component_factory
        mock_source = Mock()
        mock_source_class.return_value = mock_source

        manifest = {"streams": [{"name": "test_stream"}], SHOULD_MIGRATE_KEY: True}
        config = {"api_key": "test_key"}

        build_source(manifest, config)

        # Verify migrate_manifest is True
        call_args = mock_source_class.call_args[1]
        assert call_args["normalize_manifest"] is False
        assert call_args["migrate_manifest"] is True
