import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from airbyte_cdk.manifest_server.command_processor.utils import (
    SHOULD_MIGRATE_KEY,
    SHOULD_NORMALIZE_KEY,
    build_catalog,
    build_source,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)


@pytest.mark.parametrize("manifest_server", [True, False], ids=["manifest-server", "standalone"])
def test_config_migration_only_emits_to_standalone_stdout(
    manifest_server: bool, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    secret = "MARKER-12345"
    config = {"api_key": secret}
    manifest = {
        "version": "6.7.0",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": []},
        "streams": [],
        "spec": {
            "type": "Spec",
            "connection_specification": {
                "type": "object",
                "properties": {"api_key": {"type": "string", "airbyte_secret": True}},
            },
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "config_migrations": [
                    {
                        "type": "ConfigMigration",
                        "transformations": [
                            {
                                "type": "ConfigAddFields",
                                "fields": [
                                    {
                                        "type": "AddedFieldDefinition",
                                        "path": ["migrated_marker"],
                                        "value": "added-by-migration",
                                    }
                                ],
                            }
                        ],
                    }
                ],
            },
        },
    }
    if manifest_server:
        source = build_source(manifest, catalog=None, config=config, state=None)
    else:
        source = ConcurrentDeclarativeSource(source_config=manifest, config=config)

    migrated_config = {"api_key": secret, "migrated_marker": "added-by-migration"}
    assert source.configure(config, str(tmp_path)) == migrated_config
    stdout = capsys.readouterr().out
    if manifest_server:
        assert secret not in stdout
        assert "CONNECTOR_CONFIG" not in stdout
    else:
        message = json.loads(stdout)
        assert message["type"] == "CONTROL"
        assert message["control"]["type"] == "CONNECTOR_CONFIG"
        assert message["control"]["connectorConfig"]["config"] == migrated_config


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
