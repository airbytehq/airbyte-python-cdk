from unittest.mock import Mock, patch

import pytest

from airbyte_cdk.manifest_runner.manifest_runner.runner import ManifestRunner


class TestManifestRunner:
    """Test cases for the ManifestRunner class."""

    @pytest.fixture
    def mock_source(self):
        """Create a mock ManifestDeclarativeSource."""
        return Mock()

    @pytest.fixture
    def manifest_runner(self, mock_source):
        """Create a ManifestRunner instance with mocked source."""
        return ManifestRunner(mock_source)

    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing."""
        return {"api_key": "test_key", "base_url": "https://api.example.com"}

    @pytest.fixture
    def sample_catalog(self):
        """Sample configured catalog for testing."""
        from airbyte_cdk.models.airbyte_protocol import (
            AirbyteStream,
            ConfiguredAirbyteCatalog,
            ConfiguredAirbyteStream,
            DestinationSyncMode,
            SyncMode,
        )

        return ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=AirbyteStream(
                        name="test_stream",
                        json_schema={"type": "object"},
                        supported_sync_modes=[SyncMode.full_refresh],
                    ),
                    sync_mode=SyncMode.full_refresh,
                    destination_sync_mode=DestinationSyncMode.overwrite,
                )
            ]
        )

    @pytest.fixture
    def sample_state(self):
        """Sample state messages for testing."""
        return []

    @patch("airbyte_cdk.manifest_runner.manifest_runner.runner.TestReader")
    def test_test_read_success(
        self, mock_test_reader_class, manifest_runner, sample_config, sample_catalog
    ):
        """Test successful test_read execution with various parameters and state messages."""
        from airbyte_cdk.models.airbyte_protocol import (
            AirbyteStateMessage,
            AirbyteStateType,
        )

        # Mock the TestReader instance and its run_test_read method
        mock_test_reader_instance = Mock()
        mock_test_reader_class.return_value = mock_test_reader_instance

        # Mock the StreamRead return value
        mock_stream_read = Mock()
        mock_test_reader_instance.run_test_read.return_value = mock_stream_read

        # Test with state messages and various parameter values
        state_messages = [
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream={
                    "stream_descriptor": {"name": "test_stream"},
                    "stream_state": {"cursor": "2023-01-01"},
                },
            )
        ]

        record_limit = 50
        page_limit = 3
        slice_limit = 7

        # Execute test_read
        result = manifest_runner.test_read(
            config=sample_config,
            catalog=sample_catalog,
            state=state_messages,
            record_limit=record_limit,
            page_limit=page_limit,
            slice_limit=slice_limit,
        )

        # Verify TestReader was initialized with correct parameters
        mock_test_reader_class.assert_called_once_with(
            max_pages_per_slice=page_limit,
            max_slices=slice_limit,
            max_record_limit=record_limit,
        )

        # Verify run_test_read was called with correct parameters including state
        mock_test_reader_instance.run_test_read.assert_called_once_with(
            source=manifest_runner._source,
            config=sample_config,
            configured_catalog=sample_catalog,
            state=state_messages,
        )

        # Verify the result is returned correctly
        assert result == mock_stream_read

    @patch("airbyte_cdk.manifest_runner.manifest_runner.runner.TestReader")
    def test_test_read_exception_handling(
        self,
        mock_test_reader_class,
        manifest_runner,
        sample_config,
        sample_catalog,
        sample_state,
    ):
        """Test that exceptions from TestReader are properly propagated."""
        mock_test_reader_instance = Mock()
        mock_test_reader_class.return_value = mock_test_reader_instance

        # Make run_test_read raise an exception
        mock_test_reader_instance.run_test_read.side_effect = Exception("Test error")

        # Verify the exception is propagated
        with pytest.raises(Exception, match="Test error"):
            manifest_runner.test_read(
                config=sample_config,
                catalog=sample_catalog,
                state=sample_state,
                record_limit=100,
                page_limit=5,
                slice_limit=10,
            )
