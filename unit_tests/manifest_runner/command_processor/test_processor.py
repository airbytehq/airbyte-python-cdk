from unittest.mock import Mock, patch

import pytest
from airbyte_protocol_dataclasses.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStream,
    AirbyteTraceMessage,
    Status,
    TraceType,
)
from airbyte_protocol_dataclasses.models import Type as AirbyteMessageType

from airbyte_cdk.manifest_runner.command_processor.processor import ManifestCommandProcessor


class TestManifestCommandProcessor:
    """Test cases for the ManifestCommandProcessor class."""

    @pytest.fixture
    def mock_source(self):
        """Create a mock ManifestDeclarativeSource."""
        return Mock()

    @pytest.fixture
    def manifest_runner(self, mock_source):
        """Create a ManifestCommandProcessor instance with mocked source."""
        return ManifestCommandProcessor(mock_source)

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

    @patch("airbyte_cdk.manifest_runner.command_processor.processor.TestReader")
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
            stream_name="test_stream",
            state=state_messages,
            record_limit=record_limit,
        )

        # Verify the result is returned correctly
        assert result == mock_stream_read

    @patch("airbyte_cdk.manifest_runner.command_processor.processor.TestReader")
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

    @patch("airbyte_cdk.manifest_runner.command_processor.processor.AirbyteEntrypoint")
    def test_check_connection_success(self, mock_entrypoint_class, manifest_runner, sample_config):
        """Test successful check_connection execution."""

        # Mock the spec method
        manifest_runner._source.spec.return_value = Mock()

        # Mock the entrypoint instance and its check method
        mock_entrypoint_instance = Mock()
        mock_entrypoint_class.return_value = mock_entrypoint_instance

        # Create mock messages with successful connection status
        connection_status = AirbyteConnectionStatus(
            status=Status.SUCCEEDED, message="Connection test succeeded"
        )
        mock_message = AirbyteMessage(
            type=AirbyteMessageType.CONNECTION_STATUS, connectionStatus=connection_status
        )
        mock_entrypoint_instance.check.return_value = [mock_message]

        # Execute check_connection
        success, message = manifest_runner.check_connection(sample_config)

        # Verify the result
        assert success is True
        assert message == "Connection test succeeded"

        # Verify spec was called
        manifest_runner._source.spec.assert_called_once()

        # Verify entrypoint was created and check was called
        mock_entrypoint_class.assert_called_once_with(source=manifest_runner._source)
        mock_entrypoint_instance.check.assert_called_once()

    @patch("airbyte_cdk.manifest_runner.command_processor.processor.AirbyteEntrypoint")
    def test_check_connection_failure(self, mock_entrypoint_class, manifest_runner, sample_config):
        """Test check_connection with failed status."""

        # Mock the spec method
        manifest_runner._source.spec.return_value = Mock()

        # Mock the entrypoint instance
        mock_entrypoint_instance = Mock()
        mock_entrypoint_class.return_value = mock_entrypoint_instance

        # Create mock messages with failed connection status
        connection_status = AirbyteConnectionStatus(status=Status.FAILED, message="Invalid API key")
        mock_message = AirbyteMessage(
            type=AirbyteMessageType.CONNECTION_STATUS, connectionStatus=connection_status
        )
        mock_entrypoint_instance.check.return_value = [mock_message]

        # Execute check_connection
        success, message = manifest_runner.check_connection(sample_config)

        # Verify the result
        assert success is False
        assert message == "Invalid API key"

    @patch("airbyte_cdk.manifest_runner.command_processor.processor.AirbyteEntrypoint")
    def test_check_connection_no_status_message(
        self, mock_entrypoint_class, manifest_runner, sample_config
    ):
        """Test check_connection when no connection status message is returned."""
        # Mock the spec method
        manifest_runner._source.spec.return_value = Mock()

        # Mock the entrypoint instance
        mock_entrypoint_instance = Mock()
        mock_entrypoint_class.return_value = mock_entrypoint_instance

        # Return empty messages (no connection status)
        mock_entrypoint_instance.check.return_value = []

        # Execute check_connection
        success, message = manifest_runner.check_connection(sample_config)

        # Verify the result
        assert success is False
        assert message == "Connection check failed"

    @patch("airbyte_cdk.manifest_runner.command_processor.processor.AirbyteEntrypoint")
    def test_check_connection_with_trace_error(
        self, mock_entrypoint_class, manifest_runner, sample_config
    ):
        """Test check_connection raises exception when trace error is present."""

        # Mock the spec method
        manifest_runner._source.spec.return_value = Mock()

        # Mock the entrypoint instance
        mock_entrypoint_instance = Mock()
        mock_entrypoint_class.return_value = mock_entrypoint_instance

        # Create mock trace error message
        trace_message = AirbyteTraceMessage(
            type=TraceType.ERROR, error=Mock(message="Authentication failed"), emitted_at=1234567890
        )
        mock_message = AirbyteMessage(type=AirbyteMessageType.TRACE, trace=trace_message)
        mock_entrypoint_instance.check.return_value = [mock_message]

        # Verify exception is raised
        with pytest.raises(Exception, match="Authentication failed"):
            manifest_runner.check_connection(sample_config)

    @patch("airbyte_cdk.manifest_runner.command_processor.processor.AirbyteEntrypoint")
    def test_discover_success(self, mock_entrypoint_class, manifest_runner, sample_config):
        """Test successful discover execution."""

        # Mock the spec method
        manifest_runner._source.spec.return_value = Mock()

        # Mock the entrypoint instance
        mock_entrypoint_instance = Mock()
        mock_entrypoint_class.return_value = mock_entrypoint_instance

        # Create mock catalog
        catalog = AirbyteCatalog(
            streams=[
                AirbyteStream(
                    name="test_stream",
                    json_schema={"type": "object", "properties": {"id": {"type": "string"}}},
                    supported_sync_modes=["full_refresh"],
                )
            ]
        )
        mock_message = AirbyteMessage(type=AirbyteMessageType.CATALOG, catalog=catalog)
        mock_entrypoint_instance.discover.return_value = [mock_message]

        # Execute discover
        result = manifest_runner.discover(sample_config)

        # Verify the result
        assert result == catalog
        assert len(result.streams) == 1
        assert result.streams[0].name == "test_stream"

        # Verify spec was called
        manifest_runner._source.spec.assert_called_once()

        # Verify entrypoint was created and discover was called
        mock_entrypoint_class.assert_called_once_with(source=manifest_runner._source)
        mock_entrypoint_instance.discover.assert_called_once()

    @patch("airbyte_cdk.manifest_runner.command_processor.processor.AirbyteEntrypoint")
    def test_discover_no_catalog_message(
        self, mock_entrypoint_class, manifest_runner, sample_config
    ):
        """Test discover when no catalog message is returned."""
        # Mock the spec method
        manifest_runner._source.spec.return_value = Mock()

        # Mock the entrypoint instance
        mock_entrypoint_instance = Mock()
        mock_entrypoint_class.return_value = mock_entrypoint_instance

        # Return empty messages (no catalog)
        mock_entrypoint_instance.discover.return_value = []

        # Execute discover
        result = manifest_runner.discover(sample_config)

        # Verify the result is None
        assert result is None

    @patch("airbyte_cdk.manifest_runner.command_processor.processor.AirbyteEntrypoint")
    def test_discover_with_trace_error(self, mock_entrypoint_class, manifest_runner, sample_config):
        """Test discover raises exception when trace error is present."""

        # Mock the spec method
        manifest_runner._source.spec.return_value = Mock()

        # Mock the entrypoint instance
        mock_entrypoint_instance = Mock()
        mock_entrypoint_class.return_value = mock_entrypoint_instance

        # Create mock trace error message
        trace_message = AirbyteTraceMessage(
            type=TraceType.ERROR,
            error=Mock(message="Stream discovery failed"),
            emitted_at=1234567890,
        )
        mock_message = AirbyteMessage(type=AirbyteMessageType.TRACE, trace=trace_message)
        mock_entrypoint_instance.discover.return_value = [mock_message]

        # Verify exception is raised
        with pytest.raises(Exception, match="Stream discovery failed"):
            manifest_runner.discover(sample_config)
