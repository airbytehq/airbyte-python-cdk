from unittest.mock import Mock, patch

import pytest
from airbyte_protocol_dataclasses.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteErrorTraceMessage,
    AirbyteMessage,
    AirbyteStream,
    AirbyteTraceMessage,
    FailureType,
    Status,
    TraceType,
)
from airbyte_protocol_dataclasses.models import Type as AirbyteMessageType
from fastapi import HTTPException

from airbyte_cdk.entrypoint import AirbyteEntrypoint
from airbyte_cdk.manifest_server.command_processor.processor import ManifestCommandProcessor
from airbyte_cdk.models.airbyte_protocol import (
    AirbyteStream as AirbyteStreamProtocol,
)
from airbyte_cdk.models.airbyte_protocol import (
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)


class TestManifestCommandProcessor:
    """Test cases for the ManifestCommandProcessor class."""

    @pytest.fixture
    def mock_source(self):
        """Create a mock ManifestDeclarativeSource."""
        return Mock()

    @pytest.fixture
    def command_processor(self, mock_source):
        """Create a ManifestCommandProcessor instance with mocked source."""
        return ManifestCommandProcessor(mock_source)

    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing."""
        return {"api_key": "test_key", "base_url": "https://api.example.com"}

    @pytest.fixture
    def sample_catalog(self):
        """Sample configured catalog for testing."""
        return ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=AirbyteStreamProtocol(
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

    @patch("airbyte_cdk.manifest_server.command_processor.processor.TestReader")
    def test_test_read_success(
        self, mock_test_reader_class, command_processor, sample_config, sample_catalog
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
        result = command_processor.test_read(
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
            source=command_processor._source,
            config=sample_config,
            configured_catalog=sample_catalog,
            stream_name="test_stream",
            state=state_messages,
            record_limit=record_limit,
        )

        # Verify the result is returned correctly
        assert result == mock_stream_read

    @patch("airbyte_cdk.manifest_server.command_processor.processor.TestReader")
    def test_test_read_exception_handling(
        self,
        mock_test_reader_class,
        command_processor,
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
            command_processor.test_read(
                config=sample_config,
                catalog=sample_catalog,
                state=sample_state,
                record_limit=100,
                page_limit=5,
                slice_limit=10,
            )

    def test_check_connection_success(
        self, command_processor: ManifestCommandProcessor, sample_config
    ):
        """Test successful check_connection execution."""

        # Mock the spec method
        command_processor._source.spec.return_value = Mock()

        # Create mock messages with successful connection status
        connection_status = AirbyteConnectionStatus(
            status=Status.SUCCEEDED, message="Connection test succeeded"
        )
        mock_message = AirbyteMessage(
            type=AirbyteMessageType.CONNECTION_STATUS, connectionStatus=connection_status
        )

        # Mock the entrypoint method
        with patch.object(AirbyteEntrypoint, "check", return_value=[mock_message]):
            # Execute check_connection
            success, message = command_processor.check_connection(sample_config)

        # Verify the result
        assert success is True
        assert message == "Connection test succeeded"

        # Verify spec was called
        command_processor._source.spec.assert_called_once()

    def test_check_connection_failure(self, command_processor, sample_config):
        """Test check_connection with failed status."""

        # Mock the spec method
        command_processor._source.spec.return_value = Mock()

        # Create mock messages with failed connection status
        connection_status = AirbyteConnectionStatus(status=Status.FAILED, message="Invalid API key")
        mock_message = AirbyteMessage(
            type=AirbyteMessageType.CONNECTION_STATUS, connectionStatus=connection_status
        )

        # Mock the entrypoint method
        with patch.object(AirbyteEntrypoint, "check", return_value=[mock_message]):
            # Execute check_connection
            success, message = command_processor.check_connection(sample_config)

        # Verify the result
        assert success is False
        assert message == "Invalid API key"

    def test_check_connection_no_status_message(self, command_processor, sample_config):
        """Test check_connection when no connection status message is returned."""
        # Mock the spec method
        command_processor._source.spec.return_value = Mock()

        # Mock the entrypoint method
        with patch.object(AirbyteEntrypoint, "check", return_value=[]):
            # Execute check_connection
            success, message = command_processor.check_connection(sample_config)

        # Verify the result
        assert success is False
        assert message == "Connection check did not return a status message"

    def test_check_connection_with_trace_error(self, command_processor, sample_config):
        """Test check_connection raises exception when trace error is present."""

        # Mock the spec method
        command_processor._source.spec.return_value = Mock()

        # Create mock trace error message
        error_trace = AirbyteErrorTraceMessage(
            message="Authentication failed", failure_type=FailureType.config_error
        )
        trace_message = AirbyteTraceMessage(
            type=TraceType.ERROR, error=error_trace, emitted_at=1234567890
        )
        mock_message = AirbyteMessage(type=AirbyteMessageType.TRACE, trace=trace_message)

        # Mock the entrypoint method
        with patch.object(AirbyteEntrypoint, "check", return_value=[mock_message]):
            # Verify exception is raised
            with pytest.raises(HTTPException):
                command_processor.check_connection(sample_config)

    def test_discover_success(self, command_processor, sample_config):
        """Test successful discover execution."""

        # Mock the spec method
        command_processor._source.spec.return_value = Mock()

        # Create mock catalog
        catalog = AirbyteCatalog(
            streams=[
                AirbyteStream(
                    name="test_stream",
                    json_schema={"type": "object", "properties": {"id": {"type": "string"}}},
                    supported_sync_modes=[SyncMode.full_refresh],
                )
            ]
        )
        mock_message = AirbyteMessage(type=AirbyteMessageType.CATALOG, catalog=catalog)

        # Mock the entrypoint method
        with patch.object(AirbyteEntrypoint, "discover", return_value=[mock_message]):
            # Execute discover
            result = command_processor.discover(sample_config)

        # Verify the result
        assert result == catalog
        assert len(result.streams) == 1
        assert result.streams[0].name == "test_stream"

        # Verify spec was called
        command_processor._source.spec.assert_called_once()

    def test_discover_no_catalog_message(self, command_processor, sample_config):
        """Test discover when no catalog message is returned."""
        # Mock the spec method
        command_processor._source.spec.return_value = Mock()

        # Mock the entrypoint method
        with patch.object(AirbyteEntrypoint, "discover", return_value=[]):
            # Execute discover
            result = command_processor.discover(sample_config)

        # Verify the result is None
        assert result is None

    def test_discover_with_trace_error(self, command_processor, sample_config):
        """Test discover raises exception when trace error is present."""

        # Mock the spec method
        command_processor._source.spec.return_value = Mock()

        # Create mock trace error message
        error_trace = AirbyteErrorTraceMessage(
            message="Stream discovery failed", failure_type=FailureType.system_error
        )
        trace_message = AirbyteTraceMessage(
            type=TraceType.ERROR,
            error=error_trace,
            emitted_at=1234567890,
        )
        mock_message = AirbyteMessage(type=AirbyteMessageType.TRACE, trace=trace_message)

        # Mock the entrypoint method
        with patch.object(AirbyteEntrypoint, "discover", return_value=[mock_message]):
            # Verify exception is raised
            with pytest.raises(HTTPException):
                command_processor.discover(sample_config)
