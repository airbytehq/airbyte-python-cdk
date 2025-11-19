#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC, abstractmethod
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    Union,
)

from airbyte_cdk.exception_handler import generate_failed_streams_error_message
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStateMessage,
    AirbyteStreamStatus,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    FailureType,
    Status,
    StreamDescriptor,
)
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.message import InMemoryMessageRepository, MessageRepository
from airbyte_cdk.sources.source import Source
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http.http import HttpStream
from airbyte_cdk.sources.utils.record_helper import stream_data_to_airbyte_message
from airbyte_cdk.sources.utils.schema_helpers import InternalConfig, split_config
from airbyte_cdk.sources.utils.slice_logger import DebugSliceLogger, SliceLogger
from airbyte_cdk.utils.event_timing import create_timer
from airbyte_cdk.utils.stream_status_utils import (
    as_airbyte_message as stream_status_as_airbyte_message,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

_default_message_repository = InMemoryMessageRepository()


class AbstractSource(Source, ABC):
    """Base class for Airbyte source connectors that orchestrates stream reading and state management."""

    @abstractmethod
    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Optional[Any]]:
        """Validates that the provided configuration can successfully connect to the data source.

        Args:
            logger: Source logger for diagnostic output.
            config: User-provided configuration containing credentials and connection parameters.

        Returns:
            Tuple of (success boolean, error object). If success is True, connection is valid.
            If False, error object describes what went wrong and will be displayed to the user.
        """

    @abstractmethod
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """Returns the list of streams available in this source connector.

        Args:
            config: User-provided configuration for initializing streams.
        """

    # Stream name to instance map for applying output object transformation
    _stream_to_instance_map: Dict[str, Stream] = {}
    _slice_logger: SliceLogger = DebugSliceLogger()

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        """Discovers available streams and their schemas from the data source."""
        streams = [stream.as_airbyte_stream() for stream in self.streams(config=config)]
        return AirbyteCatalog(streams=streams)

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """Validates connection to the data source using the provided configuration."""
        check_succeeded, error = self.check_connection(logger, config)
        if not check_succeeded:
            return AirbyteConnectionStatus(status=Status.FAILED, message=repr(error))
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[List[AirbyteStateMessage]] = None,
    ) -> Iterator[AirbyteMessage]:
        """Reads records from configured streams and emits them as Airbyte messages."""
        logger.info(f"Starting syncing {self.name}")
        config, internal_config = split_config(config)
        # TODO assert all streams exist in the connector
        # get the streams once in case the connector needs to make any queries to generate them
        stream_instances = {s.name: s for s in self.streams(config)}
        state_manager = ConnectorStateManager(state=state)
        self._stream_to_instance_map = stream_instances

        stream_name_to_exception: MutableMapping[str, AirbyteTracedException] = {}

        with create_timer(self.name) as timer:
            for configured_stream in catalog.streams:
                stream_instance = stream_instances.get(configured_stream.stream.name)
                is_stream_exist = bool(stream_instance)
                try:
                    # Used direct reference to `stream_instance` instead of `is_stream_exist` to avoid mypy type checking errors
                    if not stream_instance:
                        if not self.raise_exception_on_missing_stream:
                            yield stream_status_as_airbyte_message(
                                configured_stream.stream, AirbyteStreamStatus.INCOMPLETE
                            )
                            continue

                        error_message = (
                            f"The stream '{configured_stream.stream.name}' in your connection configuration was not found in the source. "
                            f"Refresh the schema in your replication settings and remove this stream from future sync attempts."
                        )

                        # Use configured_stream as stream_instance to support references in error handling.
                        stream_instance = configured_stream.stream

                        raise AirbyteTracedException(
                            message="A stream listed in your configuration was not found in the source. Please check the logs for more "
                            "details.",
                            internal_message=error_message,
                            failure_type=FailureType.config_error,
                        )

                    timer.start_event(f"Syncing stream {configured_stream.stream.name}")
                    logger.info(f"Marking stream {configured_stream.stream.name} as STARTED")
                    yield stream_status_as_airbyte_message(
                        configured_stream.stream, AirbyteStreamStatus.STARTED
                    )
                    yield from self._read_stream(
                        logger=logger,
                        stream_instance=stream_instance,
                        configured_stream=configured_stream,
                        state_manager=state_manager,
                        internal_config=internal_config,
                    )
                    logger.info(f"Marking stream {configured_stream.stream.name} as STOPPED")
                    yield stream_status_as_airbyte_message(
                        configured_stream.stream, AirbyteStreamStatus.COMPLETE
                    )

                except Exception as e:
                    yield from self._emit_queued_messages()
                    logger.exception(
                        f"Encountered an exception while reading stream {configured_stream.stream.name}"
                    )
                    logger.info(f"Marking stream {configured_stream.stream.name} as STOPPED")
                    yield stream_status_as_airbyte_message(
                        configured_stream.stream, AirbyteStreamStatus.INCOMPLETE
                    )

                    stream_descriptor = StreamDescriptor(name=configured_stream.stream.name)

                    if isinstance(e, AirbyteTracedException):
                        traced_exception = e
                        info_message = f"Stopping sync on error from stream {configured_stream.stream.name} because {self.name} does not support continuing syncs on error."
                    else:
                        traced_exception = self._serialize_exception(
                            stream_descriptor, e, stream_instance=stream_instance
                        )
                        info_message = f"{self.name} does not support continuing syncs on error from stream {configured_stream.stream.name}"

                    yield traced_exception.as_sanitized_airbyte_message(
                        stream_descriptor=stream_descriptor
                    )
                    stream_name_to_exception[stream_instance.name] = traced_exception  # type: ignore # use configured_stream if stream_instance is None
                    if self.stop_sync_on_stream_failure:
                        logger.info(info_message)
                        break
                finally:
                    # Finish read event only if the stream instance exists;
                    # otherwise, there's no need as it never started
                    if is_stream_exist:
                        timer.finish_event()
                        logger.info(f"Finished syncing {configured_stream.stream.name}")
                        logger.info(timer.report())

        if len(stream_name_to_exception) > 0:
            error_message = generate_failed_streams_error_message(
                {key: [value] for key, value in stream_name_to_exception.items()}
            )
            logger.info(error_message)
            # We still raise at least one exception when a stream raises an exception because the platform currently relies
            # on a non-zero exit code to determine if a sync attempt has failed. We also raise the exception as a config_error
            # type because this combined error isn't actionable, but rather the previously emitted individual errors.
            raise AirbyteTracedException(
                message=error_message, failure_type=FailureType.config_error
            )
        logger.info(f"Finished syncing {self.name}")

    @staticmethod
    def _serialize_exception(
        stream_descriptor: StreamDescriptor, e: Exception, stream_instance: Optional[Stream] = None
    ) -> AirbyteTracedException:
        """Converts an exception into an AirbyteTracedException with optional stream-specific error message."""
        display_message = stream_instance.get_error_display_message(e) if stream_instance else None
        if display_message:
            return AirbyteTracedException.from_exception(
                e, message=display_message, stream_descriptor=stream_descriptor
            )
        return AirbyteTracedException.from_exception(e, stream_descriptor=stream_descriptor)

    @property
    def raise_exception_on_missing_stream(self) -> bool:
        """Controls whether to raise an exception when a configured stream is not found in the source."""
        return False

    def _read_stream(
        self,
        logger: logging.Logger,
        stream_instance: Stream,
        configured_stream: ConfiguredAirbyteStream,
        state_manager: ConnectorStateManager,
        internal_config: InternalConfig,
    ) -> Iterator[AirbyteMessage]:
        """Reads records from a single stream and emits them as Airbyte messages."""
        if internal_config.page_size and isinstance(stream_instance, HttpStream):
            logger.info(
                f"Setting page size for {stream_instance.name} to {internal_config.page_size}"
            )
            stream_instance.page_size = internal_config.page_size
        logger.debug(
            f"Syncing configured stream: {configured_stream.stream.name}",
            extra={
                "sync_mode": configured_stream.sync_mode,
                "primary_key": configured_stream.primary_key,
                "cursor_field": configured_stream.cursor_field,
            },
        )
        stream_instance.log_stream_sync_configuration()

        stream_name = configured_stream.stream.name
        stream_state = state_manager.get_stream_state(stream_name, stream_instance.namespace)

        # This is a hack. Existing full refresh streams that are converted into resumable full refresh need to discard
        # the state because the terminal state for a full refresh sync is not compatible with substream resumable full
        # refresh state. This is only required when running live traffic regression testing since the platform normally
        # handles whether to pass state
        if stream_state == {"__ab_no_cursor_state_message": True}:
            stream_state = {}

        if "state" in dir(stream_instance):
            stream_instance.state = stream_state  # type: ignore # we check that state in the dir(stream_instance)
            logger.info(f"Setting state of {self.name} stream to {stream_state}")

        record_iterator = stream_instance.read(
            configured_stream,
            logger,
            self._slice_logger,
            stream_state,
            state_manager,
            internal_config,
        )

        record_counter = 0
        logger.info(f"Syncing stream: {stream_name} ")
        for record_data_or_message in record_iterator:
            record = self._get_message(record_data_or_message, stream_instance)
            if record.type == MessageType.RECORD:
                record_counter += 1
                if record_counter == 1:
                    logger.info(f"Marking stream {stream_name} as RUNNING")
                    # If we just read the first record of the stream, emit the transition to the RUNNING state
                    yield stream_status_as_airbyte_message(
                        configured_stream.stream, AirbyteStreamStatus.RUNNING
                    )
            yield from self._emit_queued_messages()
            yield record

        logger.info(f"Read {record_counter} records from {stream_name} stream")

    def _emit_queued_messages(self) -> Iterable[AirbyteMessage]:
        """Emits any messages that have been queued in the message repository."""
        if self.message_repository:
            yield from self.message_repository.consume_queue()
        return

    def _get_message(
        self, record_data_or_message: Union[StreamData, AirbyteMessage], stream: Stream
    ) -> AirbyteMessage:
        """Converts StreamData to AirbyteMessage or returns the input if already an AirbyteMessage."""
        match record_data_or_message:
            case AirbyteMessage():
                return record_data_or_message
            case _:
                return stream_data_to_airbyte_message(
                    stream.name,
                    record_data_or_message,
                    stream.transformer,
                    stream.get_json_schema(),
                )

    @property
    def message_repository(self) -> Union[None, MessageRepository]:
        """Returns the message repository used for queuing messages during sync operations."""
        return _default_message_repository

    @property
    def stop_sync_on_stream_failure(self) -> bool:
        """Controls whether to stop the entire sync when a single stream fails.

        WARNING: This function is in-development which means it is subject to change. Use at your own risk.

        By default, when a source encounters an exception while syncing a stream, it will emit an error trace message and then
        continue syncing the next stream. This can be overwritten on a per-source basis so that the source will stop the sync
        on the first error seen and emit a single error trace message for that stream.
        """
        return False
