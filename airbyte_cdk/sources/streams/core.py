#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import copy
import inspect
import itertools
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property, lru_cache
from typing import Any, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional, Union

from typing_extensions import deprecated

import airbyte_cdk.sources.utils.casing as casing
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStream,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.streams.checkpoint import (
    CheckpointMode,
    CheckpointReader,
    Cursor,
    CursorBasedCheckpointReader,
    FullRefreshCheckpointReader,
    IncrementalCheckpointReader,
    LegacyCursorBasedCheckpointReader,
    ResumableFullRefreshCheckpointReader,
)
from airbyte_cdk.sources.types import StreamSlice

# list of all possible HTTP methods which can be used for sending of request bodies
from airbyte_cdk.sources.utils.schema_helpers import InternalConfig, ResourceSchemaLoader
from airbyte_cdk.sources.utils.slice_logger import DebugSliceLogger, SliceLogger
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer

# A stream's read method can return one of the following types:
# Mapping[str, Any]: The content of an AirbyteRecordMessage
# AirbyteMessage: An AirbyteMessage. Could be of any type
StreamData = Union[Mapping[str, Any], AirbyteMessage]

JsonSchema = Mapping[str, Any]

NO_CURSOR_STATE_KEY = "__ab_no_cursor_state_message"


def package_name_from_class(cls: object) -> str:
    """Get the package name for a given class.

    Args:
        cls: The class to get the package name for.

    Returns:
        The package name as a string.

    Raises:
        ValueError: If package name cannot be determined.
    """
    module = inspect.getmodule(cls)
    if module is not None:
        return module.__name__.split(".")[0]
    else:
        raise ValueError(f"Could not find package name for class {cls}")


class CheckpointMixin(ABC):
    """Mixin for implementing stream state checkpointing.

    Provides interface for reading and writing internal state used to
    track sync progress. Streams using this mixin should implement
    state getter/setter methods.

    Example:
        class CheckpointedStream(Stream, CheckpointMixin):
            @property
            def state(self):
                return self._state

            @state.setter
            def state(self, value):
                self._state[self.cursor_field] = value[self.cursor_field]
    """

    @property
    @abstractmethod
    def state(self) -> MutableMapping[str, Any]:
        """State getter, should return state in form that can serialized to a string and send to the output
        as a STATE AirbyteMessage.

        A good example of a state is a cursor_value:
            {
                self.cursor_field: "cursor_value"
            }

         State should try to be as small as possible but at the same time descriptive enough to restore
         syncing process from the point where it stopped.
        """

    @state.setter
    @abstractmethod
    def state(self, value: MutableMapping[str, Any]) -> None:
        """State setter, accept state serialized by state getter."""


@deprecated(
    "Deprecated as of CDK version 0.87.0. "
    "Deprecated in favor of the `CheckpointMixin` which offers similar functionality."
)
class IncrementalMixin(CheckpointMixin, ABC):
    """Mixin for implementing incremental sync capability.

    Deprecated: Use CheckpointMixin instead as of CDK version 0.87.0.

    Example:
        class IncrementalStream(Stream, IncrementalMixin):
            @property
            def state(self):
                return self._state

            @state.setter
            def state(self, value):
                self._state[self.cursor_field] = value[self.cursor_field]
    """


@dataclass
class StreamClassification:
    is_legacy_format: bool
    has_multiple_slices: bool


class Stream(ABC):
    """
    Base abstract class for an Airbyte Stream. Makes no assumption of the Stream's underlying transport protocol.
    """

    _configured_json_schema: Optional[Dict[str, Any]] = None
    _exit_on_rate_limit: bool = False

    # Use self.logger in subclasses to log any messages
    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f"airbyte.streams.{self.name}")

    # TypeTransformer object to perform output data transformation
    transformer: TypeTransformer = TypeTransformer(TransformConfig.NoTransform)

    cursor: Optional[Cursor] = None

    has_multiple_slices = False

    @cached_property
    def name(self) -> str:
        """Get the name of this stream.

        Returns:
            Stream name, defaulting to the implementing class name in snake_case.
            Can be overridden as needed.
        """
        return casing.camel_to_snake(self.__class__.__name__)

    def get_error_display_message(self, exception: BaseException) -> Optional[str]:
        """Get a user-friendly error message for an exception.

        Called when encountering an exception while reading records from the stream.
        Used to build the AirbyteTraceMessage for error reporting.

        The default implementation returns None for all exceptions.
        Override to provide custom error messages.

        Args:
            exception: The exception that was raised.

        Returns:
            A user-friendly message describing the error, or None.
        """
        return None

    def read(  # type: ignore  # ignoring typing for ConnectorStateManager because of circular dependencies
        self,
        configured_stream: ConfiguredAirbyteStream,
        logger: logging.Logger,
        slice_logger: SliceLogger,
        stream_state: MutableMapping[str, Any],
        state_manager,
        internal_config: InternalConfig,
    ) -> Iterable[StreamData]:
        """Read records from the stream with state management and checkpointing.

        Handles stream state management, record checkpointing, and slice logging
        while reading records from the stream.

        Args:
            configured_stream: Stream configuration including sync mode and schema.
            logger: Logger instance for the stream.
            slice_logger: Logger for stream slice messages.
            stream_state: Current state of the stream.
            state_manager: Manager for handling connector state.
            internal_config: Internal configuration options.

        Returns:
            An iterable of records or Airbyte messages from the stream.
        """
        sync_mode = configured_stream.sync_mode
        cursor_field = configured_stream.cursor_field
        self.configured_json_schema = configured_stream.stream.json_schema

        # WARNING: When performing a read() that uses incoming stream state, we MUST use the self.state that is defined as
        # opposed to the incoming stream_state value. Because some connectors like ones using the file-based CDK modify
        # state before setting the value on the Stream attribute, the most up-to-date state is derived from Stream.state
        # instead of the stream_state parameter. This does not apply to legacy connectors using get_updated_state().
        try:
            stream_state = self.state  # type: ignore # we know the field might not exist...
        except AttributeError:
            pass

        should_checkpoint = bool(state_manager)
        checkpoint_reader = self._get_checkpoint_reader(
            logger=logger, cursor_field=cursor_field, sync_mode=sync_mode, stream_state=stream_state
        )

        next_slice = checkpoint_reader.next()
        record_counter = 0
        stream_state_tracker = copy.deepcopy(stream_state)
        while next_slice is not None:
            if slice_logger.should_log_slice_message(logger):
                yield slice_logger.create_slice_log_message(next_slice)
            records = self.read_records(
                sync_mode=sync_mode,  # todo: change this interface to no longer rely on sync_mode for behavior
                stream_slice=next_slice,
                stream_state=stream_state,
                cursor_field=cursor_field or None,
            )
            for record_data_or_message in records:
                yield record_data_or_message
                if isinstance(record_data_or_message, Mapping) or (
                    hasattr(record_data_or_message, "type")
                    and record_data_or_message.type == MessageType.RECORD
                ):
                    record_data = (
                        record_data_or_message
                        if isinstance(record_data_or_message, Mapping)
                        else record_data_or_message.record
                    )

                    # Thanks I hate it. RFR fundamentally doesn't fit with the concept of the legacy Stream.get_updated_state()
                    # method because RFR streams rely on pagination as a cursor. Stream.get_updated_state() was designed to make
                    # the CDK manage state using specifically the last seen record. don't @ brian.lai
                    #
                    # Also, because the legacy incremental state case decouples observing incoming records from emitting state, it
                    # requires that we separate CheckpointReader.observe() and CheckpointReader.get_checkpoint() which could
                    # otherwise be combined.
                    if self.cursor_field:
                        # Some connectors have streams that implement get_updated_state(), but do not define a cursor_field. This
                        # should be fixed on the stream implementation, but we should also protect against this in the CDK as well
                        stream_state_tracker = self.get_updated_state(
                            stream_state_tracker,
                            record_data,  # type: ignore [arg-type]
                        )
                        self._observe_state(checkpoint_reader, stream_state_tracker)
                    record_counter += 1

                    checkpoint_interval = self.state_checkpoint_interval
                    checkpoint = checkpoint_reader.get_checkpoint()
                    if (
                        should_checkpoint
                        and checkpoint_interval
                        and record_counter % checkpoint_interval == 0
                        and checkpoint is not None
                    ):
                        airbyte_state_message = self._checkpoint_state(
                            checkpoint, state_manager=state_manager
                        )
                        yield airbyte_state_message

                    if internal_config.is_limit_reached(record_counter):
                        break
            self._observe_state(checkpoint_reader)
            checkpoint_state = checkpoint_reader.get_checkpoint()
            if should_checkpoint and checkpoint_state is not None:
                airbyte_state_message = self._checkpoint_state(
                    checkpoint_state, state_manager=state_manager
                )
                yield airbyte_state_message

            next_slice = checkpoint_reader.next()

        checkpoint = checkpoint_reader.get_checkpoint()
        if should_checkpoint and checkpoint is not None:
            airbyte_state_message = self._checkpoint_state(checkpoint, state_manager=state_manager)
            yield airbyte_state_message

    def read_only_records(self, state: Optional[Mapping[str, Any]] = None) -> Iterable[StreamData]:
        """Read records from the stream without updating state.

        Helper method that performs a read operation and emits records without
        updating the stream's internal state or emitting state messages, even
        if the stream supports incremental syncs.

        Args:
            state: Optional state to use for the read operation.

        Returns:
            An iterable of records from the stream.
        """

        configured_stream = ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name=self.name,
                json_schema={},
                supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
            ),
            sync_mode=SyncMode.incremental if state else SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.append,
        )

        yield from self.read(
            configured_stream=configured_stream,
            logger=self.logger,
            slice_logger=DebugSliceLogger(),
            stream_state=dict(state)
            if state
            else {},  # read() expects MutableMapping instead of Mapping which is used more often
            state_manager=None,
            internal_config=InternalConfig(),  # type: ignore [call-arg]
        )

    @abstractmethod
    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        """Read records from the stream based on the inputs.

        Args:
            sync_mode: The sync mode to use.
            cursor_field: Field to use as the stream cursor.
            stream_slice: A slice of the stream to read.
            stream_state: The current state of the stream.

        Returns:
            An iterable of records from the stream.
        """

    @lru_cache(maxsize=None)
    def get_json_schema(self) -> Mapping[str, Any]:
        """Get the JSON schema for this stream.

        The default implementation looks for a JSONSchema file matching the stream's name.
        Override as needed.

        Returns:
            A dict containing the JSON schema for this stream.
        """
        # TODO show an example of using pydantic to define the JSON schema, or reading an OpenAPI spec
        return ResourceSchemaLoader(package_name_from_class(self.__class__)).get_schema(self.name)

    def as_airbyte_stream(self) -> AirbyteStream:
        """Convert this stream to an AirbyteStream object.

        Creates an AirbyteStream instance with this stream's configuration,
        including name, schema, sync modes, and other metadata.

        Returns:
            An AirbyteStream object representing this stream's configuration.
        """
        stream = AirbyteStream(
            name=self.name,
            json_schema=dict(self.get_json_schema()),
            supported_sync_modes=[SyncMode.full_refresh],
            is_resumable=self.is_resumable,
        )

        if self.namespace:
            stream.namespace = self.namespace

        # If we can offer incremental we always should. RFR is always less reliable than incremental which uses a real cursor value
        if self.supports_incremental:
            stream.source_defined_cursor = self.source_defined_cursor
            stream.supported_sync_modes.append(SyncMode.incremental)
            stream.default_cursor_field = self._wrapped_cursor_field()

        keys = Stream._wrapped_primary_key(self.primary_key)
        if keys and len(keys) > 0:
            stream.source_defined_primary_key = keys

        return stream

    @property
    def supports_incremental(self) -> bool:
        """Check if this stream supports incremental syncs.

        Returns:
            True if the stream supports incremental reading of data.
        """
        return len(self._wrapped_cursor_field()) > 0

    @property
    def is_resumable(self) -> bool:
        """Check if this stream supports resuming from checkpoints.

        Differs from supports_incremental because some streams like resumable
        full refresh can checkpoint progress between attempts for fault tolerance,
        but will start from the beginning on the next sync job.

        Returns:
            True if stream allows checkpointing sync progress and can resume,
            False otherwise.
        """
        if self.supports_incremental:
            return True
        if self.has_multiple_slices:
            # We temporarily gate substream to not support RFR because puts a pretty high burden on connector developers
            # to structure stream state in a very specific way. We also can't check for issubclass(HttpSubStream) because
            # not all substreams implement the interface and it would be a circular dependency so we use parent as a surrogate
            return False
        elif hasattr(type(self), "state") and getattr(type(self), "state").fset is not None:
            # Modern case where a stream manages state using getter/setter
            return True
        else:
            # Legacy case where the CDK manages state via the get_updated_state() method. This is determined by checking if
            # the stream's get_updated_state() differs from the Stream class and therefore has been overridden
            return type(self).get_updated_state != Stream.get_updated_state

    def _wrapped_cursor_field(self) -> List[str]:
        """Wrap cursor field in a consistent list format.

        Converts string cursor fields to single-item lists for consistency.
        Leaves list cursor fields unchanged.

        Returns:
            List containing the cursor field(s).
        """
        return [self.cursor_field] if isinstance(self.cursor_field, str) else self.cursor_field

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        """Get the default cursor field for this stream.

        Override to specify the field used for incremental syncing.
        For example, an API entity might use 'created_at' as its cursor.

        Returns:
            Field name or path to the cursor field. For nested cursors,
            return a list representing the path to the cursor.
        """
        return []

    @property
    def namespace(self) -> Optional[str]:
        """Get the namespace for this stream.

        Override to specify a namespace, such as a Postgres schema,
        that this stream will emit records for.

        Returns:
            The namespace name as a string, or None if no namespace.
        """
        return None

    @property
    def source_defined_cursor(self) -> bool:
        """Check if the cursor field is defined by the source.

        Returns:
            True if cursor field is defined by the source,
            False if it can be configured by the user.
        """
        return True

    @property
    def exit_on_rate_limit(self) -> bool:
        """Determine behavior when rate limit is encountered.

        Returns:
            True to exit gracefully on rate limit,
            False to retry endlessly when rate limited.
        """
        return self._exit_on_rate_limit

    @exit_on_rate_limit.setter
    def exit_on_rate_limit(self, value: bool) -> None:
        """Set whether to exit on rate limit.

        Args:
            value: True to exit on rate limit, False to retry.
        """
        self._exit_on_rate_limit = value

    @property
    @abstractmethod
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """Get the primary key(s) for this stream.

        Returns:
            A string for single primary key, list of strings for composite key,
            list of list of strings for nested composite key, or None if no primary key.
        """

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """Define the slices for this stream.

        Override to implement stream slicing logic. See documentation for details
        on stream slicing.

        Args:
            sync_mode: The sync mode being used.
            cursor_field: Field used for stream state/incremental syncs.
            stream_state: The current state of the stream.

        Returns:
            An iterable of stream slices.
        """
        yield StreamSlice(partition={}, cursor_slice={})

    @property
    def state_checkpoint_interval(self) -> Optional[int]:
        """Define how frequently to checkpoint stream state.

        Controls how often to emit STATE messages. For example, if this returns 100,
        state is persisted after reading 100 records, then 200, 300, etc.
        A typical value is 1000, but may vary by data source.

        Checkpointing helps resume syncs after failures or cancellations.

        Returns:
            Number of records between checkpoints, or None if checkpointing should
            be disabled (e.g. when records are not ordered by cursor field).
        """
        return None

    # Commented-out to avoid any runtime penalty, since this is used in a hot per-record codepath.
    # To be evaluated for re-introduction here: https://github.com/airbytehq/airbyte-python-cdk/issues/116
    # @deprecated(
    #     "Deprecated method `get_updated_state` as of CDK version 0.1.49. "
    #     "Please use explicit state property instead, see `IncrementalMixin` docs."
    # )
    def get_updated_state(
        self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]
    ) -> MutableMapping[str, Any]:
        """Extract updated state from the latest record.

        DEPRECATED: Use explicit state property instead, see `IncrementalMixin` docs.

        Updates stream state based on the latest record. Used for implementing
        incremental sync by comparing current state with latest record values.

        Args:
            current_stream_state: The stream's current state object.
            latest_record: The latest record extracted from the stream.

        Returns:
            Updated state object based on latest record.

        Example:
            If state tracks created_at timestamp:
            - Current state: {'created_at': 10}
            - Latest record: {'name': 'octavia', 'created_at': 20}
            - Returns: {'created_at': 20}
        """
        return {}

    def get_cursor(self) -> Optional[Cursor]:
        """Get the cursor for managing stream state.

        A Cursor manages how stream state is read and updated while reading records.
        Streams should define a cursor implementation and override this method to
        manage state through a Cursor interface.

        Returns:
            A Cursor instance for managing stream state, or None if not needed.
        """
        return self.cursor

    def _get_checkpoint_reader(
        self,
        logger: logging.Logger,
        cursor_field: Optional[List[str]],
        sync_mode: SyncMode,
        stream_state: MutableMapping[str, Any],
    ) -> CheckpointReader:
        """Get appropriate CheckpointReader for stream's sync mode and state management.

        Determines whether to use legacy state management via get_updated_state()
        or modern state management via state getter/setter.

        Args:
            logger: Logger instance for the stream.
            cursor_field: Field used for stream state/incremental syncs.
            sync_mode: The sync mode being used.
            stream_state: Current state of the stream.

        Returns:
            CheckpointReader instance configured for the stream.
        """
        mappings_or_slices = self.stream_slices(
            cursor_field=cursor_field,
            sync_mode=sync_mode,  # todo: change this interface to no longer rely on sync_mode for behavior
            stream_state=stream_state,
        )

        # Because of poor foresight, we wrote the default Stream.stream_slices() method to return [None] which is confusing and
        # has now normalized this behavior for connector developers. Now some connectors return [None]. This is objectively
        # misleading and a more ideal interface is [{}] to indicate we still want to iterate over one slice, but with no
        # specific slice values. None is bad, and now I feel bad that I have to write this hack.
        if mappings_or_slices == [None]:
            mappings_or_slices = [{}]

        slices_iterable_copy, iterable_for_detecting_format = itertools.tee(mappings_or_slices, 2)
        stream_classification = self._classify_stream(
            mappings_or_slices=iterable_for_detecting_format
        )

        # Streams that override has_multiple_slices are explicitly indicating that they will iterate over
        # multiple partitions. Inspecting slices to automatically apply the correct cursor is only needed as
        # a backup. So if this value was already assigned to True by the stream, we don't need to reassign it
        self.has_multiple_slices = (
            self.has_multiple_slices or stream_classification.has_multiple_slices
        )

        cursor = self.get_cursor()
        if cursor:
            cursor.set_initial_state(stream_state=stream_state)

        checkpoint_mode = self._checkpoint_mode

        if cursor and stream_classification.is_legacy_format:
            return LegacyCursorBasedCheckpointReader(
                stream_slices=slices_iterable_copy, cursor=cursor, read_state_from_cursor=True
            )
        elif cursor:
            return CursorBasedCheckpointReader(
                stream_slices=slices_iterable_copy,
                cursor=cursor,
                read_state_from_cursor=checkpoint_mode == CheckpointMode.RESUMABLE_FULL_REFRESH,
            )
        elif checkpoint_mode == CheckpointMode.RESUMABLE_FULL_REFRESH:
            # Resumable full refresh readers rely on the stream state dynamically being updated during pagination and does
            # not iterate over a static set of slices.
            return ResumableFullRefreshCheckpointReader(stream_state=stream_state)
        elif checkpoint_mode == CheckpointMode.INCREMENTAL:
            return IncrementalCheckpointReader(
                stream_slices=slices_iterable_copy, stream_state=stream_state
            )
        else:
            return FullRefreshCheckpointReader(stream_slices=slices_iterable_copy)

    @property
    def _checkpoint_mode(self) -> CheckpointMode:
        """Get the appropriate checkpoint mode for this stream.

        Returns:
            INCREMENTAL if stream is resumable with cursor field,
            RESUMABLE_FULL_REFRESH if stream is resumable without cursor,
            FULL_REFRESH otherwise.
        """
        if self.is_resumable and len(self._wrapped_cursor_field()) > 0:
            return CheckpointMode.INCREMENTAL
        elif self.is_resumable:
            return CheckpointMode.RESUMABLE_FULL_REFRESH
        else:
            return CheckpointMode.FULL_REFRESH

    @staticmethod
    def _classify_stream(
        mappings_or_slices: Iterator[Optional[Union[Mapping[str, Any], StreamSlice]]],
    ) -> StreamClassification:
        """Classify a stream based on its slice format and structure.

        Detects stream attributes by analyzing its slice implementation:
        - is_substream: Identifies substreams with parent dependencies
        - uses_legacy_slice_format: Determines if stream uses legacy mapping format
          instead of structured StreamSlice objects

        This method is needed because Python streams lack consistent implementation
        patterns. It helps manage backwards compatibility and incremental feature
        releases.

        Args:
            mappings_or_slices: Iterator of stream slices to analyze.

        Returns:
            StreamClassification with detected stream attributes.

        Note:
            This method can be deprecated once all streams use StreamSlice objects
            and substream implementation is standardized.
        """
        if not mappings_or_slices:
            raise ValueError("A stream should always have at least one slice")
        try:
            next_slice = next(mappings_or_slices)
            if isinstance(next_slice, StreamSlice) and next_slice == StreamSlice(
                partition={}, cursor_slice={}
            ):
                is_legacy_format = False
                slice_has_value = False
            elif next_slice == {}:
                is_legacy_format = True
                slice_has_value = False
            elif isinstance(next_slice, StreamSlice):
                is_legacy_format = False
                slice_has_value = True
            else:
                is_legacy_format = True
                slice_has_value = True
        except StopIteration:
            # If the stream has no slices, the format ultimately does not matter since no data will get synced. This is technically
            # a valid case because it is up to the stream to define its slicing behavior
            return StreamClassification(is_legacy_format=False, has_multiple_slices=False)

        if slice_has_value:
            # If the first slice contained a partition value from the result of stream_slices(), this is a substream that might
            # have multiple parent records to iterate over
            return StreamClassification(
                is_legacy_format=is_legacy_format, has_multiple_slices=slice_has_value
            )

        try:
            # If stream_slices() returns multiple slices, this is also a substream that can potentially generate empty slices
            next(mappings_or_slices)
            return StreamClassification(is_legacy_format=is_legacy_format, has_multiple_slices=True)
        except StopIteration:
            # If the result of stream_slices() only returns a single empty stream slice, then we know this is a regular stream
            return StreamClassification(
                is_legacy_format=is_legacy_format, has_multiple_slices=False
            )

    def log_stream_sync_configuration(self) -> None:
        """Log the stream's sync configuration.

        Outputs debug-level logs containing the stream's name,
        primary key, and cursor field configuration.
        """
        self.logger.debug(
            f"Syncing stream instance: {self.name}",
            extra={
                "primary_key": self.primary_key,
                "cursor_field": self.cursor_field,
            },
        )

    @staticmethod
    def _wrapped_primary_key(
        keys: Optional[Union[str, List[str], List[List[str]]]],
    ) -> Optional[List[List[str]]]:
        """Wrap primary key in the format required by Airbyte Stream objects.

        Converts various primary key formats into a list of list of strings.
        For example:
        - "id" -> [["id"]]
        - ["id", "name"] -> [["id"], ["name"]]
        - [["id", "name"]] -> [["id", "name"]]

        Args:
            keys: Primary key(s) in string or list format.

        Returns:
            Primary key wrapped as list of list of strings, or None if no keys.

        Raises:
            ValueError: If keys are not in string or list format.
        """
        if not keys:
            return None

        if isinstance(keys, str):
            return [[keys]]
        elif isinstance(keys, list):
            wrapped_keys = []
            for component in keys:
                if isinstance(component, str):
                    wrapped_keys.append([component])
                elif isinstance(component, list):
                    wrapped_keys.append(component)
                else:
                    raise ValueError(f"Element must be either list or str. Got: {type(component)}")
            return wrapped_keys
        else:
            raise ValueError(f"Element must be either list or str. Got: {type(keys)}")

    def _observe_state(
        self, checkpoint_reader: CheckpointReader, stream_state: Optional[Mapping[str, Any]] = None
    ) -> None:
        """Read stream state using recommended state management or fallback method.

        Attempts to read state using the recommended state setter/getter pattern.
        Falls back to legacy Stream.get_updated_state() if AttributeError occurs.

        Args:
            checkpoint_reader: Reader for managing stream checkpoints.
            stream_state: Optional current state of the stream.
        """

        # This is an inversion of the original logic that used to try state getter/setters first. As part of the work to
        # automatically apply resumable full refresh to all streams, all HttpStream classes implement default state
        # getter/setter methods, we should default to only using the incoming stream_state parameter value is {} which
        # indicates the stream does not override the default get_updated_state() implementation. When the default method
        # is not overridden, then the stream defers to self.state getter
        if stream_state:
            checkpoint_reader.observe(stream_state)
        elif type(self).get_updated_state == Stream.get_updated_state:
            # We only default to the state getter/setter if the stream does not use the legacy get_updated_state() method
            try:
                new_state = self.state  # type: ignore # This will always exist on HttpStreams, but may not for Stream
                if new_state:
                    checkpoint_reader.observe(new_state)
            except AttributeError:
                pass

    def _checkpoint_state(  # type: ignore  # ignoring typing for ConnectorStateManager because of circular dependencies
        self,
        stream_state: Mapping[str, Any],
        state_manager,
    ) -> AirbyteMessage:
        """Create a state checkpoint message for the stream.

        Updates the state manager with the current stream state and creates
        a state message for checkpointing.

        Args:
            stream_state: Current state of the stream.
            state_manager: Manager for handling connector state.

        Returns:
            AirbyteMessage containing the state checkpoint.
        """
        # todo: This can be consolidated into one ConnectorStateManager.update_and_create_state_message() method, but I want
        #  to reduce changes right now and this would span concurrent as well
        state_manager.update_state_for_stream(self.name, self.namespace, stream_state)
        return state_manager.create_state_message(self.name, self.namespace)  # type: ignore [no-any-return]

    @property
    def configured_json_schema(self) -> Optional[Dict[str, Any]]:
        """Get the configured JSON schema for this stream.

        This property is set during the read method execution and contains
        the schema from the configured catalog, filtered to remove any
        invalid or deprecated properties.

        Returns:
            JSON schema from configured catalog if provided, otherwise None.
        """
        return self._configured_json_schema

    @configured_json_schema.setter
    def configured_json_schema(self, json_schema: Dict[str, Any]) -> None:
        """Set the configured JSON schema for this stream.

        Filters out any invalid properties before storing the schema.

        Args:
            json_schema: The JSON schema to configure for this stream.
        """
        self._configured_json_schema = self._filter_schema_invalid_properties(json_schema)

    def _filter_schema_invalid_properties(
        self, configured_catalog_json_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Filter out invalid properties from configured JSON schema.

        Removes properties that are not present in the current stream schema.
        This cleanup is needed because configured schemas may contain
        deprecated fields from older versions.

        Args:
            configured_catalog_json_schema: The JSON schema from configured catalog.

        Returns:
            Filtered JSON schema containing only valid properties.

        Note:
            Logs a warning if any properties are removed due to being deprecated.
        """
        configured_schema: Any = configured_catalog_json_schema.get("properties", {})
        stream_schema_properties: Any = self.get_json_schema().get("properties", {})

        configured_keys = configured_schema.keys()
        stream_keys = stream_schema_properties.keys()
        invalid_properties = configured_keys - stream_keys
        if not invalid_properties:
            return configured_catalog_json_schema

        self.logger.warning(
            f"Stream {self.name}: the following fields are deprecated and cannot be synced. {invalid_properties}. Refresh the connection's source schema to resolve this warning."
        )

        valid_configured_schema_properties_keys = stream_keys & configured_keys
        valid_configured_schema_properties = {}

        for configured_schema_property in valid_configured_schema_properties_keys:
            valid_configured_schema_properties[configured_schema_property] = (
                stream_schema_properties[configured_schema_property]
            )

        return {**configured_catalog_json_schema, "properties": valid_configured_schema_properties}
