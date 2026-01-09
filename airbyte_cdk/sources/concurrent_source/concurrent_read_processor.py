#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging
import os
from typing import Dict, Iterable, List, Optional, Set

from airbyte_cdk.exception_handler import generate_failed_streams_error_message
from airbyte_cdk.models import AirbyteMessage, AirbyteStreamStatus, FailureType, StreamDescriptor
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.concurrent_source.partition_generation_completed_sentinel import (
    PartitionGenerationCompletedSentinel,
)
from airbyte_cdk.sources.concurrent_source.stream_thread_exception import StreamThreadException
from airbyte_cdk.sources.concurrent_source.thread_pool_manager import ThreadPoolManager
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.concurrent.abstract_stream import AbstractStream
from airbyte_cdk.sources.streams.concurrent.partition_enqueuer import PartitionEnqueuer
from airbyte_cdk.sources.streams.concurrent.partition_reader import PartitionReader
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition
from airbyte_cdk.sources.streams.concurrent.partitions.types import PartitionCompleteSentinel
from airbyte_cdk.sources.types import Record
from airbyte_cdk.sources.utils.record_helper import stream_data_to_airbyte_message
from airbyte_cdk.sources.utils.slice_logger import SliceLogger
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_cdk.utils.stream_status_utils import (
    as_airbyte_message as stream_status_as_airbyte_message,
)


class ConcurrentReadProcessor:
    def __init__(
        self,
        stream_instances_to_read_from: List[AbstractStream],
        partition_enqueuer: PartitionEnqueuer,
        thread_pool_manager: ThreadPoolManager,
        logger: logging.Logger,
        slice_logger: SliceLogger,
        message_repository: MessageRepository,
        partition_reader: PartitionReader,
    ):
        """
        This class is responsible for handling items from a concurrent stream read process.
        :param stream_instances_to_read_from: List of streams to read from
        :param partition_enqueuer: PartitionEnqueuer instance
        :param thread_pool_manager: ThreadPoolManager instance
        :param logger: Logger instance
        :param slice_logger: SliceLogger instance
        :param message_repository: MessageRepository instance
        :param partition_reader: PartitionReader instance
        """
        self._stream_name_to_instance = {s.name: s for s in stream_instances_to_read_from}
        self._record_counter = {}
        self._streams_to_running_partitions: Dict[str, Set[Partition]] = {}
        for stream in stream_instances_to_read_from:
            self._streams_to_running_partitions[stream.name] = set()
            self._record_counter[stream.name] = 0
        self._thread_pool_manager = thread_pool_manager
        self._partition_enqueuer = partition_enqueuer
        self._stream_instances_to_start_partition_generation = stream_instances_to_read_from
        self._streams_currently_generating_partitions: List[str] = []
        self._logger = logger
        self._slice_logger = slice_logger
        self._message_repository = message_repository
        self._partition_reader = partition_reader
        self._streams_done: Set[str] = set()
        self._exceptions_per_stream_name: dict[str, List[Exception]] = {}

        # Track which streams (by name) are currently active
        # A stream is "active" if it's generating partitions or has partitions being read
        self._active_stream_names: Set[str] = set()

        # Store blocking group names for streams that require blocking simultaneous reads
        # Maps stream name -> group name (empty string means no blocking)
        self._stream_block_simultaneous_read: Dict[str, str] = {
            stream.name: stream.block_simultaneous_read for stream in stream_instances_to_read_from
        }

        # Track which groups are currently active
        # Maps group name -> set of stream names in that group
        self._active_groups: Dict[str, Set[str]] = {}

        for stream in stream_instances_to_read_from:
            if stream.block_simultaneous_read:
                self._logger.info(
                    f"Stream '{stream.name}' is in blocking group '{stream.block_simultaneous_read}'. "
                    f"Will defer starting this stream if another stream in the same group or its parents are active."
                )

    def on_partition_generation_completed(
        self, sentinel: PartitionGenerationCompletedSentinel
    ) -> Iterable[AirbyteMessage]:
        """
        This method is called when a partition generation is completed.
        1. Remove the stream from the list of streams currently generating partitions
        2. Deactivate parent streams (they were only needed for partition generation)
        3. If the stream is done, mark it as such and return a stream status message
        4. If there are more streams to read from, start the next partition generator
        """
        stream_name = sentinel.stream.name
        self._streams_currently_generating_partitions.remove(sentinel.stream.name)

        # Deactivate all parent streams now that partition generation is complete
        # Parents were only needed to generate slices, they can now be reused
        parent_streams = self._collect_all_parent_stream_names(stream_name)
        for parent_stream_name in parent_streams:
            if parent_stream_name in self._active_stream_names:
                self._logger.debug(f"Removing '{parent_stream_name}' from active streams")
                self._active_stream_names.discard(parent_stream_name)

                # Remove from active groups
                parent_group = self._stream_block_simultaneous_read.get(parent_stream_name, "")
                if parent_group:
                    if parent_group in self._active_groups:
                        self._active_groups[parent_group].discard(parent_stream_name)
                        if not self._active_groups[parent_group]:
                            del self._active_groups[parent_group]
                    self._logger.info(
                        f"Parent stream '{parent_stream_name}' (group '{parent_group}') deactivated after "
                        f"partition generation completed for child '{stream_name}'. "
                        f"Blocked streams in the queue will be retried on next start_next_partition_generator call."
                    )

        # It is possible for the stream to already be done if no partitions were generated
        # If the partition generation process was completed and there are no partitions left to process, the stream is done
        if (
            self._is_stream_done(stream_name)
            or len(self._streams_to_running_partitions[stream_name]) == 0
        ):
            yield from self._on_stream_is_done(stream_name)
        if self._stream_instances_to_start_partition_generation:
            status_message = self.start_next_partition_generator()
            if status_message:
                yield status_message

    def on_partition(self, partition: Partition) -> None:
        """
        This method is called when a partition is generated.
        1. Add the partition to the set of partitions for the stream
        2. Log the slice if necessary
        3. Submit the partition to the thread pool manager
        """
        stream_name = partition.stream_name()
        self._streams_to_running_partitions[stream_name].add(partition)
        cursor = self._stream_name_to_instance[stream_name].cursor
        if self._slice_logger.should_log_slice_message(self._logger):
            self._message_repository.emit_message(
                self._slice_logger.create_slice_log_message(partition.to_slice())
            )
        self._thread_pool_manager.submit(
            self._partition_reader.process_partition, partition, cursor
        )

    def on_partition_complete_sentinel(
        self, sentinel: PartitionCompleteSentinel
    ) -> Iterable[AirbyteMessage]:
        """
        This method is called when a partition is completed.
        1. Close the partition
        2. If the stream is done, mark it as such and return a stream status message
        3. Emit messages that were added to the message repository
        """
        partition = sentinel.partition

        partitions_running = self._streams_to_running_partitions[partition.stream_name()]
        if partition in partitions_running:
            partitions_running.remove(partition)
            # If all partitions were generated and this was the last one, the stream is done
            if (
                partition.stream_name() not in self._streams_currently_generating_partitions
                and len(partitions_running) == 0
            ):
                yield from self._on_stream_is_done(partition.stream_name())
        yield from self._message_repository.consume_queue()

    def on_record(self, record: Record) -> Iterable[AirbyteMessage]:
        """
        This method is called when a record is read from a partition.
        1. Convert the record to an AirbyteMessage
        2. If this is the first record for the stream, mark the stream as RUNNING
        3. Increment the record counter for the stream
        4. Ensures the cursor knows the record has been successfully emitted
        5. Emit the message
        6. Emit messages that were added to the message repository
        """
        # Do not pass a transformer or a schema
        # AbstractStreams are expected to return data as they are expected.
        # Any transformation on the data should be done before reaching this point
        message = stream_data_to_airbyte_message(
            stream_name=record.stream_name,
            data_or_message=record.data,
            file_reference=record.file_reference,
        )
        stream = self._stream_name_to_instance[record.stream_name]

        if message.type == MessageType.RECORD:
            if self._record_counter[stream.name] == 0:
                self._logger.info(f"Marking stream {stream.name} as RUNNING")
                yield stream_status_as_airbyte_message(
                    stream.as_airbyte_stream(), AirbyteStreamStatus.RUNNING
                )
            self._record_counter[stream.name] += 1
        yield message
        yield from self._message_repository.consume_queue()

    def on_exception(self, exception: StreamThreadException) -> Iterable[AirbyteMessage]:
        """
        This method is called when an exception is raised.
        1. Stop all running streams
        2. Raise the exception
        """
        self._flag_exception(exception.stream_name, exception.exception)
        self._logger.exception(
            f"Exception while syncing stream {exception.stream_name}", exc_info=exception.exception
        )

        stream_descriptor = StreamDescriptor(name=exception.stream_name)
        if isinstance(exception.exception, AirbyteTracedException):
            yield exception.exception.as_airbyte_message(stream_descriptor=stream_descriptor)
        else:
            yield AirbyteTracedException.from_exception(
                exception, stream_descriptor=stream_descriptor
            ).as_airbyte_message()

    def _flag_exception(self, stream_name: str, exception: Exception) -> None:
        self._exceptions_per_stream_name.setdefault(stream_name, []).append(exception)

    def start_next_partition_generator(self) -> Optional[AirbyteMessage]:
        """
        Submits the next partition generator to the thread pool.

        A stream will be deferred (moved to end of queue) if:
        1. The stream itself has block_simultaneous_read=True AND is already active
        2. Any parent stream has block_simultaneous_read=True AND is currently active

        This prevents simultaneous reads of streams that shouldn't be accessed concurrently.

        :return: A status message if a partition generator was started, otherwise None
        """
        if not self._stream_instances_to_start_partition_generation:
            return None

        # Remember initial queue size to avoid infinite loops if all streams are blocked
        max_attempts = len(self._stream_instances_to_start_partition_generation)
        attempts = 0

        while self._stream_instances_to_start_partition_generation and attempts < max_attempts:
            attempts += 1

            # Pop the first stream from the queue
            stream = self._stream_instances_to_start_partition_generation.pop(0)
            stream_name = stream.name
            stream_group = self._stream_block_simultaneous_read.get(stream_name, "")

            # Check if this stream has a blocking group and is already active
            if stream_group and stream_name in self._active_stream_names:
                # Add back to the END of the queue for retry later
                self._stream_instances_to_start_partition_generation.append(stream)
                self._logger.info(
                    f"Deferring stream '{stream_name}' (group '{stream_group}') because it's already active. Trying next stream."
                )
                continue  # Try the next stream in the queue

            # Check if this stream's group is already active (another stream in the same group is running)
            if (
                stream_group
                and stream_group in self._active_groups
                and self._active_groups[stream_group]
            ):
                # Add back to the END of the queue for retry later
                self._stream_instances_to_start_partition_generation.append(stream)
                active_streams_in_group = self._active_groups[stream_group]
                self._logger.info(
                    f"Deferring stream '{stream_name}' (group '{stream_group}') because other stream(s) "
                    f"{active_streams_in_group} in the same group are active. Trying next stream."
                )
                continue  # Try the next stream in the queue

            # Check if any parent streams have a blocking group and are currently active
            parent_streams = self._collect_all_parent_stream_names(stream_name)
            blocked_by_parents = [
                p
                for p in parent_streams
                if self._stream_block_simultaneous_read.get(p, "")
                and p in self._active_stream_names
            ]

            if blocked_by_parents:
                # Add back to the END of the queue for retry later
                self._stream_instances_to_start_partition_generation.append(stream)
                parent_groups = {
                    self._stream_block_simultaneous_read.get(p, "") for p in blocked_by_parents
                }
                self._logger.info(
                    f"Deferring stream '{stream_name}' because parent stream(s) "
                    f"{blocked_by_parents} (groups {parent_groups}) are active. Trying next stream."
                )
                continue  # Try the next stream in the queue

            # No blocking - start this stream
            # Mark stream as active before starting
            self._active_stream_names.add(stream_name)
            self._streams_currently_generating_partitions.append(stream_name)

            # Track this stream in its group if it has one
            if stream_group:
                if stream_group not in self._active_groups:
                    self._active_groups[stream_group] = set()
                self._active_groups[stream_group].add(stream_name)
                self._logger.debug(f"Added '{stream_name}' to active group '{stream_group}'")

            # Also mark all parent streams as active (they will be read from during partition generation)
            parent_streams = self._collect_all_parent_stream_names(stream_name)
            for parent_stream_name in parent_streams:
                parent_group = self._stream_block_simultaneous_read.get(parent_stream_name, "")
                if parent_group:
                    self._active_stream_names.add(parent_stream_name)
                    if parent_group not in self._active_groups:
                        self._active_groups[parent_group] = set()
                    self._active_groups[parent_group].add(parent_stream_name)
                    self._logger.info(
                        f"Marking parent stream '{parent_stream_name}' (group '{parent_group}') as active "
                        f"(will be read during partition generation for '{stream_name}')"
                    )

            self._thread_pool_manager.submit(self._partition_enqueuer.generate_partitions, stream)
            self._logger.info(f"Marking stream {stream_name} as STARTED")
            self._logger.info(f"Syncing stream: {stream_name}")
            return stream_status_as_airbyte_message(
                stream.as_airbyte_stream(),
                AirbyteStreamStatus.STARTED,
            )

        # All streams in the queue are currently blocked
        return None

    def is_done(self) -> bool:
        """
        This method is called to check if the sync is done.
        The sync is done when:
        1. There are no more streams generating partitions
        2. There are no more streams to read from
        3. All partitions for all streams are closed
        """
        is_done = all(
            [
                self._is_stream_done(stream_name)
                for stream_name in self._stream_name_to_instance.keys()
            ]
        )
        if is_done and self._exceptions_per_stream_name:
            error_message = generate_failed_streams_error_message(self._exceptions_per_stream_name)
            self._logger.info(error_message)
            # We still raise at least one exception when a stream raises an exception because the platform currently relies
            # on a non-zero exit code to determine if a sync attempt has failed. We also raise the exception as a config_error
            # type because this combined error isn't actionable, but rather the previously emitted individual errors.
            raise AirbyteTracedException(
                message=error_message,
                internal_message="Concurrent read failure",
                failure_type=FailureType.config_error,
            )
        return is_done

    def _is_stream_done(self, stream_name: str) -> bool:
        return stream_name in self._streams_done

    def _collect_all_parent_stream_names(self, stream_name: str) -> Set[str]:
        """
        Recursively collect all parent stream names for a given stream.
        For example, if we have: epics -> issues -> comments
        Then for comments, this returns {issues, epics}

        :param stream_name: The stream to collect parents for
        :return: Set of all parent stream names (recursively)
        """
        parent_names: Set[str] = set()
        stream = self._stream_name_to_instance.get(stream_name)

        if not stream:
            return parent_names

        # Get partition router if it exists (this is where parent streams are defined)
        partition_router = None

        # Try DefaultStream path first (_stream_partition_generator._stream_slicer._partition_router)
        if (
            hasattr(stream, "_stream_partition_generator")
            and hasattr(stream._stream_partition_generator, "_stream_slicer")
            and hasattr(stream._stream_partition_generator._stream_slicer, "_partition_router")
        ):
            partition_router = stream._stream_partition_generator._stream_slicer._partition_router
        # Fallback to legacy path (retriever.partition_router) for backward compatibility and test mocks
        elif hasattr(stream, "retriever") and hasattr(stream.retriever, "partition_router"):
            partition_router = stream.retriever.partition_router

        # SubstreamPartitionRouter has parent_stream_configs
        if partition_router and hasattr(partition_router, "parent_stream_configs"):
            for parent_config in partition_router.parent_stream_configs:
                parent_stream = parent_config.stream
                parent_name = parent_stream.name
                parent_names.add(parent_name)

                # Recursively collect grandparents, great-grandparents, etc.
                parent_names.update(self._collect_all_parent_stream_names(parent_name))

        return parent_names

    def _on_stream_is_done(self, stream_name: str) -> Iterable[AirbyteMessage]:
        self._logger.info(
            f"Read {self._record_counter[stream_name]} records from {stream_name} stream"
        )
        self._logger.info(f"Marking stream {stream_name} as STOPPED")
        stream = self._stream_name_to_instance[stream_name]
        stream.cursor.ensure_at_least_one_state_emitted()
        yield from self._message_repository.consume_queue()
        self._logger.info(f"Finished syncing {stream.name}")
        self._streams_done.add(stream_name)
        stream_status = (
            AirbyteStreamStatus.INCOMPLETE
            if self._exceptions_per_stream_name.get(stream_name, [])
            else AirbyteStreamStatus.COMPLETE
        )
        yield stream_status_as_airbyte_message(stream.as_airbyte_stream(), stream_status)

        # Remove only this stream from active set (NOT parents)
        if stream_name in self._active_stream_names:
            self._active_stream_names.discard(stream_name)

            # Remove from active groups
            stream_group = self._stream_block_simultaneous_read.get(stream_name, "")
            if stream_group:
                if stream_group in self._active_groups:
                    self._active_groups[stream_group].discard(stream_name)
                    if not self._active_groups[stream_group]:
                        del self._active_groups[stream_group]
                self._logger.info(
                    f"Stream '{stream_name}' (group '{stream_group}') is no longer active. "
                    f"Blocked streams in the queue will be retried on next start_next_partition_generator call."
                )
