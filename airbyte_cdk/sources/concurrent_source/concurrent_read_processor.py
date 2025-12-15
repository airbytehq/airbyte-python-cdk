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
        self._record_counter: Dict[str, int] = {}
        self._streams_to_running_partitions: Dict[str, Set[Partition]] = {}
        for stream in stream_instances_to_read_from:
            self._streams_to_running_partitions[stream.name] = set()
            self._record_counter[stream.name] = 0
        self._thread_pool_manager = thread_pool_manager
        self._partition_enqueuer = partition_enqueuer
        self._stream_instances_to_start_partition_generation = list(stream_instances_to_read_from)
        self._streams_currently_generating_partitions: List[str] = []
        self._logger = logger
        self._slice_logger = slice_logger
        self._message_repository = message_repository
        self._partition_reader = partition_reader
        self._streams_done: Set[str] = set()
        self._exceptions_per_stream_name: Dict[str, List[Exception]] = {}

        # Track active concurrency groups and deferred streams
        self._active_concurrency_groups: Set[str] = set()
        self._deferred_streams: List[AbstractStream] = []

    def on_partition_generation_completed(
        self, sentinel: PartitionGenerationCompletedSentinel
    ) -> Iterable[AirbyteMessage]:
        """
        This method is called when a partition generation is completed.
        1. Remove the stream from the list of streams currently generating partitions
        2. If the stream is done, mark it as such and return a stream status message
        3. If there are more streams to read from, start the next partition generator
        """
        stream_name = sentinel.stream.name
        self._streams_currently_generating_partitions.remove(sentinel.stream.name)
        # It is possible for the stream to already be done if no partitions were generated
        # If the partition generation process was completed and there are no partitions left to process, the stream is done
        if (
            self._is_stream_done(stream_name)
            or len(self._streams_to_running_partitions[stream_name]) == 0
        ):
            yield from self._on_stream_is_done(stream_name)
        if self._stream_instances_to_start_partition_generation:
            yield self.start_next_partition_generator()  # type:ignore # None may be yielded

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
        Start the next partition generator.

        1. Find the next stream that can be started (respecting concurrency groups)
        2. Submit the partition generator to the thread pool manager
        3. Add the stream to the list of streams currently generating partitions
        4. Mark the concurrency group as active if applicable
        5. Return a stream status message
        """
        stream = self._get_next_eligible_stream()
        if stream:
            concurrency_group = stream.concurrency_group
            if concurrency_group:
                self._active_concurrency_groups.add(concurrency_group)
                self._logger.debug(
                    f"Stream {stream.name} activated concurrency group '{concurrency_group}'"
                )

            self._thread_pool_manager.submit(self._partition_enqueuer.generate_partitions, stream)
            self._streams_currently_generating_partitions.append(stream.name)
            self._logger.info(f"Marking stream {stream.name} as STARTED")
            self._logger.info(f"Syncing stream: {stream.name} ")
            return stream_status_as_airbyte_message(
                stream.as_airbyte_stream(),
                AirbyteStreamStatus.STARTED,
            )
        else:
            return None

    def _get_next_eligible_stream(self) -> Optional[AbstractStream]:
        """
        Get the next stream that can be started, respecting concurrency groups.

        Streams with a concurrency group that is already active will be deferred
        until the group becomes inactive.

        :return: The next eligible stream, or None if no streams are available
        """
        eligible_stream: Optional[AbstractStream] = None
        streams_to_defer: List[AbstractStream] = []

        while self._stream_instances_to_start_partition_generation:
            stream = self._stream_instances_to_start_partition_generation.pop(0)
            concurrency_group = stream.concurrency_group

            if concurrency_group and concurrency_group in self._active_concurrency_groups:
                # This stream's concurrency group is active, defer it
                streams_to_defer.append(stream)
                self._logger.debug(
                    f"Deferring stream {stream.name} because concurrency group "
                    f"'{concurrency_group}' is active"
                )
            else:
                # This stream can be started
                eligible_stream = stream
                break

        # Add deferred streams back to the list (at the end)
        self._deferred_streams.extend(streams_to_defer)

        return eligible_stream

    def is_done(self) -> bool:
        """
        Check if the sync is done.

        The sync is done when:
        1. There are no more streams generating partitions
        2. There are no more streams to read from (including deferred streams)
        3. All partitions for all streams are closed
        """
        # Check if there are still deferred streams waiting
        if self._deferred_streams:
            return False

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

        # Deactivate concurrency group if this stream had one and no other streams
        # in the same group are still running
        concurrency_group = stream.concurrency_group
        if concurrency_group and not self._is_concurrency_group_active(concurrency_group):
            self._active_concurrency_groups.discard(concurrency_group)
            self._logger.debug(
                f"Deactivated concurrency group '{concurrency_group}' after stream "
                f"{stream_name} completed"
            )
            # Re-queue deferred streams that were waiting for this group
            self._requeue_deferred_streams_for_group(concurrency_group)

        stream_status = (
            AirbyteStreamStatus.INCOMPLETE
            if self._exceptions_per_stream_name.get(stream_name, [])
            else AirbyteStreamStatus.COMPLETE
        )
        yield stream_status_as_airbyte_message(stream.as_airbyte_stream(), stream_status)

    def _is_concurrency_group_active(self, concurrency_group: str) -> bool:
        """
        Check if a concurrency group still has active streams.

        A group is active if any stream in the group is either generating partitions
        or has running partitions.

        :param concurrency_group: The concurrency group to check
        :return: True if the group has active streams, False otherwise
        """
        for stream_name in self._streams_currently_generating_partitions:
            stream = self._stream_name_to_instance[stream_name]
            if stream.concurrency_group == concurrency_group:
                return True

        for stream_name, partitions in self._streams_to_running_partitions.items():
            if partitions:  # Has running partitions
                stream = self._stream_name_to_instance[stream_name]
                if stream.concurrency_group == concurrency_group:
                    return True

        return False

    def _requeue_deferred_streams_for_group(self, concurrency_group: str) -> None:
        """
        Move deferred streams that were waiting for a concurrency group back to the main queue.

        :param concurrency_group: The concurrency group that just became inactive
        """
        streams_to_requeue: List[AbstractStream] = []
        remaining_deferred: List[AbstractStream] = []

        for stream in self._deferred_streams:
            if stream.concurrency_group == concurrency_group:
                streams_to_requeue.append(stream)
            else:
                remaining_deferred.append(stream)

        if streams_to_requeue:
            self._logger.debug(
                f"Re-queuing {len(streams_to_requeue)} deferred stream(s) for concurrency "
                f"group '{concurrency_group}': {[s.name for s in streams_to_requeue]}"
            )
            # Add to the front of the queue so they get processed next
            self._stream_instances_to_start_partition_generation = (
                streams_to_requeue + self._stream_instances_to_start_partition_generation
            )

        self._deferred_streams = remaining_deferred
