# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


from dataclasses import InitVar, dataclass
from typing import Any, Iterable, Mapping, Optional

from typing_extensions import deprecated

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.async_job.job_orchestrator import AsyncPartition
from airbyte_cdk.sources.declarative.extractors.record_selector import RecordSelector
from airbyte_cdk.sources.declarative.incremental.declarative_cursor import DeclarativeCursor
from airbyte_cdk.sources.declarative.partition_routers.async_job_partition_router import (
    AsyncJobPartitionRouter,
)
from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.source import ExperimentalClassWarning
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.types import Config, Record, StreamSlice, StreamState
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


@deprecated(
    "This class is experimental. Use at your own risk.",
    category=ExperimentalClassWarning,
)
@dataclass
class AsyncRetriever(Retriever):
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    record_selector: RecordSelector
    stream_slicer: AsyncJobPartitionRouter

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters

    @property
    def state(self) -> StreamState:
        """
        Gets the current state of the stream.

        Returns:
            StreamState: Mapping[str, Any]
        """
        return self.stream_slicer.cursor.get_stream_state() if self.stream_slicer.cursor else {}

    @state.setter
    def state(self, value: StreamState) -> None:
        """State setter, accept state serialized by state getter."""
        if self.stream_slicer.cursor:
            self.stream_slicer.cursor.set_initial_state(value)

    @property
    def cursor(self) -> Optional[DeclarativeCursor]:
        return self.stream_slicer.cursor

    def _validate_and_get_stream_slice_partition(
        self, stream_slice: Optional[StreamSlice] = None
    ) -> AsyncPartition:
        """
        Validates the stream_slice argument and returns the partition from it.

        Args:
            stream_slice (Optional[StreamSlice]): The stream slice to validate and extract the partition from.

        Returns:
            AsyncPartition: The partition extracted from the stream_slice.

        Raises:
            AirbyteTracedException: If the stream_slice is not an instance of StreamSlice or if the partition is not present in the stream_slice.

        """
        if not isinstance(stream_slice, StreamSlice) or "partition" not in stream_slice.partition:
            raise AirbyteTracedException(
                message="Invalid arguments to AsyncJobRetriever.read_records: stream_slice is no optional. Please contact Airbyte Support",
                failure_type=FailureType.system_error,
            )
        return stream_slice["partition"]  # type: ignore  # stream_slice["partition"] has been added as an AsyncPartition as part of stream_slices

    def stream_slices(self) -> Iterable[Optional[StreamSlice]]:
        return self.stream_slicer.stream_slices()

    def read_records(
        self,
        records_schema: Mapping[str, Any],
        stream_slice: Optional[StreamSlice] = None,
    ) -> Iterable[StreamData]:
        _slice = stream_slice or StreamSlice(partition={}, cursor_slice={})  # None-check

        stream_state: StreamState = self.state
        partition: AsyncPartition = self._validate_and_get_stream_slice_partition(stream_slice)
        records: Iterable[Mapping[str, Any]] = self.stream_slicer.fetch_records(partition)
        most_recent_record_from_slice = None

        for stream_data in self.record_selector.filter_and_transform(
            all_data=records,
            stream_state=stream_state,
            records_schema=records_schema,
            stream_slice=_slice,
        ):
            if self.cursor and stream_data:
                self.cursor.observe(_slice, stream_data)

            most_recent_record_from_slice = self._get_most_recent_record(
                most_recent_record_from_slice, stream_data, _slice
            )
            yield stream_data

        if self.cursor:
            # DatetimeBasedCursor doesn't expect a partition field, but for AsyncRetriever streams this will
            # be the slice range
            slice_no_partition = StreamSlice(cursor_slice=_slice.cursor_slice, partition={})
            self.cursor.close_slice(slice_no_partition, most_recent_record_from_slice)

    def _get_most_recent_record(
        self,
        current_most_recent: Optional[Record],
        current_record: Optional[Record],
        stream_slice: StreamSlice,
    ) -> Optional[Record]:
        if self.cursor and current_record:
            if not current_most_recent:
                return current_record
            else:
                return (
                    current_most_recent
                    if self.cursor.is_greater_than_or_equal(current_most_recent, current_record)
                    else current_record
                )
        else:
            return None
