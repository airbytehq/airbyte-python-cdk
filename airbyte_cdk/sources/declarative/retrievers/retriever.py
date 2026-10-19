#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import abstractmethod
from typing import Any, Iterable, Mapping, Optional

from typing_extensions import deprecated

from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.types import StreamSlice, StreamState


class Retriever:
    """
    Responsible for fetching a stream's records from an HTTP API source.

    A stream creates one retriever and reads all its partitions through it: `read_records` is called from several
    threads at once, one call per stream slice. Implementations, custom ones included, must keep per-read state such
    as page tokens, counters or recursion depth in local variables rather than on the instance or its components.
    """

    @abstractmethod
    def read_records(
        self,
        records_schema: Mapping[str, Any],
        stream_slice: Optional[StreamSlice] = None,
    ) -> Iterable[StreamData]:
        """
        Fetch a stream's records from an HTTP API source

        :param records_schema: json schema to describe record
        :param stream_slice: The stream slice to read data for
        :return: The records read from the API source
        """

    @deprecated("Stream slicing is being moved to the stream level.")
    def stream_slices(self) -> Iterable[Optional[StreamSlice]]:
        """Does nothing as this method is deprecated, so underlying Retriever implementations
        do not need to implement this.
        """
        yield from []

    @property
    @deprecated("State management is being moved to the stream level.")
    def state(self) -> StreamState:
        """
        Does nothing as this method is deprecated, so underlying Retriever implementations
        do not need to implement this.
        """
        return {}

    @state.setter
    @deprecated("State management is being moved to the stream level.")
    def state(self, value: StreamState) -> None:
        """
        Does nothing as this method is deprecated, so underlying Retriever implementations
        do not need to implement this.
        """
        pass
