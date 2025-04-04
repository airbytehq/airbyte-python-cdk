#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from itertools import islice
from typing import Any, Iterable, Mapping, Optional, Union

from airbyte_cdk.sources.declarative.requesters.request_options.request_options_provider import (
    RequestOptionsProvider,
)
from airbyte_cdk.sources.streams.concurrent.partitions.stream_slicer import (
    StreamSlicer as ConcurrentStreamSlicer,
)
from airbyte_cdk.sources.types import StreamSlice, StreamState


class StreamSlicer(ConcurrentStreamSlicer, RequestOptionsProvider, ABC):
    """
    Slices the stream into a subset of records.
    Slices enable state checkpointing and data retrieval parallelization.

    The stream slicer keeps track of the cursor state as a dict of cursor_field -> cursor_value

    See the stream slicing section of the docs for more information.
    """

    pass


class TestReadSlicerDecorator(StreamSlicer):
    """
    A stream slicer wrapper for test reads which limits the number of slices produced.
    """

    def __init__(self, stream_slicer: StreamSlicer, maximum_number_of_slices: int) -> None:
        self._decorated = stream_slicer
        self._maximum_number_of_slices = maximum_number_of_slices

    def stream_slices(self) -> Iterable[StreamSlice]:
        return islice(self._decorated.stream_slices(), self._maximum_number_of_slices)

    def get_request_params(self, *, stream_state: Optional[StreamState] = None, stream_slice: Optional[StreamSlice] = None,
                           next_page_token: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        return self._decorated.get_request_params(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )

    def get_request_headers(self, *, stream_state: Optional[StreamState] = None, stream_slice: Optional[StreamSlice] = None,
                            next_page_token: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        return self._decorated.get_request_headers(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )

    def get_request_body_data(self, *, stream_state: Optional[StreamState] = None, stream_slice: Optional[StreamSlice] = None,
                              next_page_token: Optional[Mapping[str, Any]] = None) -> Union[Mapping[str, Any], str]:
        return self._decorated.get_request_body_data(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )

    def get_request_body_json(self, *, stream_state: Optional[StreamState] = None, stream_slice: Optional[StreamSlice] = None,
                              next_page_token: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        return self._decorated.get_request_body_json(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )