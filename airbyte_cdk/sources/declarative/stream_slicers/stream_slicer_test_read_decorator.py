#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from itertools import islice
from typing import Any, Iterable, Mapping, Optional, Union

from airbyte_cdk.sources.types import StreamSlice, StreamState

from .stream_slicer import StreamSlicer


@dataclass
class StreamSlicerTestReadDecorator(StreamSlicer):
    """
    In some cases, we want to limit the number of requests that are made to the backend source. This class allows for limiting the number of
    slices that are queried throughout a read command.
    """

    wrapped_slicer: StreamSlicer
    maximum_number_of_slices: int = 5

    def stream_slices(self) -> Iterable[StreamSlice]:
        return islice(self.wrapped_slicer.stream_slices(), self.maximum_number_of_slices)

    def get_request_params(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return self.wrapped_slicer.get_request_params(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )

    def get_request_headers(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return self.wrapped_slicer.get_request_headers(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )

    def get_request_body_data(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Union[Mapping[str, Any], str]:
        return self.wrapped_slicer.get_request_body_data(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )

    def get_request_body_json(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return self.wrapped_slicer.get_request_body_json(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )

    def __getattr__(self, name: str) -> Any:
        # Delegate everything else to the wrapped object
        return getattr(self.wrapped_slicer, name)
