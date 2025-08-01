# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from dataclasses import InitVar, dataclass
from typing import Any, Iterable, Mapping, Optional

from airbyte_cdk.sources.declarative.incremental import DeclarativeCursor
from airbyte_cdk.sources.declarative.types import Record, StreamSlice, StreamState
from airbyte_cdk.sources.streams.checkpoint.checkpoint_reader import FULL_REFRESH_COMPLETE_STATE


@dataclass
class ResumableFullRefreshCursor(DeclarativeCursor):
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._cursor: StreamState = {}

    def get_stream_state(self) -> StreamState:
        return self._cursor

    def set_initial_state(self, stream_state: StreamState) -> None:
        self._cursor = stream_state

    def observe(self, stream_slice: StreamSlice, record: Record) -> None:
        """
        Resumable full refresh manages state using a page number so it does not need to update state by observing incoming records.
        """
        pass

    def close_slice(self, stream_slice: StreamSlice, *args: Any) -> None:
        # The ResumableFullRefreshCursor doesn't support nested streams yet so receiving a partition is unexpected
        if stream_slice.partition:
            raise ValueError(
                f"Stream slice {stream_slice} should not have a partition. Got {stream_slice.partition}."
            )
        self._cursor = stream_slice.cursor_slice

    def should_be_synced(self, record: Record) -> bool:
        """
        Unlike date-based cursors which filter out records outside slice boundaries, resumable full refresh records exist within pages
        that don't have filterable bounds. We should always return them.
        """
        return True

    def select_state(self, stream_slice: Optional[StreamSlice] = None) -> Optional[StreamState]:
        # A top-level RFR cursor only manages the state of a single partition
        return self._cursor

    def stream_slices(self) -> Iterable[StreamSlice]:
        """
        Resumable full refresh cursors only return a single slice and can't perform partitioning because iteration is done per-page
        along an unbounded set.
        """
        yield from [StreamSlice(cursor_slice=self._cursor, partition={})]

    # This is an interesting pattern that might not seem obvious at first glance. This cursor itself has no functional need to
    # inject any request values into the outbound response because the up-to-date pagination state is already loaded and
    # maintained by the paginator component
    def get_request_params(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {}

    def get_request_headers(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {}

    def get_request_body_data(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {}

    def get_request_body_json(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {}


@dataclass
class ChildPartitionResumableFullRefreshCursor(ResumableFullRefreshCursor):
    """
    The Sub-stream Resumable Cursor for Full-Refresh substreams.
    Follows the parent type `ResumableFullRefreshCursor` with a small override,
    to provide the ability to close the substream's slice once it has finished processing.

    Check the `close_slice` method overide for more info about the actual behaviour of this cursor.
    """

    def close_slice(self, stream_slice: StreamSlice, *args: Any) -> None:
        """
        Once the current slice has finished syncing:
         - paginator returns None
         - no more slices to process

        we assume that the records are processed and emitted already,
        thus we have to set the cursor to ` __ab_full_refresh_sync_complete: true `,
        otherwise there is a risk of Inf. Loop processing the same slice.
        """
        self._cursor = FULL_REFRESH_COMPLETE_STATE
