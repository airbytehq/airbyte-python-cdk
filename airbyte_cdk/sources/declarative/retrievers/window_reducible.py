#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import datetime
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Protocol, runtime_checkable

from airbyte_cdk.sources.types import StreamSlice


class OnPartialResponse(Enum):
    FAIL = "FAIL"
    ALLOW_REPLAY = "ALLOW_REPLAY"


@runtime_checkable
class WindowReducible(Protocol):
    """
    Implemented by a stream slicer/cursor that can replace a failing `StreamSlice` with smaller child slices
    covering the same range.

    The component responsible for slicing owns the boundary field names, parsing, output formatting, and
    interval semantics (granularity, clamping, comparison), so `split_request_window` lives on that component - not on
    a standalone retriever-side parser - and is checked with `isinstance(slicer, WindowReducible)` wherever a
    retriever needs to know whether the stream it is reading supports request-window splitting at all.
    """

    def split_request_window(
        self, stream_slice: StreamSlice, min_split_window: Optional[datetime.timedelta] = None
    ) -> Optional[List[StreamSlice]]:
        """
        Split `stream_slice` into two or more smaller, non-overlapping child slices that together cover exactly
        the same range, preserving `partition` and `extra_fields` unchanged.

        Returns `None` when the slice cannot be split any further - either because it is already at (or below)
        the minimum granularity the cursor supports, because it is already at or below `min_split_window` (a
        connector-configured floor expressed in domain terms rather than raw split count), or because splitting
        it would not produce children strictly smaller than the parent (a no-progress guard independent of
        either floor). Callers should treat `None` as a terminal condition, not retry with the same slice.
        """
        raise NotImplementedError(
            "WindowReducible.split_request_window must be implemented by protocol implementers"
        )


@dataclass(frozen=True)
class RequestWindowSplitting:
    """
    Configuration for the `request_window_splitting` retriever field - created once per stream and shared,
    mirroring `PageSizeReduction`. There is no per-partition mutable counterpart to instantiate the way
    `PageSizeReducer` is: splitting a window does not accumulate attempts across sibling requests the way
    reducing a page size does, since a split window is never retried at the same size twice - it is either
    split again (recursion) or the sync fails. All state needed to decide that lives in the `StreamSlice` being
    read and the `WindowReducible.split_request_window` result for it, so a plain config object is enough.
    """

    on_partial_response: OnPartialResponse = OnPartialResponse.FAIL
    failure_message: Optional[str] = None
    # An upper bound on how small a window `split_request_window` is asked to produce, expressed in the same
    # domain terms as `cursor_granularity` (a duration) rather than a raw split count. Independent of - and
    # typically looser than - the cursor's own granularity floor: a connector whose cursor could technically
    # split down to the second may still want to stop earlier, e.g. because the API's rate limit makes many
    # small requests worse than a few large ones.
    min_split_window: Optional[datetime.timedelta] = None
