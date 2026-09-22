#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

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
    interval semantics (granularity, clamping, comparison), so `reduce_window` lives on that component - not on
    a standalone retriever-side parser - and is checked with `isinstance(slicer, WindowReducible)` wherever a
    retriever needs to know whether the stream it is reading supports request-window reduction at all.
    """

    def reduce_window(self, stream_slice: StreamSlice) -> Optional[List[StreamSlice]]:
        """
        Split `stream_slice` into two or more smaller, non-overlapping child slices that together cover exactly
        the same range, preserving `partition` and `extra_fields` unchanged.

        Returns `None` when the slice cannot be split any further - either because it is already at (or below)
        the minimum granularity the cursor supports, or because splitting it would not produce children strictly
        smaller than the parent (a no-progress guard independent of the granularity check). Callers should treat
        `None` as a terminal condition, not retry with the same slice.
        """
        ...


@dataclass(frozen=True)
class RequestWindowReduction:
    """
    Configuration for the `request_window_reduction` retriever field - created once per stream and shared,
    mirroring `PageSizeReduction`. There is no per-partition mutable counterpart to instantiate the way
    `PageSizeReducer` is: reducing a window does not accumulate attempts across sibling requests the way
    reducing a page size does, since a reduced window is never retried at the same size twice - it is either
    split again (recursion) or the sync fails. All state needed to decide that lives in the `StreamSlice` being
    read and the `WindowReducible.reduce_window` result for it, so a plain config object is enough.
    """

    on_partial_response: OnPartialResponse = OnPartialResponse.FAIL
    failure_message: Optional[str] = None
