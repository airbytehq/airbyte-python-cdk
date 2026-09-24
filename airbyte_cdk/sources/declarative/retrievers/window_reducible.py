#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class OnPartialResponse(Enum):
    FAIL = "FAIL"
    ALLOW_REPLAY = "ALLOW_REPLAY"


@dataclass(frozen=True)
class RequestWindowSplitting:
    """
    Configuration for the `request_window_splitting` retriever field - created once per stream and shared,
    mirroring `PageSizeReduction`. There is no per-partition mutable counterpart to instantiate the way
    `PageSizeReducer` is: splitting a window does not accumulate attempts across sibling requests the way
    reducing a page size does, since a split window is never retried at the same size twice - it is either
    split again (recursion) or the sync fails. All state needed to decide that lives in the `StreamSlice` being
    read and the `request_window_splitter` result for it, so a plain config object is enough.
    """

    on_partial_response: OnPartialResponse = OnPartialResponse.FAIL
    failure_message: Optional[str] = None
    # An upper bound on how small a window `request_window_splitter` is asked to produce, expressed in the same
    # domain terms as `cursor_granularity` (a duration) rather than a raw split count. Independent of - and
    # typically looser than - the cursor's own granularity floor: a connector whose cursor could technically
    # split down to the second may still want to stop earlier, e.g. because the API's rate limit makes many
    # small requests worse than a few large ones.
    min_split_window: Optional[datetime.timedelta] = None
