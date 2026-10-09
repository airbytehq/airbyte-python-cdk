#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import datetime
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RequestWindowSplitting:
    """
    Configuration for the `request_window_splitting` retriever field - created once per stream and shared,
    mirroring `PageSizeReduction`. Unlike `PageSizeReducer`, there is no per-partition mutable counterpart: a
    split window is either split again (recursion) or the sync fails, so a plain config object is enough.
    """

    failure_message: Optional[str] = None
    # Independent of, and typically looser than, the cursor's own granularity floor - useful when a cursor
    # could split further but the API's rate limit makes many small requests worse than a few large ones.
    min_split_window: Optional[datetime.timedelta] = None
