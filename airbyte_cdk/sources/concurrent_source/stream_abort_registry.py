# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

import threading
from typing import Set


class StreamAbortRegistry:
    """Thread-safe registry of streams whose remaining partitions should be skipped.

    When a partition raises a fatal, stream-wide error (e.g. an authorization failure
    that affects every partition of the stream), the concurrent reader records the
    stream here. Partitions that are still queued or not yet started then short-circuit
    instead of repeating the same doomed request, so the stream fails fast rather than
    exhausting every remaining partition.

    The registry is shared between the main thread (which marks streams as aborted) and
    the worker threads generating and reading partitions (which check it), so all access
    is guarded by a lock.
    """

    def __init__(self) -> None:
        self._aborted: Set[str] = set()
        self._lock = threading.Lock()

    def abort(self, stream_name: str) -> None:
        with self._lock:
            self._aborted.add(stream_name)

    def is_aborted(self, stream_name: str) -> bool:
        with self._lock:
            return stream_name in self._aborted
