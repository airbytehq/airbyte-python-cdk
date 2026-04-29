# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Module-level registry for the concurrent source queue.

The heartbeat thread in entrypoint.py needs to report queue stats (size, full/empty)
to help diagnose deadlocks. Since the queue is created deep inside ConcurrentSource,
this registry provides a lightweight way to expose it without threading the queue
object through the entire call chain.

Usage:
    # In ConcurrentSource.read():
    register_queue(self._queue)

    # In the heartbeat thread:
    q = get_queue()
    if q is not None:
        print(f"queue_size={q.qsize()} queue_full={q.full()}")
"""

from queue import Queue
from typing import Optional

from airbyte_cdk.sources.streams.concurrent.partitions.types import QueueItem

_queue: Optional[Queue[QueueItem]] = None


def register_queue(queue: Queue[QueueItem]) -> None:
    """Register the concurrent source queue for heartbeat monitoring."""
    global _queue
    _queue = queue


def get_queue() -> Optional[Queue[QueueItem]]:
    """Return the registered queue, or None if no concurrent source is active."""
    return _queue


def unregister_queue() -> None:
    """Clear the registered queue."""
    global _queue
    _queue = None
