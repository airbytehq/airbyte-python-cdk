#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from queue import Queue
from typing import Optional

from airbyte_cdk.sources.streams.concurrent.partitions.types import QueueItem

_queue: Optional[Queue[QueueItem]] = None


def register_queue(queue: Queue[QueueItem]) -> None:
    global _queue
    _queue = queue


def get_queue() -> Optional[Queue[QueueItem]]:
    return _queue


def unregister_queue() -> None:
    global _queue
    _queue = None
