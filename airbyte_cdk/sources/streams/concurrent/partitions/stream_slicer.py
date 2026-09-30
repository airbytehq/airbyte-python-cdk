# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import threading
from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Iterable

from airbyte_cdk.sources.types import StreamSlice


class StreamSlicerMeta(ABCMeta):
    """
    Metaclass for wrapper scenario that allows it to be used as a type check for StreamSlicer.
    This is necessary because StreamSlicerTestReadDecorator wraps a StreamSlicer and we want to be able to check
    if an instance is a StreamSlicer, even if it is wrapped in a StreamSlicerTestReadDecorator.

    For example in ConcurrentDeclarativeSource, we do things like:
        isinstance(declarative_stream.retriever.stream_slicer,(GlobalSubstreamCursor, PerPartitionWithGlobalCursor))
    """

    _checking: threading.local = threading.local()

    def __instancecheck__(cls, instance: Any) -> bool:
        if not hasattr(cls._checking, "in_progress"):
            cls._checking.in_progress = set()

        instance_id = id(instance)
        if instance_id in cls._checking.in_progress:
            return super().__instancecheck__(instance)

        # Use object.__getattribute__ to bypass any custom __getattr__ that
        # could trigger further isinstance() calls and cause recursion.
        try:
            wrapped = object.__getattribute__(instance, "wrapped_slicer")
        except AttributeError:
            return super().__instancecheck__(instance)

        cls._checking.in_progress.add(instance_id)
        try:
            return isinstance(wrapped, cls)
        finally:
            cls._checking.in_progress.discard(instance_id)


class StreamSlicer(ABC, metaclass=StreamSlicerMeta):
    """
    Slices the stream into chunks that can be fetched independently. Slices enable state checkpointing and data retrieval parallelization.
    """

    @abstractmethod
    def stream_slices(self) -> Iterable[StreamSlice]:
        """
        Defines stream slices

        :return: An iterable of stream slices
        """
        pass
