# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from abc import ABC, abstractmethod
from typing import Iterable

from airbyte_cdk.sources.types import StreamSlice


class StreamSlicer(ABC):
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
