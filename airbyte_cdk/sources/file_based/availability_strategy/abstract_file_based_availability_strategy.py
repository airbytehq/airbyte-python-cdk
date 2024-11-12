#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING

from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.concurrent.availability_strategy import (
    AbstractAvailabilityStrategy,
    StreamAvailability,
    StreamAvailable,
    StreamUnavailable,
)


if TYPE_CHECKING:
    import logging

    from airbyte_cdk.sources import Source
    from airbyte_cdk.sources.file_based.stream import AbstractFileBasedStream
    from airbyte_cdk.sources.streams.core import Stream


class AbstractFileBasedAvailabilityStrategy(AvailabilityStrategy):
    @abstractmethod
    def check_availability(
        self, stream: Stream, logger: logging.Logger, _: Source | None
    ) -> tuple[bool, str | None]:
        """Perform a connection check for the stream.

        Returns (True, None) if successful, otherwise (False, <error message>).
        """
        ...

    @abstractmethod
    def check_availability_and_parsability(
        self, stream: AbstractFileBasedStream, logger: logging.Logger, _: Source | None
    ) -> tuple[bool, str | None]:
        """Performs a connection check for the stream, as well as additional checks that
        verify that the connection is working as expected.

        Returns (True, None) if successful, otherwise (False, <error message>).
        """
        ...


class AbstractFileBasedAvailabilityStrategyWrapper(AbstractAvailabilityStrategy):
    def __init__(self, stream: AbstractFileBasedStream) -> None:
        self.stream = stream

    def check_availability(self, logger: logging.Logger) -> StreamAvailability:
        is_available, reason = self.stream.availability_strategy.check_availability(
            self.stream, logger, None
        )
        if is_available:
            return StreamAvailable()
        return StreamUnavailable(reason or "")

    def check_availability_and_parsability(self, logger: logging.Logger) -> tuple[bool, str | None]:
        return self.stream.availability_strategy.check_availability_and_parsability(
            self.stream, logger, None
        )
