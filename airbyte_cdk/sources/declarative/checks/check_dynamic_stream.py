#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import logging
import traceback
from dataclasses import InitVar, dataclass
from typing import Any, List, Mapping, Tuple

from airbyte_cdk import AbstractSource
from airbyte_cdk.sources.declarative.checks.connection_checker import ConnectionChecker
from airbyte_cdk.sources.streams.http.availability_strategy import HttpAvailabilityStrategy


@dataclass
class CheckDynamicStream(ConnectionChecker):
    """
    Checks the connections by checking availability of one or many dynamic streams

    Attributes:
        stream_count (int): numbers of streams to check
    """

    # TODO: Add field stream_names to check_connection for static streams
    #  https://github.com/airbytehq/airbyte-python-cdk/pull/293#discussion_r1934933483

    stream_count: int
    parameters: InitVar[Mapping[str, Any]]
    use_check_availability: bool = True

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters

    def check_connection(
        self, source: AbstractSource, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Any]:
        streams = source.streams(config=config)

        if len(streams) == 0:
            return False, f"No streams to connect to from source {source}"
        if not self.use_check_availability:
            return True, None

        availability_strategy = HttpAvailabilityStrategy()

        try:
            for stream in streams[: min(self.stream_count, len(streams))]:
                stream_is_available, reason = availability_strategy.check_availability(
                    stream, logger
                )
                if not stream_is_available:
                    logger.warning(f"Stream {stream.name} is not available: {reason}")
                    return False, reason
        except Exception as error:
            error_message = (
                f"Encountered an error trying to connect to stream {stream.name}. Error: {error}"
            )
            logger.error(error_message, exc_info=True)
            return False, error_message

        return True, None
