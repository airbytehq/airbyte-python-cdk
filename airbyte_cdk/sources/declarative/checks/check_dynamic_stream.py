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

    stream_count: int
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters

    def check_connection(
        self, source: AbstractSource, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Any]:
        streams = source.streams(config=config)
        if len(streams) == 0:
            return False, f"No streams to connect to from source {source}"

        for stream_index in range(min(self.stream_count, len(streams))):
            stream = streams[stream_index]
            availability_strategy = HttpAvailabilityStrategy()
            try:
                stream_is_available, reason = availability_strategy.check_availability(
                    stream, logger
                )
                if not stream_is_available:
                    return False, reason
            except Exception as error:
                logger.error(
                    f"Encountered an error trying to connect to stream {stream.name}. Error: \n {traceback.format_exc()}"
                )
                return False, f"Unable to connect to stream {stream.name} - {error}"
        return True, None
