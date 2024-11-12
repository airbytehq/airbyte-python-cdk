#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import logging
import traceback
from collections.abc import Mapping
from dataclasses import InitVar, dataclass
from typing import TYPE_CHECKING, Any

from airbyte_cdk.sources.declarative.checks.connection_checker import ConnectionChecker
from airbyte_cdk.sources.streams.http.availability_strategy import HttpAvailabilityStrategy


if TYPE_CHECKING:
    from airbyte_cdk import AbstractSource


@dataclass
class CheckStream(ConnectionChecker):
    """Checks the connections by checking availability of one or many streams selected by the developer

    Attributes:
        stream_name (List[str]): names of streams to check
    """

    stream_names: list[str]
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters

    def check_connection(
        self, source: AbstractSource, logger: logging.Logger, config: Mapping[str, Any]
    ) -> tuple[bool, Any]:
        streams = source.streams(config=config)
        stream_name_to_stream = {s.name: s for s in streams}
        if len(streams) == 0:
            return False, f"No streams to connect to from source {source}"
        for stream_name in self.stream_names:
            if stream_name not in stream_name_to_stream:
                raise ValueError(
                    f"{stream_name} is not part of the catalog. Expected one of {stream_name_to_stream.keys()}."
                )

            stream = stream_name_to_stream[stream_name]
            availability_strategy = HttpAvailabilityStrategy()
            try:
                stream_is_available, reason = availability_strategy.check_availability(
                    stream, logger
                )
                if not stream_is_available:
                    return False, reason
            except Exception as error:
                logger.error(
                    f"Encountered an error trying to connect to stream {stream_name}. "
                    f"Error: \n {traceback.format_exc()}"
                )
                return False, f"Unable to connect to stream {stream_name} - {error}"
        return True, None
