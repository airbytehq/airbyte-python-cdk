#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
import traceback
from dataclasses import InitVar, dataclass
from typing import Any, List, Mapping, Tuple

from airbyte_cdk import AbstractSource
from airbyte_cdk.sources.declarative.checks.connection_checker import ConnectionChecker
from airbyte_cdk.sources.streams.http.availability_strategy import HttpAvailabilityStrategy


@dataclass(frozen=True)
class DynamicStreamCheckConfig:
    """Defines the configuration for dynamic stream during connection checking. This class specifies
    what dynamic streams  in the stream template should be updated with value, supporting dynamic interpolation
    and type enforcement."""

    dynamic_stream_name: str
    stream_count: int = 0


@dataclass
class CheckStream(ConnectionChecker):
    """
    Checks the connections by checking availability of one or many streams selected by the developer

    Attributes:
        stream_name (List[str]): names of streams to check
    """

    stream_names: List[str]
    dynamic_streams_check_configs: List[DynamicStreamCheckConfig]
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters

    def check_connection(
        self, source: AbstractSource, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Any]:
        try:
            streams = source.streams(config=config)
        except Exception as error:
            error_message = (
                f"Encountered an error trying to connect to streams. Error: {error}"
            )
            logger.error(error_message, exc_info=True)
            return False, error_message

        stream_name_to_stream = {s.name: s for s in streams}
        if len(streams) == 0:
            return False, f"No streams to connect to from source {source}"
        for stream_name in self.stream_names:
            if stream_name not in stream_name_to_stream.keys():
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
                    f"Encountered an error trying to connect to stream {stream_name}. Error: \n {traceback.format_exc()}"
                )
                return False, f"Unable to connect to stream {stream_name} - {error}"

        if hasattr(source, "resolved_manifest") and hasattr(source, "dynamic_streams") and self.dynamic_streams_check_configs:
            dynamic_stream_name_to_dynamic_stream = {dynamic_stream.get("name", f"dynamic_stream_{i}"): dynamic_stream for i, dynamic_stream in enumerate(source.resolved_manifest.get("dynamic_streams", []))}

            dynamic_stream_name_to_generated_streams = {}
            for stream in source.dynamic_streams:
                dynamic_stream_name_to_generated_streams[
                    stream["dynamic_stream_name"]] = dynamic_stream_name_to_generated_streams.setdefault(
                    stream["dynamic_stream_name"], []) + [stream]

            for dynamic_streams_check_config in self.dynamic_streams_check_configs:
                dynamic_stream = dynamic_stream_name_to_dynamic_stream.get(dynamic_streams_check_config.dynamic_stream_name)

                is_config_depend = dynamic_stream["components_resolver"]["type"] == "ConfigComponentsResolver"

                if not is_config_depend and not bool(dynamic_streams_check_config.stream_count):
                    continue

                generated_streams = dynamic_stream_name_to_generated_streams.get(dynamic_streams_check_config.dynamic_stream_name)
                availability_strategy = HttpAvailabilityStrategy()

                for declarative_stream in generated_streams[: min(dynamic_streams_check_config.stream_count, len(generated_streams))]:
                    stream = stream_name_to_stream.get(declarative_stream["name"])
                    try:
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
