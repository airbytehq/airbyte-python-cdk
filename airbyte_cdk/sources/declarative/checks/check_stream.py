#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
import traceback
from dataclasses import InitVar, dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import dpath

from airbyte_cdk.sources import Source
from airbyte_cdk.sources.declarative.checks.connection_checker import ConnectionChecker
from airbyte_cdk.sources.streams.concurrent.abstract_stream import AbstractStream
from airbyte_cdk.sources.streams.core import Stream
from airbyte_cdk.sources.streams.http.availability_strategy import HttpAvailabilityStrategy


def evaluate_availability(
    stream: Union[Stream, AbstractStream], logger: logging.Logger
) -> Tuple[bool, Optional[str]]:
    """
    As a transition period, we want to support both Stream and AbstractStream until we migrate everything to AbstractStream.
    """
    if isinstance(stream, Stream):
        return HttpAvailabilityStrategy().check_availability(stream, logger)
    elif isinstance(stream, AbstractStream):
        availability = stream.check_availability()
        return availability.is_available, availability.reason
    else:
        raise ValueError(f"Unsupported stream type {type(stream)}")


@dataclass(frozen=True)
class DynamicStreamCheckConfig:
    """Defines the configuration for dynamic stream during connection checking. This class specifies
    what dynamic streams  in the stream template should be updated with value, supporting dynamic interpolation
    and type enforcement."""

    dynamic_stream_name: str
    stream_count: Optional[int] = None


@dataclass
class CheckStream(ConnectionChecker):
    """Checks the connection by verifying availability of one or many streams.

    Attributes:
        stream_names: Manifest-declared default stream names to check.
        dynamic_streams_check_configs: Optional dynamic-stream check configs.
        config_check_streams_path: Optional dot-delimited path into the
            user-provided config whose value (when present and a non-empty
            list) overrides `stream_names` for this check. When empty,
            missing, or `None`, the manifest's `stream_names` is used.
    """

    stream_names: List[str]
    parameters: InitVar[Mapping[str, Any]]
    dynamic_streams_check_configs: Optional[List[DynamicStreamCheckConfig]] = None
    config_check_streams_path: Optional[str] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters
        if self.dynamic_streams_check_configs is None:
            self.dynamic_streams_check_configs = []

    def _log_error(self, logger: logging.Logger, action: str, error: Exception) -> Tuple[bool, str]:
        """Logs an error and returns a formatted error message."""
        error_message = f"Encountered an error while {action}. Error: {error}"
        logger.error(error_message + f"Error traceback: \n {traceback.format_exc()}", exc_info=True)
        return False, error_message

    def _resolve_effective_stream_names(
        self, config: Mapping[str, Any]
    ) -> Tuple[Optional[List[str]], Optional[str]]:
        """Resolves the list of stream names to check for this connection.

        Returns a `(stream_names, error_message)` tuple. When `error_message`
        is set, the caller should short-circuit with `(False, error_message)`.
        When `config_check_streams_path` is unset, or the referenced value is
        missing or an empty list, falls back to the manifest's `stream_names`.
        """
        if not self.config_check_streams_path:
            return self.stream_names, None

        configured_value = dpath.get(
            dict(config), self.config_check_streams_path, separator=".", default=None
        )
        if configured_value is None:
            return self.stream_names, None
        if not isinstance(configured_value, list):
            return None, (
                f"Config field '{self.config_check_streams_path}' must be a list of stream names."
            )
        if not configured_value:
            return self.stream_names, None
        return list(configured_value), None

    def check_connection(
        self,
        source: Source,
        logger: logging.Logger,
        config: Mapping[str, Any],
    ) -> Tuple[bool, Any]:
        """Checks the connection to the source and its streams."""
        try:
            streams: List[Union[Stream, AbstractStream]] = source.streams(config=config)  # type: ignore  # this is a migration step and we expect the declarative CDK to migrate off of ConnectionChecker
            if not streams:
                return False, f"No streams to connect to from source {source}"
        except Exception as error:
            return self._log_error(logger, "discovering streams", error)

        stream_name_to_stream = {s.name: s for s in streams}

        effective_stream_names, override_error = self._resolve_effective_stream_names(config)
        if override_error is not None:
            return False, override_error

        source_label = (
            f"config path '{self.config_check_streams_path}'"
            if self.config_check_streams_path and effective_stream_names is not self.stream_names
            else "manifest"
        )

        unknown_stream_names = [
            stream_name
            for stream_name in (effective_stream_names or [])
            if stream_name not in stream_name_to_stream
        ]
        if unknown_stream_names:
            available = list(stream_name_to_stream.keys())
            message = (
                f"Stream(s) {unknown_stream_names} from {source_label} are not part of "
                f"the catalog. Expected one of {available}."
            )
            return False, message

        for stream_name in effective_stream_names or []:
            stream_availability, message = self._check_stream_availability(
                stream_name_to_stream, stream_name, logger
            )
            if not stream_availability:
                return stream_availability, message

        should_check_dynamic_streams = (
            hasattr(source, "resolved_manifest")
            and hasattr(source, "dynamic_streams")
            and self.dynamic_streams_check_configs
        )

        if should_check_dynamic_streams:
            return self._check_dynamic_streams_availability(source, stream_name_to_stream, logger)

        return True, None

    def _check_stream_availability(
        self,
        stream_name_to_stream: Dict[str, Union[Stream, AbstractStream]],
        stream_name: str,
        logger: logging.Logger,
    ) -> Tuple[bool, Any]:
        """Checks if streams are available."""
        try:
            stream = stream_name_to_stream[stream_name]
            stream_is_available, reason = evaluate_availability(stream, logger)
            if not stream_is_available:
                message = f"Stream {stream_name} is not available: {reason}"
                logger.warning(message)
                return stream_is_available, message
        except Exception as error:
            return self._log_error(logger, f"checking availability of stream {stream_name}", error)
        return True, None

    def _check_dynamic_streams_availability(
        self,
        source: Source,
        stream_name_to_stream: Dict[str, Union[Stream, AbstractStream]],
        logger: logging.Logger,
    ) -> Tuple[bool, Any]:
        """Checks the availability of dynamic streams."""
        dynamic_streams = source.resolved_manifest.get("dynamic_streams", [])  # type: ignore[attr-defined] # The source's resolved_manifest manifest is checked before calling this method
        dynamic_stream_name_to_dynamic_stream = {
            ds.get("name", f"dynamic_stream_{i}"): ds for i, ds in enumerate(dynamic_streams)
        }
        generated_streams = self._map_generated_streams(source.dynamic_streams)  # type: ignore[attr-defined] # The source's dynamic_streams manifest is checked before calling this method

        for check_config in self.dynamic_streams_check_configs:  # type: ignore[union-attr] # None value for self.dynamic_streams_check_configs handled in __post_init__
            if check_config.dynamic_stream_name not in dynamic_stream_name_to_dynamic_stream:
                return (
                    False,
                    f"Dynamic stream {check_config.dynamic_stream_name} is not found in manifest.",
                )

            generated = generated_streams.get(check_config.dynamic_stream_name, [])
            stream_availability, message = self._check_generated_streams_availability(
                generated, stream_name_to_stream, logger, check_config.stream_count
            )
            if not stream_availability:
                return stream_availability, message

        return True, None

    def _map_generated_streams(
        self, dynamic_streams: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Maps dynamic stream names to their corresponding generated streams."""
        mapped_streams: Dict[str, List[Dict[str, Any]]] = {}
        for stream in dynamic_streams:
            mapped_streams.setdefault(stream["dynamic_stream_name"], []).append(stream)
        return mapped_streams

    def _check_generated_streams_availability(
        self,
        generated_streams: List[Dict[str, Any]],
        stream_name_to_stream: Dict[str, Union[Stream, AbstractStream]],
        logger: logging.Logger,
        max_count: Optional[int],
    ) -> Tuple[bool, Any]:
        """Checks availability of generated dynamic streams.

        If `max_count` is `None`, all generated streams are checked. Otherwise, the
        first `max_count` streams are checked (capped at the number of available streams).
        """
        streams_to_check = generated_streams if max_count is None else generated_streams[:max_count]
        for declarative_stream in streams_to_check:
            stream = stream_name_to_stream[declarative_stream["name"]]
            try:
                stream_is_available, reason = evaluate_availability(stream, logger)
                if not stream_is_available:
                    message = f"Dynamic Stream {stream.name} is not available: {reason}"
                    logger.warning(message)
                    return False, message
            except Exception as error:
                return self._log_error(
                    logger, f"checking availability of dynamic stream {stream.name}", error
                )
        return True, None
