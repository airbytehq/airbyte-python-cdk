# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""
The AirbyteEntrypoint is important because it is a service layer that orchestrate how we execute commands from the
[common interface](https://docs.airbyte.com/understanding-airbyte/airbyte-protocol#common-interface) through the source Python
implementation. There is some logic about which message we send to the platform and when which is relevant for integration testing. Other
than that, there are integrations point that are annoying to integrate with using Python code:
* Sources communicate with the platform using stdout. The implication is that the source could just print every message instead of
    returning things to source.<method> or to using the message repository. WARNING: As part of integration testing, we will not support
    messages that are simply printed. The reason is that capturing stdout relies on overriding sys.stdout (see
    https://docs.python.org/3/library/contextlib.html#contextlib.redirect_stdout) which clashes with how pytest captures logs and brings
    considerations for multithreaded applications. If code you work with uses `print` statements, please migrate to
    source.message_repository to emit those messages
* The entrypoint interface relies on file being written on the file system
"""

import json
import logging
import re
import tempfile
import traceback
from collections.abc import Callable
from io import StringIO
from pathlib import Path
from typing import Any, List, Mapping, Optional, Union

import orjson
from requests_cache import Iterable
from serpyco_rs import SchemaValidationError
from typing_extensions import deprecated
from ulid import T

from airbyte_cdk.connector_builder.models import LogMessage
from airbyte_cdk.exception_handler import assemble_uncaught_exception
from airbyte_cdk.logger import AirbyteLogFormatter
from airbyte_cdk.models import (
    AirbyteLogMessage,
    AirbyteMessage,
    AirbyteMessageSerializer,
    AirbyteStateMessage,
    AirbyteStateMessageSerializer,
    AirbyteStreamStatus,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteCatalogSerializer,
    Level,
    TraceType,
    Type,
)
from airbyte_cdk.models.airbyte_protocol import AirbyteMessage
from airbyte_cdk.sources.message.repository import (
    InMemoryMessageRepository,
    MessageRepository,
    _is_severe_enough,
)
from airbyte_cdk.sources.source import Source
from airbyte_cdk.utils.airbyte_secrets_utils import filter_secrets
from airbyte_cdk.utils.cli_arg_parse import ConnectorCLIArgs, parse_cli_args


class TestOutputMessageRepository(MessageRepository):
    """An implementation of MessageRepository used for testing.

    It captures both the messages emitted by the source and the logs printed to stdout.

    This class replaces `EntrypointOutput`.

    Warning: OOM errors may occur if the source generates a large number of messages.
    TODO: Optimize this to switch to a disk-side buffer if available memory is at risk of being
    overrun.
    """

    def __init__(self, log_level: Level = Level.INFO) -> None:
        self._log_level = log_level
        self._messages: list[AirbyteMessage] = []
        self._ignored_logs: list[LogMessage] = []
        self._consumed_to_marker = 0

    def emit_message(self, message: AirbyteMessage) -> None:
        self._messages.append(message)

    def log_message(self, level: Level, message_provider: Callable[[], LogMessage]) -> None:
        if _is_severe_enough(self._log_level, level):
            self.emit_message(
                AirbyteMessage(
                    type=Type.LOG,
                    log=AirbyteLogMessage(
                        level=level, message=filter_secrets(json.dumps(message_provider()))
                    ),
                )
            )
        else:
            self._ignored_logs.append(message_provider())

    def consume_queue(self) -> Iterable[AirbyteMessage]:
        """Consume the message queue and return all messages.

        This method primarily exists to support the `MessageRepository` interface.
        Note: Callers can more easily consume the queue by reading from `messages` directly.

        To avoid race conditions, we first get the high-water mark and then return to that point.
        """
        self._consumed_to_marker = len(self._messages)
        return self._messages[: self._consumed_to_marker]

    @staticmethod
    def _parse_message(message: str) -> AirbyteMessage:
        try:
            return AirbyteMessage.from_json(message)
        except (orjson.JSONDecodeError, SchemaValidationError):
            # The platform assumes that logs that are not of AirbyteMessage format are log messages
            return AirbyteMessage(
                type=Type.LOG, log=AirbyteLogMessage(level=Level.INFO, message=message)
            )

    @property
    def records_and_state_messages(self) -> List[AirbyteMessage]:
        return self._get_message_by_types([Type.RECORD, Type.STATE])

    @property
    def records(self) -> List[AirbyteMessage]:
        return self._get_message_by_types([Type.RECORD])

    @property
    def state_messages(self) -> List[AirbyteMessage]:
        return self._get_message_by_types([Type.STATE])

    @property
    def spec_messages(self) -> List[AirbyteMessage]:
        return self._get_message_by_types([Type.SPEC])

    @property
    def connection_status_messages(self) -> List[AirbyteMessage]:
        return self._get_message_by_types([Type.CONNECTION_STATUS])

    @property
    def most_recent_state(self) -> Any:
        state_messages = self._get_message_by_types([Type.STATE])
        if not state_messages:
            raise ValueError("Can't provide most recent state as there are no state messages")
        return state_messages[-1].state.stream  # type: ignore[union-attr] # state has `stream`

    @property
    def logs(self) -> List[AirbyteMessage]:
        return self._get_message_by_types([Type.LOG])

    @property
    def trace_messages(self) -> List[AirbyteMessage]:
        return self._get_message_by_types([Type.TRACE])

    @property
    def analytics_messages(self) -> List[AirbyteMessage]:
        return self._get_trace_message_by_trace_type(TraceType.ANALYTICS)

    @property
    def errors(self) -> List[AirbyteMessage]:
        return self._get_trace_message_by_trace_type(TraceType.ERROR)

    @property
    def catalog(self) -> AirbyteMessage:
        catalog = self._get_message_by_types([Type.CATALOG])
        if len(catalog) != 1:
            raise ValueError(f"Expected exactly one catalog but got {len(catalog)}")
        return catalog[0]

    def get_stream_statuses(self, stream_name: str) -> List[AirbyteStreamStatus]:
        status_messages = map(
            lambda message: message.trace.stream_status.status,  # type: ignore
            filter(
                lambda message: message.trace.stream_status.stream_descriptor.name == stream_name,  # type: ignore # callable; trace has `stream_status`
                self._get_trace_message_by_trace_type(TraceType.STREAM_STATUS),
            ),
        )
        return list(status_messages)

    def _get_message_by_types(self, message_types: list[Type]) -> list[AirbyteMessage]:
        """Return all messages of the given types."""
        return [message for message in self._messages if message.type in message_types]

    def _get_trace_message_by_trace_type(self, trace_type: TraceType) -> List[AirbyteMessage]:
        return [
            message
            for message in self._get_message_by_types([Type.TRACE])
            if message.trace.type == trace_type  # type: ignore[union-attr] # trace has `type`
        ]

    def is_in_logs(self, pattern: str) -> bool:
        """Check if any log message case-insensitive matches the pattern."""
        return any(
            re.search(
                pattern,
                entry.log.message,  # type: ignore[union-attr] # log has `message`
                flags=re.IGNORECASE,
            )
            for entry in self.logs
        )

    def is_not_in_logs(self, pattern: str) -> bool:
        """Check if no log message matches the case-insensitive pattern."""
        return not self.is_in_logs(pattern)


@deprecated("Please use `TestOutputMessageRepository` instead.")
class EntrypointOutput(TestOutputMessageRepository):
    """A class that captures the output of the entrypoint.

    It captures both the messages emitted by the source and the logs printed to stdout.
    """

    def __init__(
        self,
        messages: list[str] | None = None,
        uncaught_exception: BaseException | None = None,
    ) -> None:
        super().__init__()
        for msg in messages or []:
            self.emit_message(self._parse_message(msg))

        self._uncaught_exception = uncaught_exception
        if uncaught_exception:
            self.emit_message(
                assemble_uncaught_exception(
                    type(uncaught_exception), uncaught_exception
                ).as_airbyte_message()
            )

    @property
    def uncaught_exception(self) -> BaseException | None:
        return self._uncaught_exception


def _run_command(
    source: Source, args: list[str], expecting_exception: bool = False
) -> TestOutputMessageRepository:
    log_capture_buffer = StringIO()
    stream_handler = logging.StreamHandler(log_capture_buffer)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(AirbyteLogFormatter())
    parent_logger = logging.getLogger("")
    parent_logger.addHandler(stream_handler)

    message_repository = TestOutputMessageRepository()
    try:
        source.launch_with_cli_args(
            args,
            logger=parent_logger,
            message_repository=message_repository,
        )
    except Exception as exception:
        if not expecting_exception:
            print("Printing unexpected error from entrypoint_wrapper")
            print("".join(traceback.format_exception(None, exception, exception.__traceback__)))
        uncaught_exception = exception

    parent_logger.removeHandler(stream_handler)

    return message_repository


def discover(
    source: Source,
    config: Mapping[str, Any],
    expecting_exception: bool = False,
) -> TestOutputMessageRepository:
    """
    config must be json serializable
    :param expecting_exception: By default if there is an uncaught exception, the exception will be printed out. If this is expected, please
        provide expecting_exception=True so that the test output logs are cleaner
    """

    with tempfile.TemporaryDirectory() as tmp_directory:
        tmp_directory_path = Path(tmp_directory)
        config_file = make_file(tmp_directory_path / "config.json", config)

        return _run_command(
            source, ["discover", "--config", config_file, "--debug"], expecting_exception
        )


def read(
    source: Source,
    config: Mapping[str, Any],
    catalog: ConfiguredAirbyteCatalog,
    state: Optional[List[AirbyteStateMessage]] = None,
    expecting_exception: bool = False,
) -> TestOutputMessageRepository:
    """
    config and state must be json serializable

    :param expecting_exception: By default if there is an uncaught exception, the exception will be printed out. If this is expected, please
        provide expecting_exception=True so that the test output logs are cleaner
    """
    with tempfile.TemporaryDirectory() as tmp_directory:
        tmp_directory_path = Path(tmp_directory)
        config_file = make_file(tmp_directory_path / "config.json", config)
        catalog_file = make_file(
            tmp_directory_path / "catalog.json",
            orjson.dumps(ConfiguredAirbyteCatalogSerializer.dump(catalog)).decode(),
        )
        args = [
            "read",
            "--config",
            config_file,
            "--catalog",
            catalog_file,
        ]
        if state is not None:
            args.extend(
                [
                    "--state",
                    make_file(
                        tmp_directory_path / "state.json",
                        f"[{','.join([orjson.dumps(AirbyteStateMessageSerializer.dump(stream_state)).decode() for stream_state in state])}]",
                    ),
                ]
            )

        return _run_command(source, args, expecting_exception)


def make_file(
    path: Path, file_contents: Optional[Union[str, Mapping[str, Any], List[Mapping[str, Any]]]]
) -> str:
    if isinstance(file_contents, str):
        path.write_text(file_contents)
    else:
        path.write_text(json.dumps(file_contents))
    return str(path)
