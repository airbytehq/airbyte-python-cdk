#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import argparse
import io
import logging
import sys
from abc import ABC, abstractmethod
from multiprocessing import Value
from typing import Any, Iterable, List, Mapping

import orjson

from airbyte_cdk.connector import Connector
from airbyte_cdk.exception_handler import init_uncaught_exception_handler
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteMessageSerializer,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteCatalogSerializer,
    Type,
)
from airbyte_cdk.sources.utils.schema_helpers import check_config_against_spec_or_exit
from airbyte_cdk.utils.cli_arg_parse import ConnectorCLIArgs, parse_cli_args
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

logger = logging.getLogger("airbyte")


class Destination(Connector, ABC):
    VALID_CMDS = {"spec", "check", "write"}

    @abstractmethod
    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """Implement to define how the connector writes data to the destination"""

    def _run_check(self, config: Mapping[str, Any]) -> AirbyteMessage:
        check_result = self.check(logger, config)
        return AirbyteMessage(type=Type.CONNECTION_STATUS, connectionStatus=check_result)

    def _parse_input_stream(self, input_stream: io.TextIOWrapper) -> Iterable[AirbyteMessage]:
        """Reads from stdin, converting to Airbyte messages"""
        for line in input_stream:
            try:
                yield AirbyteMessageSerializer.load(orjson.loads(line))
            except orjson.JSONDecodeError:
                logger.info(
                    f"ignoring input which can't be deserialized as Airbyte Message: {line}"
                )

    def _run_write(
        self,
        config: Mapping[str, Any],
        configured_catalog_path: str,
        input_stream: io.TextIOWrapper,
    ) -> Iterable[AirbyteMessage]:
        catalog = ConfiguredAirbyteCatalogSerializer.load(
            orjson.loads(open(configured_catalog_path).read())
        )
        input_messages = self._parse_input_stream(input_stream)
        logger.info("Begin writing to the destination...")
        yield from self.write(
            config=config, configured_catalog=catalog, input_messages=input_messages
        )
        logger.info("Writing complete.")

    def run_cmd(
        self,
        parsed_args: ConnectorCLIArgs,
    ) -> Iterable[AirbyteMessage]:
        cmd = parsed_args.command
        if cmd not in self.VALID_CMDS:
            raise Exception(f"Unrecognized command: {cmd}")

        spec = self.spec(logger)
        if cmd == "spec":
            yield AirbyteMessage(type=Type.SPEC, spec=spec)
            return
        config = self.read_config(config_path=parsed_args.config)
        if self.check_config_against_spec or cmd == "check":
            try:
                check_config_against_spec_or_exit(config, spec)
            except AirbyteTracedException as traced_exc:
                connection_status = traced_exc.as_connection_status_message()
                if connection_status and cmd == "check":
                    yield connection_status
                    return
                raise traced_exc

        if cmd == "check":
            yield self._run_check(config=config)
        elif cmd == "write":
            if not parsed_args.catalog:
                raise ValueError("Catalog path is required for write command.")

            # Wrap in UTF-8 to override any other input encodings
            wrapped_stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
            yield from self._run_write(
                config=config,
                configured_catalog_path=parsed_args.catalog,
                input_stream=wrapped_stdin,
            )

    def run(self, args: List[str]) -> None:
        init_uncaught_exception_handler(logger)
        parsed_args: ConnectorCLIArgs = parse_cli_args(
            args,
            with_write=True,
            with_read=False,
        )
        output_messages = self.run_cmd(parsed_args)
        for message in output_messages:
            print(orjson.dumps(AirbyteMessageSerializer.dump(message)).decode())
