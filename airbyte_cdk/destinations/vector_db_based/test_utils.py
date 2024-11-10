#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import json
import unittest
from typing import Any

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)


class BaseIntegrationTest(unittest.TestCase):
    """BaseIntegrationTest is a base class for integration tests for vector db destinations.

    It provides helper methods to create Airbyte catalogs, records and state messages.
    """

    def _get_configured_catalog(
        self, destination_mode: DestinationSyncMode
    ) -> ConfiguredAirbyteCatalog:
        stream_schema = {
            "type": "object",
            "properties": {"str_col": {"type": "str"}, "int_col": {"type": "integer"}},
        }

        overwrite_stream = ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="mystream",
                json_schema=stream_schema,
                supported_sync_modes=[SyncMode.incremental, SyncMode.full_refresh],
            ),
            primary_key=[["int_col"]],
            sync_mode=SyncMode.incremental,
            destination_sync_mode=destination_mode,
        )

        return ConfiguredAirbyteCatalog(streams=[overwrite_stream])

    def _state(self, data: dict[str, Any]) -> AirbyteMessage:
        return AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(data=data))

    def _record(self, stream: str, str_value: str, int_value: int) -> AirbyteMessage:
        return AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(
                stream=stream, data={"str_col": str_value, "int_col": int_value}, emitted_at=0
            ),
        )

    def setUp(self) -> None:
        with open("secrets/config.json") as f:
            self.config = json.loads(f.read())
