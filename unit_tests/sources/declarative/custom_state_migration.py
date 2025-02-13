#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from typing import Any, Mapping

from airbyte_cdk.sources.declarative.declarative_stream import DeclarativeStream
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.migrations.state_migration import StateMigration
from airbyte_cdk.sources.types import Config


class CustomStateMigration(StateMigration):
    declarative_stream: DeclarativeStream
    config: Config

    def __init__(self, declarative_stream: DeclarativeStream, config: Config):
        self._config = config
        self.declarative_stream = declarative_stream
        self._cursor = declarative_stream.incremental_sync
        self._parameters = declarative_stream.parameters
        self._cursor_field = InterpolatedString.create(
            self._cursor.cursor_field, parameters=self._parameters
        ).eval(self._config)

    def should_migrate(self, stream_state: Mapping[str, Any]) -> bool:
        return True

    def migrate(self, stream_state: Mapping[str, Any]) -> Mapping[str, Any]:
        if not self.should_migrate(stream_state):
            return stream_state
        updated_at = stream_state[self._cursor.cursor_field]

        migrated_stream_state = {
            "states": [
                {
                    "partition": {"type": "type_1"},
                    "cursor": {self._cursor.cursor_field: updated_at},
                },
                {
                    "partition": {"type": "type_2"},
                    "cursor": {self._cursor.cursor_field: updated_at},
                },
            ]
        }

        return migrated_stream_state
