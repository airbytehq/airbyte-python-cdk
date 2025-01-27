#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import traceback
from functools import cache
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

from airbyte_protocol_dataclasses.models import SyncMode

from airbyte_cdk.models import AirbyteLogMessage, AirbyteMessage, Level
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.file_based.config.file_based_stream_config import PrimaryKeyType
from airbyte_cdk.sources.file_based.discovery_policy import AbstractDiscoveryPolicy
from airbyte_cdk.sources.file_based.exceptions import FileBasedErrorsCollector, FileBasedSourceError
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader
from airbyte_cdk.sources.file_based.schema_helpers import remote_file_identity_schema
from airbyte_cdk.sources.file_based.types import StreamSlice
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.checkpoint import Cursor
from airbyte_cdk.sources.streams.core import JsonSchema
from airbyte_cdk.sources.utils.record_helper import stream_data_to_airbyte_message
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

IDENTITIES_STREAM_NAME = "identities"


class IdentitiesStream(Stream):
    """
    The identities stream. A full refresh stream to sync identities from a certain domain.
    The stream reader manage the logic to get such data, which is implemented on connector side.
    """

    is_resumable = False

    def __init__(
        self,
        catalog_schema: Optional[Mapping[str, Any]],
        stream_reader: AbstractFileBasedStreamReader,
        discovery_policy: AbstractDiscoveryPolicy,
        errors_collector: FileBasedErrorsCollector,
    ):
        super().__init__()
        self.catalog_schema = catalog_schema
        self.stream_reader = stream_reader
        self._discovery_policy = discovery_policy
        self.errors_collector = errors_collector
        self._cursor: MutableMapping[str, Any] = {}

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._cursor

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        """State setter, accept state serialized by state getter."""
        self._cursor = value

    @property
    def primary_key(self) -> PrimaryKeyType:
        return None

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[StreamSlice] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any] | AirbyteMessage]:
        try:
            identity_groups = self.stream_reader.load_identity_groups(logger=self.logger)
            for record in identity_groups:
                yield stream_data_to_airbyte_message(self.name, record)
        except AirbyteTracedException as exc:
            # Re-raise the exception to stop the whole sync immediately as this is a fatal error
            raise exc
        except Exception:
            yield AirbyteMessage(
                type=MessageType.LOG,
                log=AirbyteLogMessage(
                    level=Level.ERROR,
                    message=f"{FileBasedSourceError.ERROR_PARSING_RECORD.value} stream={self.name}",
                    stack_trace=traceback.format_exc(),
                ),
            )

    @cache
    def get_json_schema(self) -> JsonSchema:
        return remote_file_identity_schema

    @property
    def name(self) -> str:
        return IDENTITIES_STREAM_NAME

    def get_cursor(self) -> Optional[Cursor]:
        return None
