import logging
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from airbyte_protocol_dataclasses.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    Status,
    TraceType,
)
from airbyte_protocol_dataclasses.models import Type as AirbyteMessageType

from airbyte_cdk.connector_builder.models import StreamRead
from airbyte_cdk.connector_builder.test_reader import TestReader
from airbyte_cdk.entrypoint import AirbyteEntrypoint
from airbyte_cdk.models import (
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
)
from airbyte_cdk.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)


class ManifestCommandProcessor:
    _source: ManifestDeclarativeSource
    _logger = logging.getLogger("airbyte.manifest-runner")

    def __init__(self, source: ManifestDeclarativeSource) -> None:
        self._source = source

    def test_read(
        self,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: List[AirbyteStateMessage],
        record_limit: int,
        page_limit: int,
        slice_limit: int,
    ) -> StreamRead:
        """
        Test the read method of the source.
        """

        test_read_handler = TestReader(
            max_pages_per_slice=page_limit,
            max_slices=slice_limit,
            max_record_limit=record_limit,
        )

        stream_read = test_read_handler.run_test_read(
            source=self._source,
            config=config,
            configured_catalog=catalog,
            state=state,
            stream_name=catalog.streams[0].stream.name,
            record_limit=record_limit,
        )

        return stream_read

    def check_connection(
        self,
        config: Mapping[str, Any],
    ) -> Tuple[bool, str]:
        """
        Check the connection to the source.
        """

        spec = self._source.spec(self._logger)
        messages = AirbyteEntrypoint(source=self._source).check(spec, config)
        messages_by_type = self._get_messages_by_type(messages)
        self._raise_on_trace_message(messages_by_type)
        connection_status = self._get_connection_status(messages_by_type)

        if connection_status:
            return connection_status.status == Status.SUCCEEDED, connection_status.message
        return False, "Connection check failed"

    def discover(
        self,
        config: Mapping[str, Any],
    ) -> Optional[AirbyteCatalog]:
        """
        Discover the catalog from the source.
        """
        spec = self._source.spec(self._logger)
        messages = AirbyteEntrypoint(source=self._source).discover(spec, config)
        messages_by_type = self._get_messages_by_type(messages)
        self._raise_on_trace_message(messages_by_type)
        return self._get_catalog(messages_by_type)

    def _get_messages_by_type(
        self,
        messages: Iterable[AirbyteMessage],
    ) -> Dict[str, List[AirbyteMessage]]:
        """
        Group messages by type.
        """
        grouped: Dict[str, List[AirbyteMessage]] = {}
        for message in messages:
            msg_type = message.type
            if msg_type not in grouped:
                grouped[msg_type] = []
            grouped[msg_type].append(message)
        return grouped

    def _get_connection_status(
        self,
        messages_by_type: Mapping[str, List[AirbyteMessage]],
    ) -> Optional[AirbyteConnectionStatus]:
        """
        Get the connection status from the messages.
        """
        messages = messages_by_type.get(AirbyteMessageType.CONNECTION_STATUS)
        return messages[-1].connectionStatus if messages else None

    def _get_catalog(
        self,
        messages_by_type: Mapping[str, List[AirbyteMessage]],
    ) -> Optional[AirbyteCatalog]:
        """
        Get the catalog from the messages.
        """
        messages = messages_by_type.get(AirbyteMessageType.CATALOG)
        return messages[-1].catalog if messages else None

    def _raise_on_trace_message(
        self,
        messages_by_type: Mapping[str, List[AirbyteMessage]],
    ) -> None:
        """
        Raise an exception if a trace message is found.
        """
        messages = [
            message
            for message in messages_by_type.get(AirbyteMessageType.TRACE, [])
            if message.trace.type == TraceType.ERROR
        ]
        if messages:
            # TODO: raise a better exception
            raise Exception(messages[-1].trace.error.message)
