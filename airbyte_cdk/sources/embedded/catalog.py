#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from airbyte_cdk.sources.embedded.tools import get_first


def get_stream(catalog: AirbyteCatalog, stream_name: str) -> AirbyteStream | None:
    return get_first(catalog.streams, lambda s: s.name == stream_name)


def get_stream_names(catalog: AirbyteCatalog) -> list[str]:
    return [stream.name for stream in catalog.streams]


def to_configured_stream(
    stream: AirbyteStream,
    sync_mode: SyncMode = SyncMode.full_refresh,
    destination_sync_mode: DestinationSyncMode = DestinationSyncMode.append,
    cursor_field: list[str] | None = None,
    primary_key: list[list[str]] | None = None,
) -> ConfiguredAirbyteStream:
    return ConfiguredAirbyteStream(
        stream=stream,
        sync_mode=sync_mode,
        destination_sync_mode=destination_sync_mode,
        cursor_field=cursor_field,
        primary_key=primary_key,
    )


def to_configured_catalog(
    configured_streams: list[ConfiguredAirbyteStream],
) -> ConfiguredAirbyteCatalog:
    return ConfiguredAirbyteCatalog(streams=configured_streams)


def create_configured_catalog(
    stream: AirbyteStream, sync_mode: SyncMode = SyncMode.full_refresh
) -> ConfiguredAirbyteCatalog:
    configured_streams = [
        to_configured_stream(
            stream, sync_mode=sync_mode, primary_key=stream.source_defined_primary_key
        )
    ]

    return to_configured_catalog(configured_streams)
