from typing import Any, List, Mapping, Optional

from airbyte_protocol_dataclasses.models import AirbyteStateMessage

from airbyte_cdk.models import (
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
    TestLimits,
)
from airbyte_cdk.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)

SHOULD_NORMALIZE_KEY = "__should_normalize"
SHOULD_MIGRATE_KEY = "__should_migrate"


def build_catalog(stream_name: str) -> ConfiguredAirbyteCatalog:
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=stream_name,
                    json_schema={},
                    supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
                ),
                sync_mode=SyncMode.incremental,
                destination_sync_mode=DestinationSyncMode.overwrite,
            )
        ]
    )


def should_migrate_manifest(manifest: Mapping[str, Any]) -> bool:
    """
    Determines whether the manifest should be migrated,
    based on the presence of the "__should_migrate" key.

    This flag is set by the UI.
    """
    return manifest.get(SHOULD_MIGRATE_KEY, False)


def should_normalize_manifest(manifest: Mapping[str, Any]) -> bool:
    """
    Determines whether the manifest should be normalized,
    based on the presence of the "__should_normalize" key.

    This flag is set by the UI.
    """
    return manifest.get(SHOULD_NORMALIZE_KEY, False)


def build_source(
    manifest: Mapping[str, Any],
    catalog: Optional[ConfiguredAirbyteCatalog],
    config: Mapping[str, Any],
    state: Optional[List[AirbyteStateMessage]],
    record_limit: Optional[int] = None,
    page_limit: Optional[int] = None,
    slice_limit: Optional[int] = None,
) -> ConcurrentDeclarativeSource[Optional[List[AirbyteStateMessage]]]:
    # We enforce a concurrency level of 1 so that the stream is processed on a single thread
    # to retain ordering for the grouping of the builder message responses.
    if "concurrency_level" in manifest:
        manifest["concurrency_level"]["default_concurrency"] = 1
    else:
        manifest["concurrency_level"] = {"type": "ConcurrencyLevel", "default_concurrency": 1}

    return ConcurrentDeclarativeSource(
        catalog=catalog,
        state=state,
        source_config=manifest,
        config=config,
        normalize_manifest=should_normalize_manifest(manifest),
        migrate_manifest=should_migrate_manifest(manifest),
        emit_connector_builder_messages=True,
        limits=TestLimits(
            max_pages_per_slice=page_limit,
            max_slices=slice_limit,
            max_records=record_limit,
        ),
    )
