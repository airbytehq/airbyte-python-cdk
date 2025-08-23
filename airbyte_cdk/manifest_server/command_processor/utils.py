from typing import Any, Mapping, Optional

from airbyte_cdk.models import (
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
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
    config: Mapping[str, Any],
    page_limit: Optional[int] = None,
    slice_limit: Optional[int] = None,
) -> ManifestDeclarativeSource:
    return ManifestDeclarativeSource(
        source_config=manifest,
        config=config,
        normalize_manifest=should_normalize_manifest(manifest),
        migrate_manifest=should_migrate_manifest(manifest),
        emit_connector_builder_messages=True,
        component_factory=ModelToComponentFactory(
            emit_connector_builder_messages=True,
            limit_pages_fetched_per_slice=page_limit,
            limit_slices_fetched=slice_limit,
            disable_retries=True,
            disable_cache=True,
        ),
    )
