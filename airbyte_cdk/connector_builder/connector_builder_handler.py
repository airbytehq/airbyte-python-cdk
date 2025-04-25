#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar, Dict, List, Mapping

from airbyte_cdk.connector_builder.test_reader import TestReader
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
    Type,
)
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.declarative.declarative_source import DeclarativeSource
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)
from airbyte_cdk.utils.airbyte_secrets_utils import filter_secrets
from airbyte_cdk.utils.datetime_helpers import ab_datetime_now
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

DEFAULT_MAXIMUM_NUMBER_OF_PAGES_PER_SLICE = 5
DEFAULT_MAXIMUM_NUMBER_OF_SLICES = 5
DEFAULT_MAXIMUM_RECORDS = 100
DEFAULT_MAXIMUM_STREAMS = 100

MAX_PAGES_PER_SLICE_KEY = "max_pages_per_slice"
MAX_SLICES_KEY = "max_slices"
MAX_RECORDS_KEY = "max_records"
MAX_STREAMS_KEY = "max_streams"


@dataclass
class TestLimits:
    __test__: ClassVar[bool] = False  # Tell Pytest this is not a Pytest class, despite its name

    max_records: int = field(default=DEFAULT_MAXIMUM_RECORDS)
    max_pages_per_slice: int = field(default=DEFAULT_MAXIMUM_NUMBER_OF_PAGES_PER_SLICE)
    max_slices: int = field(default=DEFAULT_MAXIMUM_NUMBER_OF_SLICES)
    max_streams: int = field(default=DEFAULT_MAXIMUM_STREAMS)


def get_limits(config: Mapping[str, Any]) -> TestLimits:
    command_config = config.get("__test_read_config", {})
    max_pages_per_slice = (
        command_config.get(MAX_PAGES_PER_SLICE_KEY) or DEFAULT_MAXIMUM_NUMBER_OF_PAGES_PER_SLICE
    )
    max_slices = command_config.get(MAX_SLICES_KEY) or DEFAULT_MAXIMUM_NUMBER_OF_SLICES
    max_records = command_config.get(MAX_RECORDS_KEY) or DEFAULT_MAXIMUM_RECORDS
    max_streams = command_config.get(MAX_STREAMS_KEY) or DEFAULT_MAXIMUM_STREAMS
    return TestLimits(max_records, max_pages_per_slice, max_slices, max_streams)


def should_migrate_manifest(config: Mapping[str, Any]) -> bool:
    """
    Determines whether the manifest should be migrated,
    based on the presence of the "__should_migrate" key in the config.

    This flag is set by the UI.
    """
    return config.get("__should_migrate", False)


def should_normalize_manifest(config: Mapping[str, Any]) -> bool:
    """
    Check if the manifest should be normalized.
    :param config: The configuration to check
    :return: True if the manifest should be normalized, False otherwise.
    """
    return config.get("__should_normalize", False)


def create_source(config: Mapping[str, Any], limits: TestLimits) -> ManifestDeclarativeSource:
    manifest = config["__injected_declarative_manifest"]
    return ManifestDeclarativeSource(
        config=config,
        emit_connector_builder_messages=True,
        source_config=manifest,
        migrate_manifest=should_migrate_manifest(config),
        normalize_manifest=should_normalize_manifest(config),
        component_factory=ModelToComponentFactory(
            emit_connector_builder_messages=True,
            limit_pages_fetched_per_slice=limits.max_pages_per_slice,
            limit_slices_fetched=limits.max_slices,
            disable_retries=True,
            disable_cache=True,
        ),
    )


def read_stream(
    source: DeclarativeSource,
    config: Mapping[str, Any],
    configured_catalog: ConfiguredAirbyteCatalog,
    state: List[AirbyteStateMessage],
    limits: TestLimits,
) -> AirbyteMessage:
    try:
        test_read_handler = TestReader(
            limits.max_pages_per_slice, limits.max_slices, limits.max_records
        )
        # The connector builder only supports a single stream
        stream_name = configured_catalog.streams[0].stream.name

        stream_read = test_read_handler.run_test_read(
            source, config, configured_catalog, state, limits.max_records
        )

        return AirbyteMessage(
            type=MessageType.RECORD,
            record=AirbyteRecordMessage(
                data=asdict(stream_read), stream=stream_name, emitted_at=_emitted_at()
            ),
        )
    except Exception as exc:
        error = AirbyteTracedException.from_exception(
            exc,
            message=filter_secrets(
                f"Error reading stream with config={config} and catalog={configured_catalog}: {str(exc)}"
            ),
        )
        return error.as_airbyte_message()


def resolve_manifest(source: ManifestDeclarativeSource) -> AirbyteMessage:
    try:
        return AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(
                data={"manifest": source.resolved_manifest},
                emitted_at=_emitted_at(),
                stream="resolve_manifest",
            ),
        )
    except Exception as exc:
        error = AirbyteTracedException.from_exception(
            exc, message=f"Error resolving manifest: {str(exc)}"
        )
        return error.as_airbyte_message()


def full_resolve_manifest(source: ManifestDeclarativeSource, limits: TestLimits) -> AirbyteMessage:
    try:
        manifest = {**source.resolved_manifest}
        streams = manifest.get("streams", [])
        for stream in streams:
            stream["dynamic_stream_name"] = None

        mapped_streams: Dict[str, List[Dict[str, Any]]] = {}
        for stream in source.dynamic_streams:
            generated_streams = mapped_streams.setdefault(stream["dynamic_stream_name"], [])

            if len(generated_streams) < limits.max_streams:
                generated_streams += [stream]

        for generated_streams_list in mapped_streams.values():
            streams.extend(generated_streams_list)

        manifest["streams"] = streams
        return AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(
                data={"manifest": manifest},
                emitted_at=_emitted_at(),
                stream="full_resolve_manifest",
            ),
        )
    except AirbyteTracedException as exc:
        return exc.as_airbyte_message()
    except Exception as exc:
        error = AirbyteTracedException.from_exception(
            exc, message=f"Error full resolving manifest: {str(exc)}"
        )
        return error.as_airbyte_message()


def _emitted_at() -> int:
    return ab_datetime_now().to_epoch_millis()
