#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from dataclasses import asdict, dataclass, field
from typing import Any, List, Mapping

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

MAX_PAGES_PER_SLICE_KEY = "max_pages_per_slice"
MAX_SLICES_KEY = "max_slices"
MAX_RECORDS_KEY = "max_records"


@dataclass
class TestReadLimits:
    max_records: int = field(default=DEFAULT_MAXIMUM_RECORDS)
    max_pages_per_slice: int = field(default=DEFAULT_MAXIMUM_NUMBER_OF_PAGES_PER_SLICE)
    max_slices: int = field(default=DEFAULT_MAXIMUM_NUMBER_OF_SLICES)


def get_limits(config: Mapping[str, Any]) -> TestReadLimits:
    command_config = config.get("__test_read_config", {})
    max_pages_per_slice = (
        command_config.get(MAX_PAGES_PER_SLICE_KEY) or DEFAULT_MAXIMUM_NUMBER_OF_PAGES_PER_SLICE
    )
    max_slices = command_config.get(MAX_SLICES_KEY) or DEFAULT_MAXIMUM_NUMBER_OF_SLICES
    max_records = command_config.get(MAX_RECORDS_KEY) or DEFAULT_MAXIMUM_RECORDS
    return TestReadLimits(max_records, max_pages_per_slice, max_slices)


def create_source(config: Mapping[str, Any], limits: TestReadLimits) -> ManifestDeclarativeSource:
    manifest = config["__injected_declarative_manifest"]
    return ManifestDeclarativeSource(
        config=config,
        emit_connector_builder_messages=True,
        source_config=manifest,
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
    limits: TestReadLimits,
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


def _emitted_at() -> int:
    return ab_datetime_now().to_epoch_millis()
