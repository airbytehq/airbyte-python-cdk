from typing import Any, List, Mapping

from airbyte_cdk.connector_builder.models import StreamRead
from airbyte_cdk.connector_builder.test_reader import TestReader
from airbyte_cdk.models.airbyte_protocol import (
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
)
from airbyte_cdk.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)


class ManifestRunner:
    _source: ManifestDeclarativeSource

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
        )

        return stream_read
