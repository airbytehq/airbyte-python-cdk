#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, Mapping, Optional

from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.types import Record, StreamSlice


class ClientSideIncrementalRetrieverDecorator(Retriever):
    """
    Decorator that wraps a Retriever and applies client-side incremental filtering.

    This decorator filters out records that are older than the cursor state,
    enabling client-side incremental sync for custom retrievers that don't
    natively support the ClientSideIncrementalRecordFilterDecorator.

    When a stream uses `is_client_side_incremental: true` with a custom retriever,
    this decorator ensures that only records newer than the cursor state are emitted.

    Attributes:
        retriever: The underlying retriever to wrap
        cursor: The cursor used to determine if records should be synced
    """

    def __init__(
        self,
        retriever: Retriever,
        cursor: Cursor,
    ):
        self._retriever = retriever
        self._cursor = cursor

    def read_records(
        self,
        records_schema: Mapping[str, Any],
        stream_slice: Optional[StreamSlice] = None,
    ) -> Iterable[StreamData]:
        for record in self._retriever.read_records(
            records_schema=records_schema,
            stream_slice=stream_slice,
        ):
            if isinstance(record, Record):
                if self._cursor.should_be_synced(record):
                    yield record
            elif isinstance(record, Mapping):
                record_obj = Record(data=record, associated_slice=stream_slice, stream_name="")
                if self._cursor.should_be_synced(record_obj):
                    yield record
            else:
                yield record
