#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import threading
from dataclasses import InitVar, dataclass
from typing import Any, Iterable, Mapping, Optional, Union

from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor
from airbyte_cdk.sources.types import Config, Record, StreamSlice, StreamState


@dataclass
class RecordFilter:
    """
    Filter applied on a list of Records

    config (Config): The user-provided configuration as specified by the source's spec
    condition (str): The string representing the predicate to filter a record. Records will be removed if evaluated to False
    """

    parameters: InitVar[Mapping[str, Any]]
    config: Config
    condition: str = ""

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._filter_interpolator = InterpolatedBoolean(
            condition=self.condition, parameters=parameters
        )

    def filter_records(
        self,
        records: Iterable[Mapping[str, Any]],
        stream_state: StreamState,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        kwargs = {
            "stream_state": stream_state,
            "stream_slice": stream_slice,
            "next_page_token": next_page_token,
            "stream_slice.extra_fields": stream_slice.extra_fields if stream_slice else {},
        }
        for record in records:
            if self._filter_interpolator.eval(self.config, record=record, **kwargs):
                yield record


class ClientSideIncrementalRecordFilterDecorator(RecordFilter):
    """
    Applies a filter to a list of records to exclude those that are older than the stream_state/start_date.

    :param Cursor cursor: Cursor used to filter out values
    :param PerPartitionCursor per_partition_cursor: Optional Cursor used for mapping cursor value in nested stream_state
    """

    def __init__(
        self,
        cursor: Union[Cursor],
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._cursor = cursor
        # One retriever (and hence one record filter) may be shared across partitions that are read
        # concurrently, so per-page bookkeeping must be tracked per thread
        self._thread_local = threading.local()

    @property
    def stale_record_seen_on_current_page(self) -> bool:
        """
        Whether the page being filtered contained at least one record the cursor considers already
        synced. Used by `FilterAwareStopCondition` to stop paginating on data feed streams, since
        such records never reach the paginator. Reset on every `filter_records` call.
        """
        return getattr(self._thread_local, "stale_record_seen", False)

    def filter_records(
        self,
        records: Iterable[Mapping[str, Any]],
        stream_state: StreamState,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        self._thread_local.stale_record_seen = False
        records = (record for record in records if self._should_be_synced(record, stream_slice))
        if self.condition:
            records = super().filter_records(
                records=records,
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            )
        return records

    def _should_be_synced(
        self, record: Mapping[str, Any], stream_slice: Optional[StreamSlice]
    ) -> bool:
        if self._cursor.should_be_synced(
            # Record is created on the fly to align with cursors interface; stream name is ignored as we don't need it here
            # Record stream name is empty because it is not used during the filtering
            Record(data=record, associated_slice=stream_slice, stream_name="")
        ):
            return True
        self._thread_local.stale_record_seen = True
        return False
