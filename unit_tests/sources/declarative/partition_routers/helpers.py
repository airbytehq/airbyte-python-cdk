#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, Optional, Union

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.declarative.declarative_stream import DeclarativeStream
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.streams.checkpoint import Cursor
from airbyte_cdk.sources.types import Record, StreamSlice


class MockStream(DeclarativeStream):
    def __init__(self, slices, records, name, cursor_field="", cursor=None):
        self.config = {}
        self._slices = slices
        self._records = records
        self._stream_cursor_field = (
            InterpolatedString.create(cursor_field, parameters={})
            if isinstance(cursor_field, str)
            else cursor_field
        )
        self._name = name
        self._state = {"states": []}
        self._cursor = cursor

    @property
    def name(self) -> str:
        return self._name

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return "id"

    @property
    def state(self) -> Mapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: Mapping[str, Any]) -> None:
        self._state = value

    @property
    def is_resumable(self) -> bool:
        return bool(self._cursor)

    def get_cursor(self) -> Optional[Cursor]:
        return self._cursor

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Optional[StreamSlice]]:
        for s in self._slices:
            if isinstance(s, StreamSlice):
                yield s
            else:
                yield StreamSlice(partition=s, cursor_slice={})

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        # The parent stream's records should always be read as full refresh
        assert sync_mode == SyncMode.full_refresh

        if not stream_slice:
            result = self._records
        else:
            result = [
                Record(data=r, associated_slice=stream_slice, stream_name=self.name)
                for r in self._records
                if r["slice"] == stream_slice["slice"]
            ]

        yield from result

        # Update the state only after reading the full slice
        cursor_field = self._stream_cursor_field.eval(config=self.config)
        if stream_slice and cursor_field and result:
            self._state["states"].append(
                {cursor_field: result[-1][cursor_field], "partition": stream_slice["slice"]}
            )

    def get_json_schema(self) -> Mapping[str, Any]:
        return {}
