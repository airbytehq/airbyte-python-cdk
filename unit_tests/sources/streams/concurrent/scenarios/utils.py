#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from typing import Any, List, Optional, Tuple, Union
from collections.abc import Iterable, Mapping

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import StreamData


class MockStream(Stream):
    def __init__(
        self,
        slices_and_records_or_exception: Iterable[
            tuple[Mapping[str, Any] | None, Iterable[Exception | Mapping[str, Any]]]
        ],
        name,
        json_schema,
        primary_key=None,
        cursor_field=None,
    ):
        self._slices_and_records_or_exception = slices_and_records_or_exception
        self._name = name
        self._json_schema = json_schema
        self._primary_key = primary_key
        self._cursor_field = cursor_field

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[StreamData]:
        for _slice, records_or_exception in self._slices_and_records_or_exception:
            if stream_slice == _slice:
                for item in records_or_exception:
                    if isinstance(item, Exception):
                        raise item
                    yield item

    @property
    def primary_key(self) -> str | list[str] | list[list[str]] | None:
        return self._primary_key

    @property
    def name(self) -> str:
        return self._name

    @property
    def cursor_field(self) -> str | list[str]:
        return self._cursor_field or []

    def get_json_schema(self) -> Mapping[str, Any]:
        return self._json_schema

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping[str, Any] | None]:
        if self._slices_and_records_or_exception:
            yield from [
                _slice for _slice, records_or_exception in self._slices_and_records_or_exception
            ]
        else:
            yield None
