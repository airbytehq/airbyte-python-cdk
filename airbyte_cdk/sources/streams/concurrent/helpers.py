# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from airbyte_cdk.sources.streams import Stream


def get_primary_key_from_stream(
    stream_primary_key: str | list[str] | list[list[str]] | None,
) -> list[str]:
    if stream_primary_key is None:
        return []
    if isinstance(stream_primary_key, str):
        return [stream_primary_key]
    if isinstance(stream_primary_key, list):
        if len(stream_primary_key) > 0 and all(isinstance(k, str) for k in stream_primary_key):
            return stream_primary_key  # type: ignore # We verified all items in the list are strings
        raise ValueError(f"Nested primary keys are not supported. Found {stream_primary_key}")
    raise ValueError(f"Invalid type for primary key: {stream_primary_key}")


def get_cursor_field_from_stream(stream: Stream) -> str | None:
    if isinstance(stream.cursor_field, list):
        if len(stream.cursor_field) > 1:
            raise ValueError(
                f"Nested cursor fields are not supported. Got {stream.cursor_field} for {stream.name}"
            )
        if len(stream.cursor_field) == 0:
            return None
        return stream.cursor_field[0]
    return stream.cursor_field
