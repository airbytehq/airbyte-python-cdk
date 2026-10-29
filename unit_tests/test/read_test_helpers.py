# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Message builders shared by the `read` Standard Test unit tests."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateStats,
    AirbyteStateType,
    AirbyteStreamState,
    AirbyteStreamStatus,
    StreamDescriptor,
    Type,
)
from airbyte_cdk.utils.stream_status_utils import as_airbyte_message


def final_state(
    stream: str, *, namespace: str | None = None, record_count: float | None = None
) -> AirbyteMessage:
    """The final state the CDK emits once a full-refresh stream completes.

    With `record_count`, the message carries `sourceStats`, as the CDK entrypoint adds them;
    without it, the entrypoint (when the message goes through one) sets them.
    """
    return AirbyteMessage(
        type=Type.STATE,
        state=AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name=stream, namespace=namespace),
                stream_state=AirbyteStateBlob(__ab_no_cursor_state_message=True),
            ),
            sourceStats=(
                AirbyteStateStats(recordCount=record_count) if record_count is not None else None
            ),
        ),
    )


def with_stream_statuses(
    stream: str, messages: Iterable[AirbyteMessage], *, namespace: str | None = None
) -> Iterator[AirbyteMessage]:
    """Wrap a stream's messages in the `STARTED`, `RUNNING` and `COMPLETE` statuses the CDK emits."""
    descriptor = StreamDescriptor(name=stream, namespace=namespace)
    yield as_airbyte_message(descriptor, AirbyteStreamStatus.STARTED)
    yield as_airbyte_message(descriptor, AirbyteStreamStatus.RUNNING)
    yield from messages
    yield as_airbyte_message(descriptor, AirbyteStreamStatus.COMPLETE)
