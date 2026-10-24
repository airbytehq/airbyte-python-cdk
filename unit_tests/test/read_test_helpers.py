# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Message builders shared by the `read` Standard Test unit tests."""

from __future__ import annotations

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateStats,
    AirbyteStateType,
    AirbyteStreamState,
    StreamDescriptor,
    Type,
)


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
