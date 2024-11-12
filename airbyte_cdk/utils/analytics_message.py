# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import time
from typing import Any

from airbyte_cdk.models import (
    AirbyteAnalyticsTraceMessage,
    AirbyteMessage,
    AirbyteTraceMessage,
    TraceType,
    Type,
)


def create_analytics_message(type: str, value: Any | None) -> AirbyteMessage:  # noqa: ANN401, A002  (any type, shadows built-in name)
    return AirbyteMessage(
        type=Type.TRACE,
        trace=AirbyteTraceMessage(
            type=TraceType.ANALYTICS,
            emitted_at=time.time() * 1000,
            analytics=AirbyteAnalyticsTraceMessage(
                type=type, value=str(value) if value is not None else None
            ),
        ),
    )
