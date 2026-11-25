import json
import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from airbyte_cdk.utils.outbound_request_filter import BlockKind, DestinationClass

WorkerOperation = Literal["test_read", "full_resolve", "check", "discover"]
Operation = Literal[WorkerOperation, "resolve"]
SECURITY_EVENT_LOGGER_NAME = "airbyte.manifest-server.security-events"
logger = logging.getLogger(SECURITY_EVENT_LOGGER_NAME)
logger.setLevel(logging.INFO)
logger.propagate = False
logger.addHandler(logging.StreamHandler())

_UUID_V4 = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
    re.IGNORECASE,
)


def normalize_builder_request_id(value: str) -> str | None:
    if _UUID_V4.fullmatch(value):
        return value.lower()
    return None


@dataclass(frozen=True)
class OperationContext:
    operation: Operation
    builder_request_id: str | None


_request_context: ContextVar[OperationContext | None] = ContextVar(
    "manifest_server_security_context", default=None
)
_worker_context: OperationContext | None = None


@contextmanager
def bind_request_operation(operation: Operation, builder_request_id: str | None) -> Iterator[None]:
    token = _request_context.set(OperationContext(operation, builder_request_id))
    try:
        yield
    finally:
        _request_context.reset(token)


@contextmanager
def bind_worker_operation(
    operation: WorkerOperation, builder_request_id: str | None
) -> Iterator[None]:
    # Each worker handles one operation; connector threads do not inherit ContextVars.
    global _worker_context
    _worker_context = OperationContext(operation, builder_request_id)
    try:
        yield
    finally:
        _worker_context = None


def log_egress_denied(block_kind: BlockKind, destination_class: DestinationClass) -> None:
    context = _request_context.get() or _worker_context
    if context is None:
        return
    logger.info(
        json.dumps(
            {
                "schema_version": 1,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": "airbyte.builder.egress_denied",
                "builder_request_id": context.builder_request_id,
                "operation": context.operation,
                "outcome": "blocked",
                "block_kind": block_kind,
                "destination_class": destination_class,
            }
        )
    )
