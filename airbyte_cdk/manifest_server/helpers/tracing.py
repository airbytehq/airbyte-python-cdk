import logging
from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, Union

import ddtrace

from ..api_models import (
    CheckRequest,
    DiscoverRequest,
    FullResolveRequest,
    ResolveRequest,
    StreamTestReadRequest,
)

logger = logging.getLogger(__name__)
_log_context: ContextVar[str] = ContextVar("manifest_server_log_context", default="")


def _install_record_factory() -> None:
    previous_factory = logging.getLogRecordFactory()
    if getattr(previous_factory, "_manifest_server_log_context", False):
        return

    def record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = previous_factory(*args, **kwargs)
        context = _log_context.get()
        if context:
            try:
                message = record.getMessage()
            except Exception:
                return record
            record.msg = f"[{context}] {message}"
            record.args = ()
        return record

    setattr(record_factory, "_manifest_server_log_context", True)
    logging.setLogRecordFactory(record_factory)


_install_record_factory()

RequestT = TypeVar(
    "RequestT",
    bound=Union[
        CheckRequest, DiscoverRequest, FullResolveRequest, ResolveRequest, StreamTestReadRequest
    ],
)
ResponseT = TypeVar("ResponseT")


def with_request_context(
    endpoint: Callable[[RequestT], ResponseT],
) -> Callable[[RequestT], ResponseT]:
    """Scope logs for sync endpoints with a single body parameter named `request`.

    Drives a process-wide LogRecord factory that prefixes records only in this context.
    """

    @wraps(endpoint)
    def wrapped(request: RequestT) -> ResponseT:
        context = request.context
        log_parts = []
        if context:
            if context.request_id is not None:
                log_parts.append(f"request_id={context.request_id}")
            if context.connector_name is not None:
                log_parts.append(f"connector_name={context.connector_name}")
        token = _log_context.set(" ".join(log_parts))
        try:
            if context:
                apply_trace_tags_from_context(**context.model_dump())
            return endpoint(request)
        finally:
            _log_context.reset(token)

    return wrapped


def apply_trace_tags_from_context(
    workspace_id: Optional[str] = None,
    project_id: Optional[str] = None,
    request_id: Optional[str] = None,
    connector_name: Optional[str] = None,
) -> None:
    """Apply trace tags from context to the current span."""
    if not workspace_id and not project_id and request_id is None and connector_name is None:
        return

    # Log the trace IDs for observability
    log_parts = []
    if workspace_id:
        log_parts.append(f"workspace_id={workspace_id}")
    if project_id:
        log_parts.append(f"project_id={project_id}")
    if log_parts:
        logger.info(f"Processing request with trace tags: {', '.join(log_parts)}")

    try:
        span = ddtrace.tracer.current_span()
        if span:
            if workspace_id:
                span.set_tag("workspace_id", workspace_id)
            if project_id:
                span.set_tag("project_id", project_id)
            if request_id is not None:
                span.set_tag("request_id", request_id)
            if connector_name is not None:
                span.set_tag("connector_name", connector_name)
    except Exception:
        # Silently ignore any ddtrace-related errors (e.g. if ddtrace.auto wasn't run)
        pass
