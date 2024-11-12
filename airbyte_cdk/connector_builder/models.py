#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class HttpResponse:
    status: int
    body: str | None = None
    headers: dict[str, Any] | None = None


@dataclass
class HttpRequest:
    url: str
    headers: dict[str, Any] | None
    http_method: str
    body: str | None = None


@dataclass
class StreamReadPages:
    records: list[object]
    request: HttpRequest | None = None
    response: HttpResponse | None = None


@dataclass
class StreamReadSlices:
    pages: list[StreamReadPages]
    slice_descriptor: dict[str, Any] | None
    state: list[dict[str, Any]] | None = None


@dataclass
class LogMessage:
    message: str
    level: str
    internal_message: str | None = None
    stacktrace: str | None = None


@dataclass
class AuxiliaryRequest:
    title: str
    description: str
    request: HttpRequest
    response: HttpResponse


@dataclass
class StreamRead:
    logs: list[LogMessage]
    slices: list[StreamReadSlices]
    test_read_limit_reached: bool
    auxiliary_requests: list[AuxiliaryRequest]
    inferred_schema: dict[str, Any] | None
    inferred_datetime_formats: dict[str, str] | None
    latest_config_update: dict[str, Any] | None


@dataclass
class StreamReadRequestBody:
    manifest: dict[str, Any]
    stream: str
    config: dict[str, Any]
    state: dict[str, Any] | None
    record_limit: int | None
