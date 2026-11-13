import json
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from airbyte_cdk.logger import AirbyteLogFormatter
from airbyte_cdk.manifest_server.api_models import CheckRequest
from airbyte_cdk.manifest_server.app import app
from airbyte_cdk.manifest_server.helpers.tracing import with_request_context
from airbyte_cdk.models import AirbyteConnectionStatus, ConnectorSpecification, Status


@pytest.mark.parametrize(
    "context,prefix",
    [
        (
            {"request_id": "abc", "connector_name": "source-foo"},
            "[request_id=abc connector_name=source-foo] ",
        ),
        ({"request_id": "abc"}, "[request_id=abc] "),
        ({"connector_name": "source-foo"}, "[connector_name=source-foo] "),
        ({"request_id": "", "connector_name": ""}, "[request_id= connector_name=] "),
        ({"workspace_id": "workspace", "project_id": "project"}, ""),
        ({}, ""),
        (None, ""),
    ],
)
def test_check_logs_and_span_tags(context, prefix, caplog):
    source = Mock()
    source.name = "test"
    source.spec.return_value = ConnectorSpecification(connectionSpecification={"type": "object"})
    source.check.return_value = AirbyteConnectionStatus(status=Status.SUCCEEDED)
    source.message_repository = None
    payload = {"manifest": {}, "config": {}}
    if context is not None:
        payload["context"] = context

    with (
        patch("airbyte_cdk.manifest_server.routers.manifest.build_source", return_value=source),
        patch("airbyte_cdk.manifest_server.helpers.tracing.ddtrace.tracer") as tracer,
        caplog.at_level(logging.INFO),
        TestClient(app) as client,
    ):
        response = client.post("/v1/manifest/check", json=payload)

    assert response.status_code == 200, response.text
    assert response.json() == {"success": True, "message": None}
    formatter = AirbyteLogFormatter()
    messages = [json.loads(formatter.format(record))["log"]["message"] for record in caplog.records]
    assert f"{prefix}Check succeeded" in messages
    memory_logs = [message for message in messages if "MemoryMonitor instantiated" in message]
    assert len(memory_logs) == 1, messages
    assert memory_logs[0].startswith(f"{prefix}MemoryMonitor instantiated"), memory_logs
    for key, value in (context or {}).items():
        tracer.current_span.return_value.set_tag.assert_any_call(key, value)
    if not context:
        tracer.current_span.assert_not_called()


@pytest.mark.parametrize("fail", [False, True])
def test_context_reset_on_same_thread(fail, caplog):
    @with_request_context
    def endpoint(request: CheckRequest) -> None:
        logging.getLogger("airbyte.test").info("Inside %s", "request")
        if fail and request.context:
            raise RuntimeError("failed")

    with caplog.at_level(logging.INFO):
        request = CheckRequest.model_validate(
            {"manifest": {}, "config": {}, "context": {"request_id": "first"}}
        )
        if fail:
            with pytest.raises(RuntimeError, match="failed"):
                endpoint(request)
        else:
            endpoint(request)
        endpoint(CheckRequest.model_validate({"manifest": {}, "config": {}}))
        logging.getLogger("airbyte.test").info("Outside %s", "request")

    messages = [record.getMessage() for record in caplog.records]
    assert messages[-3:] == [
        "[request_id=first] Inside request",
        "Inside request",
        "Outside request",
    ]
    formatter = AirbyteLogFormatter()
    baseline = logging.LogRecord(
        "airbyte.test", logging.INFO, "", 0, "Outside %s", ("request",), None
    )
    assert formatter.format(caplog.records[-1]) == formatter.format(baseline)


def test_concurrent_requests_are_isolated(caplog):
    barrier = Barrier(2)

    @with_request_context
    def endpoint(request: CheckRequest) -> None:
        barrier.wait(timeout=10)
        logging.getLogger("airbyte.test").info(f"Handling {request.context.request_id}")

    with caplog.at_level(logging.INFO), ThreadPoolExecutor(max_workers=2) as executor:
        list(
            executor.map(
                endpoint,
                [
                    CheckRequest.model_validate(
                        {"manifest": {}, "config": {}, "context": {"request_id": request_id}}
                    )
                    for request_id in ["first", "second"]
                ],
            )
        )
    messages = [
        record.getMessage() for record in caplog.records if "Handling" in record.getMessage()
    ]
    assert sorted(messages) == [
        "[request_id=first] Handling first",
        "[request_id=second] Handling second",
    ]
