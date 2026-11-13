import importlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import Mock, call, patch

import pytest
from fastapi.testclient import TestClient
from jsonschema.exceptions import ValidationError

from airbyte_cdk.logger import AirbyteLogFormatter
from airbyte_cdk.manifest_server.api_models import CheckRequest
from airbyte_cdk.manifest_server.app import app
from airbyte_cdk.manifest_server.helpers import tracing
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
        ({"workspace_id": "workspace", "project_id": "project"}, ""),
        ({}, ""),
        (None, ""),
    ],
)
def test_check_logs_and_span_tags(context, prefix, caplog):
    source = Mock()
    source.name = "test"
    source.spec.return_value = ConnectorSpecification(connectionSpecification={"type": "object"})
    source.message_repository = None

    def check(logger, config):
        logging.getLogger("airbyte.test").info("Fetched %s pages", 3)
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    source.check.side_effect = check
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
    assert f"{prefix}Fetched 3 pages" in messages
    assert tracer.current_span.return_value.set_tag.call_args_list == [
        call(key, context[key])
        for key in ["workspace_id", "project_id", "request_id", "connector_name"]
        if context and key in context
    ]
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
        logging.getLogger("airbyte.test").info("Between %s", "requests")
        endpoint(CheckRequest.model_validate({"manifest": {}, "config": {}}))
        logging.getLogger("airbyte.test").info("Outside %s", "request")

    messages = [record.getMessage() for record in caplog.records]
    assert messages[-4:] == [
        "[request_id=first] Inside request",
        "Between requests",
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


@pytest.mark.parametrize(
    "context",
    [None, {"workspace_id": "workspace"}, {"request_id": "abc"}, {"connector_name": "source-foo"}],
)
def test_malformed_log_does_not_fail_check(context, monkeypatch):
    monkeypatch.setattr(logging, "raiseExceptions", False)
    source = Mock()
    source.name = "test"
    source.spec.return_value = ConnectorSpecification(connectionSpecification={"type": "object"})
    source.message_repository = None

    def check(logger, config):
        logging.getLogger("airbyte.test").info("Fetched %s of %s pages", 3)
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    source.check.side_effect = check
    with (
        patch("airbyte_cdk.manifest_server.routers.manifest.build_source", return_value=source),
        TestClient(app) as client,
    ):
        response = client.post(
            "/v1/manifest/check", json={"manifest": {}, "config": {}, "context": context}
        )
    assert response.status_code == 200, response.text
    assert response.json() == {"success": True, "message": None}


@pytest.mark.parametrize("endpoint", ["test_read", "check", "discover", "resolve", "full_resolve"])
def test_all_endpoints_apply_context_before_building(endpoint, caplog):
    context = {
        "workspace_id": "workspace",
        "project_id": "project",
        "request_id": "req",
        "connector_name": "source-foo",
    }

    def build_source(*args, **kwargs):
        logging.getLogger("airbyte.test").info("Building %s", "source")
        raise ValidationError("build failed")

    with (
        patch(
            "airbyte_cdk.manifest_server.routers.manifest.build_source", side_effect=build_source
        ),
        patch("airbyte_cdk.manifest_server.helpers.tracing.ddtrace.tracer") as tracer,
        caplog.at_level(logging.INFO),
        TestClient(app) as client,
    ):
        response = client.post(
            f"/v1/manifest/{endpoint}",
            json={"manifest": {}, "config": {}, "context": context, "stream_name": "products"},
        )
    assert response.status_code == 400, response.text
    assert "[request_id=req connector_name=source-foo] Building source" in [
        record.getMessage() for record in caplog.records
    ]
    assert tracer.current_span.return_value.set_tag.call_args_list == [
        call(key, value) for key, value in context.items()
    ]


@pytest.mark.parametrize("field", ["request_id", "connector_name"])
@pytest.mark.parametrize("value", ["", "a" * 129, "has spaces", "line\nbreak", "injected=tag"])
def test_invalid_log_context_is_rejected(field, value):
    with TestClient(app) as client:
        response = client.post(
            "/v1/manifest/check", json={"manifest": {}, "config": {}, "context": {field: value}}
        )
    assert response.status_code == 422, response.text


def test_record_factory_reload_is_safe(caplog):
    factory = logging.getLogRecordFactory()
    importlib.reload(tracing)
    assert logging.getLogRecordFactory() is factory

    @tracing.with_request_context
    def endpoint(request: CheckRequest) -> None:
        logging.getLogger("airbyte.test").info("After %s", "reload")

    with caplog.at_level(logging.INFO):
        endpoint(
            CheckRequest.model_validate(
                {"manifest": {}, "config": {}, "context": {"request_id": "reload"}}
            )
        )
    assert caplog.records[-1].getMessage() == "[request_id=reload] After reload"


def test_test_read_worker_logs_are_not_prefixed(caplog):
    def read(*args, **kwargs):
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(logging.getLogger("airbyte.test").info, "Worker %s", "message").result()
        raise RuntimeError("read failed")

    with (
        patch("airbyte_cdk.manifest_server.routers.manifest.build_source"),
        patch("airbyte_cdk.manifest_server.routers.manifest.ManifestCommandProcessor") as processor,
        caplog.at_level(logging.INFO),
        TestClient(app) as client,
    ):
        processor.return_value.test_read.side_effect = read
        response = client.post(
            "/v1/manifest/test_read",
            json={
                "manifest": {},
                "config": {},
                "stream_name": "products",
                "context": {"request_id": "req", "connector_name": "source-foo"},
            },
        )
    assert response.status_code == 400, response.text
    assert [
        record.getMessage() for record in caplog.records if "Worker" in record.getMessage()
    ] == ["Worker message"]
