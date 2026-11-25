import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from threading import BoundedSemaphore

import pytest
from fastapi.testclient import TestClient

from airbyte_cdk.manifest_server import execution
from airbyte_cdk.manifest_server.app import app

client = TestClient(app)
MANIFEST = {
    "version": "6.0.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": []},
    "streams": [],
}
BUILDER_REQUEST_ID = "d31133b1-01b2-4553-a167-1921c331fc21"
TEST_READ_MANIFEST = {
    **MANIFEST,
    "check": {"type": "CheckStream", "stream_names": ["items"]},
    "spec": {
        "type": "Spec",
        "connection_specification": {"type": "object", "properties": {}},
    },
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "items",
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
            },
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url": "https://fixture.example/synthetic-input-marker",
                    "http_method": "GET",
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
            },
        }
    ],
}
TEST_READ_WORKER = """
import os
import socket
import sys
from threading import current_thread, main_thread
from unittest.mock import patch

from requests import Response, Session

from airbyte_cdk.utils import outbound_request_filter


def resolve(host, port, *, flags):
    assert host == "fixture.example"
    assert current_thread() is not main_thread()
    return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.0.9", 443))]


def send(session, request, **kwargs):
    response = Response()
    response.status_code = 200
    response.url = request.url
    response.request = request
    response._content = b'[{"id": 1}]'
    return response


blocked = bool(os.getenv("AIRBYTE_SSRF_DENY_CIDRS"))
with (
    patch.object(
        Session,
        "send",
        autospec=True,
        side_effect=AssertionError("Blocked send ran") if blocked else send,
    ) as send_mock,
    patch.object(outbound_request_filter, "_resolve_cname_chain", return_value=()),
    patch.object(outbound_request_filter.socket, "getaddrinfo", side_effect=resolve) as resolver,
):
    from airbyte_cdk.manifest_server.operation_worker import main

    main(sys.argv[1], int(sys.argv[2]), float(sys.argv[3]))
    resolver.assert_called_once()
    if blocked:
        send_mock.assert_not_called()
    else:
        send_mock.assert_called_once()
"""


def test_full_resolve_uses_supervised_worker():
    response = client.post("/v1/manifest/full_resolve", json={"manifest": MANIFEST, "config": {}})

    assert response.status_code == 200, response.json()
    assert response.json() == {"manifest": MANIFEST}


@pytest.mark.parametrize(
    "blocked,request_id",
    [(True, BUILDER_REQUEST_ID), (True, None), (False, BUILDER_REQUEST_ID)],
    ids=["correlated-deny", "uncorrelated-deny", "allowed"],
)
def test_real_test_read_worker_correlates_policy_denies(monkeypatch, capfd, blocked, request_id):
    process_factory = subprocess.Popen
    worker_inputs = []

    def start_worker(command, **kwargs):
        assert command[1:3] == ["-m", "airbyte_cdk.manifest_server.operation_worker"]
        assert all(BUILDER_REQUEST_ID not in value for value in command)
        assert all(BUILDER_REQUEST_ID not in value for value in kwargs["env"].values())
        stdin = kwargs["stdin"]
        worker_inputs.append(execution.WorkerInput.read(stdin))
        stdin.seek(0)
        return process_factory([command[0], "-c", TEST_READ_WORKER, *command[3:]], **kwargs)

    monkeypatch.setattr(execution.subprocess, "Popen", start_worker)
    monkeypatch.setenv("AIRBYTE_EDITION", "cloud")
    monkeypatch.setenv("AIRBYTE_SSRF_DENY_CIDRS", "192.0.0.9/32" if blocked else "")
    monkeypatch.delenv("AIRBYTE_SSRF_DENY_HOSTS", raising=False)
    monkeypatch.delenv("AIRBYTE_SSRF_DENY_HOST_SUFFIXES", raising=False)
    body = {
        "manifest": TEST_READ_MANIFEST,
        "config": {},
        "stream_name": "items",
        "context": {"request_id": "forged-body-id"},
    }
    headers = {"X-Airbyte-Builder-Request-Id": request_id} if request_id else {}

    response = client.post("/v1/manifest/test_read", json=body, headers=headers)

    stderr = capfd.readouterr().err
    assert response.status_code == 200, (response.json(), stderr)
    assert BUILDER_REQUEST_ID not in response.text
    assert "X-Airbyte-Builder-Request-Id" not in response.headers
    (worker_input,) = worker_inputs
    assert worker_input.builder_request_id == request_id
    assert json.loads(worker_input.payload)["context"]["request_id"] == "forged-body-id"
    assert BUILDER_REQUEST_ID.encode() not in worker_input.payload
    result = response.json()
    event_lines = [line for line in stderr.splitlines() if "airbyte.builder.egress_denied" in line]
    if not blocked:
        assert not event_lines
        assert result["slices"][0]["pages"][0]["records"] == [{"id": 1}]
        return

    assert result["slices"] == []
    assert any(
        log["level"] == "ERROR" and "not a permitted public API endpoint" in log["message"]
        for log in result["logs"]
    ), result["logs"]
    (event_line,) = event_lines
    event = json.loads(event_line)
    assert datetime.fromisoformat(event.pop("timestamp")).utcoffset() == timezone.utc.utcoffset(
        None
    )
    assert event == {
        "schema_version": 1,
        "event": "airbyte.builder.egress_denied",
        "builder_request_id": request_id,
        "operation": "test_read",
        "outcome": "blocked",
        "block_kind": "address",
        "destination_class": "restricted_network",
    }
    assert "synthetic-input-marker" not in event_line
    assert "forged-body-id" not in event_line


@pytest.mark.parametrize("wire", [b"null\n12345", b"null", b'"not-a-uuid"\n12'])
def test_worker_input_rejects_invalid_or_oversized_envelopes(monkeypatch, wire):
    monkeypatch.setattr(execution, "_MAX_MESSAGE_BYTES", 4)
    with pytest.raises(ValueError, match="Manifest worker"):
        execution.WorkerInput.read(BytesIO(wire))


def test_deadline_reaps_worker_cleans_scratch_and_releases_slot(monkeypatch):
    process_factory = subprocess.Popen
    workers = []

    def start_worker(command, **kwargs):
        if not workers:
            command = [
                sys.executable,
                "-c",
                "from pathlib import Path; import time; Path('files/partial').touch(); time.sleep(30)",
            ]
        process = process_factory(command, **kwargs)
        workers.append((process, Path(kwargs["cwd"])))
        return process

    monkeypatch.setattr(execution.subprocess, "Popen", start_worker)
    monkeypatch.setattr(execution, "_operation_timeout", 0.5)
    monkeypatch.setattr(execution, "_slots", BoundedSemaphore(1))

    started = time.monotonic()
    response = client.post("/v1/manifest/full_resolve", json={"manifest": MANIFEST, "config": {}})

    assert time.monotonic() - started < 3
    assert response.status_code == 400
    assert "ran out of time" in response.json()["detail"]
    process, scratch = workers[0]
    assert process.returncode is not None and process.returncode < 0
    with pytest.raises(ChildProcessError):
        os.waitpid(process.pid, os.WNOHANG)
    assert not scratch.exists()

    monkeypatch.setattr(execution, "_operation_timeout", 10)
    response = client.post("/v1/manifest/full_resolve", json={"manifest": MANIFEST, "config": {}})
    assert response.status_code == 200, response.json()


def test_busy_server_rejects_work_before_spawning(monkeypatch):
    slots = BoundedSemaphore(1)
    slots.acquire()
    monkeypatch.setattr(execution, "_slots", slots)

    response = client.post("/v1/manifest/full_resolve", json={"manifest": MANIFEST, "config": {}})

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"


@pytest.mark.parametrize("value", ["", "abc", "inf", "nan", "0", "-5"])
def test_invalid_deadline_override_is_rejected(monkeypatch, value):
    monkeypatch.setenv(execution._OPERATION_TIMEOUT_ENV, value)
    with pytest.raises(ValueError, match="must be a finite positive number"):
        execution._seconds(execution._OPERATION_TIMEOUT_ENV, 295)


def test_worker_logs_stack_trace_for_unexpected_errors():
    with tempfile.TemporaryFile() as result_file:
        worker = subprocess.run(
            [
                sys.executable,
                "-m",
                "airbyte_cdk.manifest_server.operation_worker",
                "full_resolve",
                str(result_file.fileno()),
                str(time.monotonic() + 30),
            ],
            input=b"not json",
            capture_output=True,
            pass_fds=(result_file.fileno(),),
            timeout=30,
        )
        result_file.seek(0)
        result = execution.WorkerResult.model_validate_json(result_file.read())

    assert worker.returncode == 0, worker.stderr
    assert result.status == 500
    assert "Unexpected exception in Manifest worker for full_resolve" in worker.stderr.decode()
    assert "Traceback" in worker.stderr.decode()
