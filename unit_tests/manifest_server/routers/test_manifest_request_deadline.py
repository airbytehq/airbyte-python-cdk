import os
import subprocess
import sys
import tempfile
import time
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


def test_full_resolve_uses_supervised_worker():
    response = client.post("/v1/manifest/full_resolve", json={"manifest": MANIFEST, "config": {}})

    assert response.status_code == 200, response.json()
    assert response.json() == {"manifest": MANIFEST}


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
