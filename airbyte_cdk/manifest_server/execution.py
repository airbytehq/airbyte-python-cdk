"""Run network-capable Manifest Server operations in a process we can stop."""

import logging
import math
import os
import signal
import subprocess
import sys
import time
from contextvars import ContextVar
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile
from threading import BoundedSemaphore
from typing import TypeVar

from fastapi import HTTPException
from pydantic import BaseModel, ValidationError

from .helpers.auth import MANIFEST_SERVER_SIGNATURE_SECRET_ENV_VAR, SHARED_SIGNATURE_SECRET_ENV_VAR

logger = logging.getLogger("airbyte.manifest-server")

# Leave five seconds for cleanup and the response with the platform's 300 s Builder
# and 60 s direct check client defaults.
_OPERATION_TIMEOUT_ENV = "AIRBYTE_MANIFEST_SERVER_OPERATION_TIMEOUT_SECONDS"
_CHECK_TIMEOUT_ENV = "AIRBYTE_MANIFEST_SERVER_CHECK_TIMEOUT_SECONDS"
_WORKER_LIMIT_ENV = "AIRBYTE_MANIFEST_SERVER_MAX_ACTIVE_OPERATIONS"
_MAX_MESSAGE_BYTES = 32 * 1024 * 1024
_TIMEOUT_DETAIL = (
    "The connector operation ran out of time. Try again or reduce the amount of data requested."
)

# Only set inside a worker. The HTTP client uses an earlier deadline to allow
# cooperative cancellation to produce the usual Builder result.
operation_deadline: ContextVar[float | None] = ContextVar("operation_deadline", default=None)
ResponseT = TypeVar("ResponseT", bound=BaseModel)


class WorkerResult(BaseModel):
    status: int
    payload: str


def _seconds(name: str, default: float) -> float:
    raw_value = os.getenv(name, str(default))
    try:
        seconds = float(raw_value)
    except ValueError:
        seconds = math.nan
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError(f"{name} must be a finite positive number of seconds")
    return seconds


def _worker_limit() -> int:
    count = int(os.getenv(_WORKER_LIMIT_ENV, "4"))
    if count <= 0:
        raise ValueError(f"{_WORKER_LIMIT_ENV} must be a positive integer")
    return count


_operation_timeout = _seconds(_OPERATION_TIMEOUT_ENV, 295)
_check_timeout = min(_seconds(_CHECK_TIMEOUT_ENV, 55), _operation_timeout)
_slots = BoundedSemaphore(_worker_limit())


def run_operation(operation: str, request: BaseModel, response_type: type[ResponseT]) -> ResponseT:
    deadline = time.monotonic() + (_check_timeout if operation == "check" else _operation_timeout)
    if not _slots.acquire(blocking=False):
        raise HTTPException(
            status_code=503,
            detail="Manifest Server is busy. Try again shortly.",
            headers={"Retry-After": "1"},
        )

    try:
        return _run_operation(operation, request, response_type, deadline)
    except subprocess.TimeoutExpired:
        logger.warning("Manifest operation %s exceeded its execution deadline", operation)
        raise HTTPException(status_code=400, detail=_TIMEOUT_DETAIL) from None
    except (OSError, ValidationError):
        logger.error("Could not execute Manifest operation %s", operation)
        raise HTTPException(
            status_code=500, detail="Manifest operation failed to complete."
        ) from None
    finally:
        _slots.release()


def _run_operation(
    operation: str, request: BaseModel, response_type: type[ResponseT], deadline: float
) -> ResponseT:
    encoded_request = request.model_dump_json().encode()
    if len(encoded_request) > _MAX_MESSAGE_BYTES:
        raise HTTPException(status_code=413, detail="Manifest operation input is too large.")

    with (
        TemporaryDirectory(prefix="airbyte-manifest-") as scratch,
        TemporaryFile(dir=scratch) as request_file,
        TemporaryFile(dir=scratch) as result_file,
    ):
        staging = Path(scratch) / "files"
        staging.mkdir()
        env = {
            **os.environ,
            "TMPDIR": scratch,
            "AIRBYTE_STAGING_DIRECTORY": str(staging),
        }
        env.pop("REQUEST_CACHE_PATH", None)
        env.pop(MANIFEST_SERVER_SIGNATURE_SECRET_ENV_VAR, None)
        env.pop(SHARED_SIGNATURE_SECRET_ENV_VAR, None)
        # Keep any connector-created relative files inside the operation directory.
        import_root = str(Path(__file__).resolve().parents[2])
        env["PYTHONPATH"] = os.pathsep.join(filter(None, (import_root, env.get("PYTHONPATH"))))
        request_file.write(encoded_request)
        request_file.seek(0)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(operation, 0)
        cooperative_deadline = deadline - min(1.0, remaining / 10)

        command = [
            sys.executable,
            "-m",
            "airbyte_cdk.manifest_server.operation_worker",
            operation,
            str(result_file.fileno()),
            str(cooperative_deadline),
        ]
        with subprocess.Popen(
            command,
            stdin=request_file,
            stdout=subprocess.DEVNULL,
            env=env,
            cwd=scratch,
            pass_fds=(result_file.fileno(),),
            start_new_session=True,
        ) as process:
            try:
                exit_code = process.wait(timeout=max(0, deadline - time.monotonic()))
            finally:
                if process.returncode is None:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()

            if exit_code:
                logger.error("Manifest worker for %s exited with status %s", operation, exit_code)
                raise HTTPException(
                    status_code=500, detail="Manifest operation failed to complete."
                )

        if time.monotonic() >= deadline:
            raise subprocess.TimeoutExpired(operation, 0)
        result_file.seek(0)
        raw_result = result_file.read(_MAX_MESSAGE_BYTES + 1)
        if len(raw_result) > _MAX_MESSAGE_BYTES:
            raise HTTPException(status_code=400, detail="Manifest operation result is too large.")
        result = WorkerResult.model_validate_json(raw_result)
        if result.status != 200:
            raise HTTPException(status_code=result.status, detail=result.payload)
        response = response_type.model_validate_json(result.payload)
        if time.monotonic() >= deadline:
            raise subprocess.TimeoutExpired(operation, 0)
        return response
