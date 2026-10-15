#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import os
import select
import subprocess
import sys
import time
from unittest.mock import MagicMock

from airbyte_cdk.utils.print_buffer import PrintBuffer


def test_flush_pushes_through_underlying_stdout(monkeypatch):
    mock_stdout = MagicMock()
    monkeypatch.setattr(sys, "__stdout__", mock_stdout)

    print_buffer = PrintBuffer()
    print_buffer.write("hello")
    print_buffer.flush()

    mock_stdout.write.assert_called_once_with("hello")
    mock_stdout.flush.assert_called_once()


def test_output_reaches_pipe_before_process_exit():
    child = r"""
import time, sys
from airbyte_cdk.logger import PRINT_BUFFER, init_logger

logger = init_logger("airbyte")
with PRINT_BUFFER:
    print('{"type":"RECORD","record":{"stream":"s","data":{"id":1},"emitted_at":0}}\n', end="")
    logger.info("Read 1 records from s stream")
    time.sleep(10)
"""
    env = os.environ.copy()
    env.pop("PYTHONUNBUFFERED", None)
    process = subprocess.Popen(
        [sys.executable, "-c", child],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    assert process.stdout is not None
    assert process.stderr is not None

    output = bytearray()
    deadline = time.monotonic() + 5.0
    record_marker = b'"type":"RECORD"'
    log_marker = b"Read 1 records from s stream"

    try:
        stdout_fd = process.stdout.fileno()
        os.set_blocking(stdout_fd, False)
        while time.monotonic() < deadline and not (
            record_marker in output and log_marker in output
        ):
            timeout = max(0.0, deadline - time.monotonic())
            readable, _, _ = select.select([process.stdout], [], [], timeout)
            if not readable:
                break
            try:
                chunk = os.read(stdout_fd, 65536)
            except BlockingIOError:
                continue
            if not chunk:
                break
            output.extend(chunk)

        process_alive = process.poll() is None
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        stderr = process.stderr.read()

    assert record_marker in output and log_marker in output, (
        "Expected both records in child output before process exit. "
        f"Received stdout: {bytes(output)!r}; stderr: {stderr!r}"
    )
    assert process_alive, (
        "Child exited before output was observed. "
        f"Received stdout: {bytes(output)!r}; stderr: {stderr!r}"
    )
