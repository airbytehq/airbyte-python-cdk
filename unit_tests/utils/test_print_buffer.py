#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import io
import logging
import os
import re
import select
import signal
import subprocess
import sys
import threading
import time
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.utils.print_buffer import PrintBuffer


def test_flush_drains_without_fd_flush_and_sweeper_flushes(monkeypatch):
    flushed = threading.Event()
    mock_stdout = MagicMock()
    mock_stdout.flush.side_effect = lambda *args, **kwargs: flushed.set()
    monkeypatch.setattr(sys, "__stdout__", mock_stdout)

    with PrintBuffer(flush_interval=1.0) as print_buffer:
        print_buffer.write("hello")
        print_buffer.flush()

        mock_stdout.write.assert_called_once_with("hello")
        # ~1 s of margin before the sweeper would fire
        assert not mock_stdout.flush.called
        assert flushed.wait(timeout=10)


def test_unexpected_sweeper_error_falls_back_to_synchronous_flush(monkeypatch):
    sweeper_failed = threading.Event()
    mock_stdout = MagicMock()

    def flush_fails_once():
        if not sweeper_failed.is_set():
            sweeper_failed.set()
            raise RuntimeError("unexpected")

    mock_stdout.flush.side_effect = flush_fails_once
    monkeypatch.setattr(sys, "__stdout__", mock_stdout)

    with PrintBuffer(flush_interval=0.01) as print_buffer:
        print_buffer.write("a")
        assert sweeper_failed.wait(timeout=10)
        print_buffer._sweeper.join(timeout=10)
        assert not print_buffer._sweeper.is_alive()
        assert print_buffer._sync_flush

        mock_stdout.flush.reset_mock()
        print_buffer.flush()

        mock_stdout.flush.assert_called_once()


class _CountingRaw(io.RawIOBase):
    def __init__(self):
        self.write_calls = 0
        self.data = bytearray()

    def writable(self):
        return True

    def write(self, b):
        self.write_calls += 1
        self.data.extend(b)
        return len(b)


def _buffered_counting_stdout(monkeypatch):
    raw = _CountingRaw()
    monkeypatch.setattr(
        sys, "__stdout__", io.TextIOWrapper(io.BufferedWriter(raw, buffer_size=8192))
    )
    return raw


def test_exit_flushes_pending_and_stops_sweeper(monkeypatch):
    raw = _buffered_counting_stdout(monkeypatch)

    with PrintBuffer(flush_interval=60) as print_buffer:
        print_buffer.write("a\n")
        print_buffer.flush()
        print_buffer.write("b\n")
        sweeper = print_buffer._sweeper

    assert bytes(raw.data) == b"a\nb\n"
    assert sweeper is not None
    assert not sweeper.is_alive()


def test_syscall_count_stays_batched_at_one_log_line_per_record(monkeypatch):
    raw = _buffered_counting_stdout(monkeypatch)
    print_buffer = PrintBuffer(flush_interval=0.1)
    logger = logging.Logger("print_buffer_syscalls")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(logging.StreamHandler(print_buffer))

    n = 2000
    record_line = (
        '{"type":"RECORD","record":{"stream":"s","data":{"id":0,'
        '"payload":"' + "x" * 200 + '"},"emitted_at":0}}\n'
    )
    expected = ""
    started = time.monotonic()
    with print_buffer:
        for i in range(n):
            print_buffer.write(record_line)
            expected += record_line
            logger.info("Read %s records", i)
            expected += f"Read {i} records\n"
    elapsed = time.monotonic() - started

    total_bytes = len(expected.encode())
    assert bytes(raw.data) == expected.encode()
    max_write_calls = total_bytes // 8192 + 1 + int(elapsed / 0.1) + 3
    assert raw.write_calls <= max_write_calls, (
        f"expected batched fd writes, got {raw.write_calls} raw writes "
        f"for {n} log lines in {elapsed:.2f}s (bound {max_write_calls})"
    )


def _spawn_child(code):
    env = os.environ.copy()
    env.pop("PYTHONUNBUFFERED", None)
    return subprocess.Popen(
        [sys.executable, "-W", "ignore::DeprecationWarning", "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        start_new_session=True,
    )


def _read_until(process, markers, timeout):
    """Read child stdout until every marker is seen or the deadline passes.

    Returns (output bytes, {marker: time.monotonic() at first sighting}).
    """
    assert process.stdout is not None
    stdout_fd = process.stdout.fileno()
    os.set_blocking(stdout_fd, False)
    output = bytearray()
    seen = {}
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and len(seen) < len(markers):
        readable, _, _ = select.select(
            [process.stdout], [], [], max(0.0, deadline - time.monotonic())
        )
        if not readable:
            break
        try:
            chunk = os.read(stdout_fd, 65536)
        except BlockingIOError:
            continue
        if not chunk:
            break
        output.extend(chunk)
        now = time.monotonic()
        for marker in markers:
            if marker not in seen and marker in output:
                seen[marker] = now
    return bytes(output), seen


def _kill_process_group(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            process.kill()
    process.wait()


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


@pytest.mark.skipif(
    sys.platform != "linux", reason="relies on CLOCK_MONOTONIC being shared across processes"
)
def test_quiet_line_reaches_pipe_within_two_flush_intervals():
    child = r"""
import time
from airbyte_cdk.logger import PRINT_BUFFER, init_logger

PRINT_BUFFER.flush_interval = 0.5
logger = init_logger("airbyte")
with PRINT_BUFFER:
    print('{"type":"RECORD","record":{"stream":"s","data":{"id":1},"emitted_at":0}}\n', end="")
    logger.info(f"quiet-line sent_at={time.monotonic()!r}")
    time.sleep(30)
"""
    process = _spawn_child(child)
    try:
        output, seen = _read_until(process, [b"sent_at="], timeout=10)
        match = re.search(rb"sent_at=([0-9.]+)", output)
        assert match is not None, f"quiet-line not observed. stdout: {output!r}"
        sent_at = float(match.group(1))
        latency = seen[b"sent_at="] - sent_at
        print(f"\nquiet-line latency: {latency:.3f}s")
        assert latency < 2 * 0.5, f"quiet-line took {latency:.3f}s to reach the pipe"
        assert process.poll() is None, "child exited before output was observed"
    finally:
        _kill_process_group(process)
        stderr = process.stderr.read()
        if stderr:
            print(f"\nchild stderr: {stderr!r}")


def test_pending_output_flushed_on_exit():
    child = r"""
from airbyte_cdk.logger import PRINT_BUFFER, init_logger

logger = init_logger("airbyte")
with PRINT_BUFFER:
    for i in range(1000):
        print('{"type":"RECORD","record":{"stream":"s","data":{"id":%d,"e":0},"emitted_at":0}}\n' % i, end="")
        logger.info("log-line-%d-done", i)
logger.info("after-with")
PRINT_BUFFER.write("raw-write-no-flush\n")
"""
    process = _spawn_child(child)
    try:
        stdout, stderr = process.communicate(timeout=60)
    finally:
        _kill_process_group(process)

    for i in range(1000):
        assert stdout.count(b'"id":%d,' % i) == 1, f"RECORD {i} missing or duplicated"
        assert stdout.count(b"log-line-%d-done" % i) == 1, f"log line {i} missing or duplicated"
    assert b"after-with" in stdout
    assert b"raw-write-no-flush" in stdout


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_forked_child_gets_a_working_sweeper():
    child = r"""
import os, time
from airbyte_cdk.logger import PRINT_BUFFER, init_logger

logger = init_logger("airbyte")
with PRINT_BUFFER:
    logger.info("before-fork")
    pid = os.fork()
    if pid == 0:
        logger.info("in-fork-child")
        time.sleep(30)
        os._exit(0)
    else:
        logger.info("in-fork-parent")
        time.sleep(30)
"""
    process = _spawn_child(child)
    try:
        output, seen = _read_until(process, [b"in-fork-child", b"in-fork-parent"], timeout=10)
        assert b"in-fork-child" in seen, (
            f"forked child's log line never arrived. stdout: {output!r}"
        )
        assert b"in-fork-parent" in seen, (
            f"fork parent's log line never arrived. stdout: {output!r}"
        )
        assert process.poll() is None, "child exited before output was observed"
        assert output.count(b"before-fork") == 1, (
            f"pre-fork output emitted != 1 time. stdout: {output!r}"
        )
    finally:
        _kill_process_group(process)
        stderr = process.stderr.read()
        if stderr:
            print(f"\nchild stderr: {stderr!r}")
