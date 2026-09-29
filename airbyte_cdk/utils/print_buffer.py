# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import atexit
import os
import sys
import threading
import time
import weakref
from io import StringIO
from threading import RLock
from types import TracebackType
from typing import Optional

_REGISTERED: "weakref.WeakSet[PrintBuffer]" = weakref.WeakSet()
_FORK_HELD: list["PrintBuffer"] = []
_process_hooks_registered = False
_PROCESS_HOOKS_LOCK = threading.Lock()


def _register_process_hooks() -> None:
    global _process_hooks_registered
    if _process_hooks_registered:
        return
    with _PROCESS_HOOKS_LOCK:
        if _process_hooks_registered:
            return
        atexit.register(_shutdown_all)
        if hasattr(os, "register_at_fork"):  # not available on Windows
            os.register_at_fork(
                before=_before_fork,
                after_in_parent=_after_fork_in_parent,
                after_in_child=_after_fork_in_child,
            )
        _process_hooks_registered = True


def _shutdown_all() -> None:
    for b in list(_REGISTERED):
        b._shutdown()


def _before_fork() -> None:
    buffers = list(_REGISTERED)
    for b in buffers:
        b.lock.acquire()
    _FORK_HELD[:] = buffers
    for b in buffers:
        try:
            b._flush_to_fd()  # the child must not inherit (and later re-emit) pending bytes
        except (OSError, ValueError):
            pass


def _after_fork_in_parent() -> None:
    for b in _FORK_HELD:
        b.lock.release()
    _FORK_HELD.clear()


def _after_fork_in_child() -> None:
    for b in _FORK_HELD:
        b._reset_after_fork()
    _FORK_HELD.clear()


class PrintBuffer:
    """
    A class to buffer print statements and flush them at a specified interval.

    The PrintBuffer class is designed to capture and buffer output that would
    normally be printed to the standard output (stdout). This can be useful for
    scenarios where you want to minimize the number of I/O operations by grouping
    multiple print statements together and flushing them as a single operation.

    Buffer content is pushed through to the stdout file descriptor by a daemon
    sweeper thread once per flush interval, so callers of `flush()` (such as
    logging handlers, which flush after every record) do not each pay for a
    file-descriptor flush.

    Attributes:
        buffer (StringIO): A buffer to store the messages before flushing.
        flush_interval (float): The time interval (in seconds) after which the buffer is flushed.
        last_flush_time (float): The last time the buffer was flushed.
        lock (RLock): A reentrant lock to ensure thread-safe operations.
        _sweeper (Optional[threading.Thread]): The daemon thread pushing pending output to the file descriptor, or None.
        _stop (threading.Event): Signals the sweeper thread to exit.
        _pending (threading.Event): Signals the sweeper thread that output is waiting to be pushed.
        _sync_flush (bool): When True, `flush()` pushes to the file descriptor synchronously (no sweeper).

    Methods:
        write(message: str) -> None:
            Writes a message to the buffer and flushes if the interval has passed.

        flush() -> None:
            Drains the buffer to the standard output and marks it pending for the sweeper thread.

        __enter__() -> "PrintBuffer":
            Enters the runtime context related to this object, redirecting stdout and stderr.

        __exit__(exc_type, exc_val, exc_tb) -> None:
            Exits the runtime context and restores the original stdout and stderr.
    """

    def __init__(self, flush_interval: float = 0.1):
        self.buffer = StringIO()
        self.flush_interval = flush_interval
        self.last_flush_time = time.monotonic()
        self.lock = RLock()
        self._sweeper: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._pending = threading.Event()
        self._sync_flush = False
        self._pending_marked = False

    def write(self, message: str) -> None:
        with self.lock:
            self.buffer.write(message)
            if not self._pending_marked:
                self._mark_pending()
            current_time = time.monotonic()
            if (current_time - self.last_flush_time) >= self.flush_interval:
                self.flush()
                self.last_flush_time = current_time

    def flush(self) -> None:
        """Drains the buffered content to the real stdout and marks it pending for the sweeper.

        `sys.__stdout__` is block-buffered when stdout is a pipe; the sweeper thread
        pushes pending output through to the file descriptor within one flush interval.
        When `_sync_flush` is set, the push to the file descriptor happens here instead.
        """
        with self.lock:
            if self._sync_flush:
                self._flush_to_fd()
            else:
                self._drain()
                if not self._pending_marked:
                    self._mark_pending()

    def _drain(self) -> None:
        combined_message = self.buffer.getvalue()
        sys.__stdout__.write(combined_message)  # type: ignore[union-attr]
        self.buffer = StringIO()

    def _flush_to_fd(self) -> None:
        self._drain()
        sys.__stdout__.flush()  # type: ignore[union-attr]

    def _mark_pending(self) -> None:
        if self._sync_flush:
            return
        if self._sweeper is None:
            self._start_sweeper()
        if self._sweeper is None:
            return  # _start_sweeper fell back to synchronous flush
        self._pending.set()
        self._pending_marked = True

    def _start_sweeper(self) -> None:
        stop, pending = threading.Event(), threading.Event()
        thread = threading.Thread(
            target=self._sweep,
            args=(stop, pending),
            name="airbyte-print-buffer-sweeper",
            daemon=True,
        )
        _register_process_hooks()
        _REGISTERED.add(self)
        try:
            thread.start()
        except RuntimeError:  # e.g. interpreter shutdown / thread limit
            self._sync_flush = True
            return
        self._sweeper, self._stop, self._pending = thread, stop, pending

    def _sweep(self, stop: threading.Event, pending: threading.Event) -> None:
        while not stop.is_set():
            pending.wait()  # idle buffers block here without consuming CPU
            stop.wait(self.flush_interval)  # coalesce for one interval
            with self.lock:
                pending.clear()
                if pending is self._pending:
                    self._pending_marked = False
                try:
                    self._flush_to_fd()
                except (OSError, ValueError):  # broken pipe / closed stdout
                    self._sync_flush = True
                    return

    def _stop_sweeper(self) -> None:
        with self.lock:
            thread, stop, pending = self._sweeper, self._stop, self._pending
            self._sweeper = None
            self._pending_marked = False
        if thread is None:
            return
        stop.set()
        pending.set()
        if thread is not threading.current_thread():
            thread.join()

    def _shutdown(self) -> None:
        with self.lock:
            self._sync_flush = True
        self._stop_sweeper()
        with self.lock:
            try:
                self._flush_to_fd()
            except (OSError, ValueError):
                pass

    def _reset_after_fork(self) -> None:
        # the sweeper thread does not survive fork; locks and events may be inconsistent
        self.lock = RLock()
        self._sweeper = None
        self._stop = threading.Event()
        self._pending = threading.Event()
        self._pending_marked = False

    def __enter__(self) -> "PrintBuffer":
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        # Used to disable buffering during the pytest session, because it is not compatible with capsys
        if "pytest" not in str(type(sys.stdout)).lower():
            sys.stdout = self
            sys.stderr = self
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._stop_sweeper()
        with self.lock:
            self._flush_to_fd()
        sys.stdout, sys.stderr = self.old_stdout, self.old_stderr
