#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from airbyte_cdk.utils.memory_monitor import (
    _CGROUP_V1_LIMIT,
    _CGROUP_V1_USAGE,
    _CGROUP_V2_CURRENT,
    _CGROUP_V2_MAX,
    MemoryMonitor,
)

_MOCK_USAGE_BELOW = "500000000\n"  # 50% of 1 GB
_MOCK_USAGE_AT_90 = "910000000\n"  # 91% of 1 GB
_MOCK_LIMIT = "1000000000\n"  # 1 GB


def _v2_exists(self: Path) -> bool:
    return self in (_CGROUP_V2_CURRENT, _CGROUP_V2_MAX)


def _v1_exists(self: Path) -> bool:
    return self in (_CGROUP_V1_USAGE, _CGROUP_V1_LIMIT)


def _v2_mock_read(usage: str = _MOCK_USAGE_BELOW, limit: str = _MOCK_LIMIT):
    """Return a mock_read_text function for cgroup v2 with the given usage/limit."""

    def mock_read_text(self: Path) -> str:
        if self == _CGROUP_V2_CURRENT:
            return usage
        if self == _CGROUP_V2_MAX:
            return limit
        return ""

    return mock_read_text


# ---------------------------------------------------------------------------
# __init__ — input validation
# ---------------------------------------------------------------------------


def test_check_interval_zero_raises() -> None:
    """check_interval=0 should raise ValueError at construction time."""
    with pytest.raises(ValueError, match="check_interval must be >= 1"):
        MemoryMonitor(check_interval=0)


def test_check_interval_negative_raises() -> None:
    """Negative check_interval should raise ValueError at construction time."""
    with pytest.raises(ValueError, match="check_interval must be >= 1"):
        MemoryMonitor(check_interval=-1)


# ---------------------------------------------------------------------------
# check_memory_usage — no-op paths
# ---------------------------------------------------------------------------


def test_noop_when_no_cgroup(caplog: pytest.LogCaptureFixture) -> None:
    """check_memory_usage should be a no-op when cgroup is unavailable."""
    monitor = MemoryMonitor()
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", return_value=False),
    ):
        monitor.check_memory_usage()
    assert not caplog.records


def test_noop_when_limit_is_max(caplog: pytest.LogCaptureFixture) -> None:
    """When cgroup v2 memory.max is 'max' (unlimited), should be a no-op."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(limit="max\n")),
    ):
        monitor.check_memory_usage()
    assert not caplog.records


def test_noop_when_limit_is_zero(caplog: pytest.LogCaptureFixture) -> None:
    """When cgroup limit file contains '0', should be a no-op."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(limit="0\n")),
    ):
        monitor.check_memory_usage()
    assert not caplog.records


# ---------------------------------------------------------------------------
# check_memory_usage — below threshold
# ---------------------------------------------------------------------------


def test_no_warning_below_threshold(caplog: pytest.LogCaptureFixture) -> None:
    """No warning should be emitted when usage is below 90%."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_BELOW)),
    ):
        monitor.check_memory_usage()
    assert not caplog.records


# ---------------------------------------------------------------------------
# check_memory_usage — at/above 90% threshold
# ---------------------------------------------------------------------------


def test_logs_at_90_percent(caplog: pytest.LogCaptureFixture) -> None:
    """Warning log should be emitted at 91% usage (above 90% threshold)."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_AT_90)),
    ):
        monitor.check_memory_usage()

    assert len(caplog.records) == 1
    assert "91%" in caplog.records[0].message


def test_logs_on_every_check_above_90_percent(caplog: pytest.LogCaptureFixture) -> None:
    """Warning should be logged on EVERY check interval when above 90%, not just once."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_AT_90)),
    ):
        monitor.check_memory_usage()
        monitor.check_memory_usage()
        monitor.check_memory_usage()

    # All three checks should produce a warning (no one-shot flag)
    assert len(caplog.records) == 3
    for record in caplog.records:
        assert "91%" in record.message


# ---------------------------------------------------------------------------
# check_memory_usage — cgroup v1 path
# ---------------------------------------------------------------------------


def test_cgroup_v1_emits_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Memory reading should work with cgroup v1 paths (proves v1 detection works)."""

    def mock_read_text(self: Path) -> str:
        if self == _CGROUP_V1_USAGE:
            return _MOCK_USAGE_AT_90
        if self == _CGROUP_V1_LIMIT:
            return _MOCK_LIMIT
        return ""

    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v1_exists),
        patch.object(Path, "read_text", mock_read_text),
    ):
        monitor.check_memory_usage()

    assert len(caplog.records) == 1
    assert "91%" in caplog.records[0].message


# ---------------------------------------------------------------------------
# check_memory_usage — check interval
# ---------------------------------------------------------------------------


def test_check_interval_skips_intermediate_calls(caplog: pytest.LogCaptureFixture) -> None:
    """Monitor should only check cgroup files every check_interval messages."""
    monitor = MemoryMonitor(check_interval=5000)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_AT_90)),
    ):
        # First 4999 calls should be skipped
        for _ in range(4999):
            monitor.check_memory_usage()
        assert not caplog.records
        # Call 5000 should trigger the actual check
        monitor.check_memory_usage()
    assert len(caplog.records) == 1


# ---------------------------------------------------------------------------
# check_memory_usage — graceful degradation
# ---------------------------------------------------------------------------


def test_malformed_cgroup_file_degrades_gracefully(caplog: pytest.LogCaptureFixture) -> None:
    """Malformed cgroup files should not crash the sync."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", return_value="not_a_number\n"),
    ):
        monitor.check_memory_usage()
    assert not caplog.records


def test_empty_cgroup_file_degrades_gracefully(caplog: pytest.LogCaptureFixture) -> None:
    """Empty cgroup file content should not crash the sync."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", return_value=""),
    ):
        monitor.check_memory_usage()
    assert not caplog.records


def test_os_error_degrades_gracefully(caplog: pytest.LogCaptureFixture) -> None:
    """OSError reading cgroup files should not crash the sync."""

    def mock_read_text(self: Path) -> str:
        raise OSError("Permission denied")

    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", mock_read_text),
    ):
        monitor.check_memory_usage()
    assert not caplog.records
