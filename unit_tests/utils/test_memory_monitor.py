#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.memory_monitor import (
    _CGROUP_V1_LIMIT,
    _CGROUP_V1_USAGE,
    _CGROUP_V2_CURRENT,
    _CGROUP_V2_MAX,
    MemoryMonitor,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

_MOCK_USAGE_BELOW = "500000000\n"  # 50% of 1 GB
_MOCK_USAGE_WARNING = "870000000\n"  # 87% of 1 GB
_MOCK_USAGE_CRITICAL = "960000000\n"  # 96% of 1 GB
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
    """No warning should be emitted when usage is below 85%."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_BELOW)),
    ):
        monitor.check_memory_usage()
    assert not caplog.records


# ---------------------------------------------------------------------------
# check_memory_usage — warning threshold
# ---------------------------------------------------------------------------


def test_warning_at_85_percent(caplog: pytest.LogCaptureFixture) -> None:
    """Warning log should be emitted at 87% usage (above 85% threshold)."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_WARNING)),
    ):
        monitor.check_memory_usage()

    assert len(caplog.records) == 1
    assert "87%" in caplog.records[0].message


def test_warning_emitted_only_once(caplog: pytest.LogCaptureFixture) -> None:
    """Warning should only be logged once even if called multiple times."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_WARNING)),
    ):
        monitor.check_memory_usage()
        monitor.check_memory_usage()

    assert len(caplog.records) == 1


def test_custom_thresholds_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Custom warning threshold should be respected."""
    monitor = MemoryMonitor(
        warning_threshold=0.70,
        critical_threshold=0.90,
        check_interval=1,
    )
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage="750000000\n")),
    ):
        # 75% exceeds 70% warning threshold but is below 90% critical
        monitor.check_memory_usage()

    assert len(caplog.records) == 1
    assert "75%" in caplog.records[0].message


# ---------------------------------------------------------------------------
# check_memory_usage — critical threshold
# ---------------------------------------------------------------------------


def test_critical_at_95_percent_raises() -> None:
    """AirbyteTracedException should be raised at 96% usage."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_CRITICAL)),
    ):
        with pytest.raises(AirbyteTracedException) as exc_info:
            monitor.check_memory_usage()

    assert exc_info.value.failure_type == FailureType.system_error
    assert "96%" in (exc_info.value.message or "")


def test_critical_raised_only_once() -> None:
    """AirbyteTracedException should only be raised once."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_CRITICAL)),
    ):
        with pytest.raises(AirbyteTracedException):
            monitor.check_memory_usage()
        # Second call should NOT raise again
        monitor.check_memory_usage()


def test_custom_thresholds_critical() -> None:
    """Custom critical threshold should be respected."""
    monitor = MemoryMonitor(
        warning_threshold=0.70,
        critical_threshold=0.80,
        check_interval=1,
    )
    with (
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage="850000000\n")),
    ):
        with pytest.raises(AirbyteTracedException):
            monitor.check_memory_usage()


# ---------------------------------------------------------------------------
# check_memory_usage — cgroup v1 path
# ---------------------------------------------------------------------------


def test_cgroup_v1_emits_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Memory reading should work with cgroup v1 paths (proves v1 detection works)."""

    def mock_read_text(self: Path) -> str:
        if self == _CGROUP_V1_USAGE:
            return _MOCK_USAGE_WARNING
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
    assert "87%" in caplog.records[0].message


# ---------------------------------------------------------------------------
# check_memory_usage — check interval
# ---------------------------------------------------------------------------


def test_check_interval_skips_intermediate_calls(caplog: pytest.LogCaptureFixture) -> None:
    """Monitor should only check cgroup files every check_interval messages."""
    monitor = MemoryMonitor(check_interval=3)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_WARNING)),
    ):
        monitor.check_memory_usage()
        assert not caplog.records  # call 1: skipped
        monitor.check_memory_usage()
        assert not caplog.records  # call 2: skipped
        # Call 3 should trigger the actual check
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
