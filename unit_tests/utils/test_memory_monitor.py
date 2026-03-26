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
    _PROC_SELF_STATUS,
    MemoryMonitor,
    _read_process_anon_rss_bytes,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

_MOCK_USAGE_BELOW = "500000000\n"  # 50% of 1 GB
_MOCK_USAGE_AT_90 = "910000000\n"  # 91% of 1 GB
_MOCK_USAGE_AT_97 = "970000000\n"  # 97% of 1 GB  (below 98% critical threshold)
_MOCK_USAGE_AT_98 = "980000000\n"  # 98% of 1 GB  (at critical threshold)
_MOCK_LIMIT = "1000000000\n"  # 1 GB

# Anonymous RSS mock values (in kB as they appear in /proc/self/status RssAnon field).
# VmRSS is intentionally kept high in the "low anon" mock to prove the metric choice matters:
# VmRSS can be inflated by file-backed pages while RssAnon stays low.
_MOCK_ANON_HIGH = "RssAnon:\t   820000 kB\n"  # ~82% of 1 GB (above 80% threshold)
_MOCK_ANON_LOW_VMRSS_HIGH = (
    "VmRSS:\t   900000 kB\n"  # ~90% of 1 GB — high total RSS
    "RssAnon:\t   500000 kB\n"  # ~50% of 1 GB — low anonymous RSS
)


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


# ---------------------------------------------------------------------------
# _read_process_anon_rss_bytes — unit tests
# ---------------------------------------------------------------------------


def test_read_process_anon_rss_bytes_parses_rssanon() -> None:
    """Correctly parses RssAnon from /proc/self/status content."""
    status_content = (
        "Name:\tpython3\nVmRSS:\t  1000000 kB\nRssAnon:\t   512000 kB\nRssShmem:\t        0 kB\n"
    )
    with patch.object(Path, "read_text", return_value=status_content):
        result = _read_process_anon_rss_bytes()
    assert result == 512000 * 1024


def test_read_process_anon_rss_bytes_returns_none_on_missing_file() -> None:
    """Returns None when /proc/self/status is unreadable."""

    def raise_oserror(self: Path) -> str:
        raise OSError("No such file")

    with patch.object(Path, "read_text", raise_oserror):
        assert _read_process_anon_rss_bytes() is None


def test_read_process_anon_rss_bytes_returns_none_when_rssanon_absent() -> None:
    """Returns None when RssAnon line is not present (e.g. older kernel)."""
    with patch.object(Path, "read_text", return_value="Name:\tpython3\nVmRSS:\t  512000 kB\n"):
        assert _read_process_anon_rss_bytes() is None


def test_read_process_anon_rss_bytes_ignores_vmrss() -> None:
    """Ensures the parser reads RssAnon specifically, not VmRSS."""
    # Only VmRSS present, no RssAnon — should return None
    status_content = "VmRSS:\t   900000 kB\n"
    with patch.object(Path, "read_text", return_value=status_content):
        assert _read_process_anon_rss_bytes() is None


# ---------------------------------------------------------------------------
# check_memory_usage — fail-fast (dual-condition)
# ---------------------------------------------------------------------------


def _proc_status_read(anon_content: str, usage: str = _MOCK_USAGE_AT_98):
    """Return a mock read_text that serves cgroup v2 AND /proc/self/status."""

    def mock_read_text(self: Path) -> str:
        if self == _CGROUP_V2_CURRENT:
            return usage
        if self == _CGROUP_V2_MAX:
            return _MOCK_LIMIT
        if self == _PROC_SELF_STATUS:
            return anon_content
        return ""

    return mock_read_text


def test_raises_when_both_cgroup_and_anon_rss_above_thresholds() -> None:
    """Fail-fast raises AirbyteTracedException when both cgroup >= 98% and RssAnon >= 80%."""
    monitor = MemoryMonitor(check_interval=1, fail_fast=True)
    with (
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _proc_status_read(_MOCK_ANON_HIGH)),
    ):
        with pytest.raises(AirbyteTracedException) as exc_info:
            monitor.check_memory_usage()
    assert exc_info.value.failure_type == FailureType.system_error
    assert "critical threshold" in (exc_info.value.message or "")
    assert "98%" in (exc_info.value.message or "")
    assert "anonymous RSS" in (exc_info.value.internal_message or "")


def test_no_raise_when_cgroup_high_but_anon_rss_low(caplog: pytest.LogCaptureFixture) -> None:
    """No exception when cgroup >= 98% but RssAnon < 80% (file-backed pages scenario).

    This test also proves the metric choice matters: VmRSS is 90% (high) but
    RssAnon is only 50% (low), so the pressure is from file-backed pages.
    """
    monitor = MemoryMonitor(check_interval=1, fail_fast=True)
    with (
        caplog.at_level(logging.INFO, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _proc_status_read(_MOCK_ANON_LOW_VMRSS_HIGH)),
    ):
        monitor.check_memory_usage()  # Should NOT raise
    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any("file-backed" in r.message for r in info_records)


def test_no_raise_when_cgroup_below_critical() -> None:
    """No exception when cgroup at 97% (< 98% threshold), even with high RssAnon."""
    monitor = MemoryMonitor(check_interval=1, fail_fast=True)
    with (
        patch.object(Path, "exists", _v2_exists),
        patch.object(
            Path, "read_text", _proc_status_read(_MOCK_ANON_HIGH, usage=_MOCK_USAGE_AT_97)
        ),
    ):
        monitor.check_memory_usage()  # Should NOT raise


def test_no_raise_when_anon_rss_unavailable_and_cgroup_critical(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Logs warning and skips fail-fast when RssAnon is unavailable (stays truly dual-condition)."""

    def mock_read_text(self: Path) -> str:
        if self == _CGROUP_V2_CURRENT:
            return _MOCK_USAGE_AT_98
        if self == _CGROUP_V2_MAX:
            return _MOCK_LIMIT
        if self == _PROC_SELF_STATUS:
            raise OSError("No such file")
        return ""

    monitor = MemoryMonitor(check_interval=1, fail_fast=True)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", mock_read_text),
    ):
        monitor.check_memory_usage()  # Should NOT raise
    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("RssAnon unavailable" in r.message for r in warning_records)


# ---------------------------------------------------------------------------
# check_memory_usage — fail-fast feature flag
# ---------------------------------------------------------------------------


def test_fail_fast_disabled_via_constructor() -> None:
    """No exception when fail_fast=False even at critical thresholds."""
    monitor = MemoryMonitor(check_interval=1, fail_fast=False)
    with (
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _proc_status_read(_MOCK_ANON_HIGH)),
    ):
        monitor.check_memory_usage()  # Should NOT raise


def test_fail_fast_disabled_via_env_var() -> None:
    """No exception when AIRBYTE_MEMORY_FAIL_FAST=false."""
    with patch.dict("os.environ", {"AIRBYTE_MEMORY_FAIL_FAST": "false"}):
        monitor = MemoryMonitor(check_interval=1)
    with (
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _proc_status_read(_MOCK_ANON_HIGH)),
    ):
        monitor.check_memory_usage()  # Should NOT raise


def test_fail_fast_enabled_by_default() -> None:
    """Fail-fast is enabled by default (no env var, no explicit arg)."""
    with patch.dict("os.environ", {}, clear=True):
        monitor = MemoryMonitor(check_interval=1)
    assert monitor._fail_fast is True


def test_fail_fast_constructor_overrides_env_var() -> None:
    """Explicit fail_fast=True overrides env var set to false."""
    with patch.dict("os.environ", {"AIRBYTE_MEMORY_FAIL_FAST": "false"}):
        monitor = MemoryMonitor(check_interval=1, fail_fast=True)
    assert monitor._fail_fast is True
