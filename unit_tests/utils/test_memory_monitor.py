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
    _CGROUP_V2_STAT,
    _PROC_SELF_STATUS,
    MemoryMonitor,
    _format_bytes,
    _read_cgroup_v2_anon_bytes,
    _read_process_anon_rss_bytes,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

_MOCK_USAGE_BELOW = "400000000\n"  # 40% of 1 GB  (below 50% high-pressure threshold)
_MOCK_USAGE_AT_40 = "400000000\n"  # 40% of 1 GB  (below 50% high-pressure threshold)
_MOCK_USAGE_AT_60 = "600000000\n"  # 60% of 1 GB  (above 50% high-pressure, below 80% critical)
_MOCK_USAGE_AT_96 = "960000000\n"  # 96% of 1 GB  (above 80% critical threshold)
_MOCK_LIMIT = "1000000000\n"  # 1 GB

# cgroup v2 memory.stat mock content.
# The "anon" field is what we parse for the cgroup-level anonymous memory signal.
_MOCK_MEMORY_STAT_ANON_HIGH = (
    "anon 840000000\n"  # 840 MB — 87.5% of 960 MB usage (above 75% threshold)
    "file 100000000\n"
    "kernel 20000000\n"
)
_MOCK_MEMORY_STAT_ANON_LOW = (
    "anon 300000000\n"  # 300 MB — 31.25% of 960 MB usage (below 75% threshold)
    "file 630000000\n"
    "kernel 30000000\n"
)
_MOCK_MEMORY_STAT_NO_ANON = (
    "file 630000000\n"  # malformed: missing anon line
    "kernel 30000000\n"
)

# /proc/self/status mock values (fallback when cgroup v2 memory.stat is unavailable).
_MOCK_PROC_ANON_HIGH = "RssAnon:\t   820313 kB\n"  # ~840 MB — 87.5% of 960 MB usage (above 75%)
_MOCK_PROC_ANON_LOW = "RssAnon:\t   300000 kB\n"  # ~307 MB — 32.0% of 960 MB usage (below 75%)


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


def test_init_logs_configured_thresholds(caplog: pytest.LogCaptureFixture) -> None:
    """Instantiation should emit one INFO line with the configured thresholds and intervals."""
    with caplog.at_level(logging.INFO, logger="airbyte"):
        MemoryMonitor(check_interval=1234)
    init_logs = [r for r in caplog.records if "MemoryMonitor instantiated" in r.message]
    assert len(init_logs) == 1
    msg = init_logs[0].message
    assert "critical threshold: 80%" in msg
    assert "anon share of usage threshold: 75%" in msg
    assert "high-pressure threshold: 50%" in msg
    assert "check interval: 1234 messages" in msg
    assert "tightens to 5 under high pressure" in msg


# ---------------------------------------------------------------------------
# _format_bytes — unit tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("num_bytes", "expected"),
    [
        (0, "0 B"),
        (999, "999 B"),
        (1_000, "1.00 KB"),
        (1_500, "1.50 KB"),
        (999_999, "1000.00 KB"),
        (1_000_000, "1.00 MB"),
        (960_000_000, "960.00 MB"),
        (999_999_999, "1000.00 MB"),
        (1_000_000_000, "1.00 GB"),
        (2_109_915_136, "2.11 GB"),
        (2_147_483_648, "2.15 GB"),
    ],
)
def test_format_bytes(num_bytes: int, expected: str) -> None:
    """`_format_bytes` renders byte counts with 2 decimals using decimal units."""
    assert _format_bytes(num_bytes) == expected


# ---------------------------------------------------------------------------
# check_memory_usage — no-op paths
# ---------------------------------------------------------------------------


def test_noop_when_no_cgroup(caplog: pytest.LogCaptureFixture) -> None:
    """check_memory_usage should be a no-op when cgroup is unavailable."""
    monitor = MemoryMonitor()
    caplog.clear()  # discard the one-shot instantiation log
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", return_value=False),
    ):
        monitor.check_memory_usage()
    assert not caplog.records


def test_noop_when_limit_is_max(caplog: pytest.LogCaptureFixture) -> None:
    """When cgroup v2 memory.max is 'max' (unlimited), should be a no-op."""
    monitor = MemoryMonitor(check_interval=1)
    caplog.clear()
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
    caplog.clear()
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


def test_no_log_below_threshold(caplog: pytest.LogCaptureFixture) -> None:
    """No log should be emitted when usage is below the 50% high-pressure threshold."""
    monitor = MemoryMonitor(check_interval=1)
    caplog.clear()
    with (
        caplog.at_level(logging.DEBUG, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_AT_40)),
    ):
        monitor.check_memory_usage()
    # Only the debug probe message, no info/warning
    assert all(r.levelno <= logging.DEBUG for r in caplog.records)


# ---------------------------------------------------------------------------
# check_memory_usage — cgroup v1 path
# ---------------------------------------------------------------------------


def test_cgroup_v1_activates_high_pressure_mode(caplog: pytest.LogCaptureFixture) -> None:
    """Memory reading works with cgroup v1 paths and activates high-pressure mode at 50%."""

    def mock_read_text(self: Path) -> str:
        if self == _CGROUP_V1_USAGE:
            return _MOCK_USAGE_AT_60
        if self == _CGROUP_V1_LIMIT:
            return _MOCK_LIMIT
        return ""

    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.INFO, logger="airbyte"),
        patch.object(Path, "exists", _v1_exists),
        patch.object(Path, "read_text", mock_read_text),
    ):
        monitor.check_memory_usage()

    assert monitor._high_pressure_mode
    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any("tightening check interval" in r.message for r in info_records)


# ---------------------------------------------------------------------------
# check_memory_usage — check interval
# ---------------------------------------------------------------------------


def test_check_interval_skips_intermediate_calls(caplog: pytest.LogCaptureFixture) -> None:
    """Monitor should only check cgroup files every check_interval messages."""
    monitor = MemoryMonitor(check_interval=5000)
    caplog.clear()
    with (
        caplog.at_level(logging.INFO, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_AT_60)),
    ):
        # First 4999 calls should be skipped
        for _ in range(4999):
            monitor.check_memory_usage()
        info_records = [r for r in caplog.records if r.levelno >= logging.INFO]
        assert not info_records
        # Call 5000 should trigger the actual check and activate high-pressure mode
        monitor.check_memory_usage()
    assert monitor._high_pressure_mode


# ---------------------------------------------------------------------------
# check_memory_usage — graceful degradation
# ---------------------------------------------------------------------------


def test_malformed_cgroup_file_degrades_gracefully(caplog: pytest.LogCaptureFixture) -> None:
    """Malformed cgroup files should not crash the sync."""
    monitor = MemoryMonitor(check_interval=1)
    caplog.clear()
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
    caplog.clear()
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
    caplog.clear()
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
# _read_cgroup_v2_anon_bytes — unit tests
# ---------------------------------------------------------------------------


def test_read_cgroup_v2_anon_bytes_parses_anon_field() -> None:
    """Correctly parses the 'anon' field from cgroup v2 memory.stat."""
    with patch.object(Path, "read_text", return_value=_MOCK_MEMORY_STAT_ANON_HIGH):
        result = _read_cgroup_v2_anon_bytes()
    assert result == 840000000


def test_read_cgroup_v2_anon_bytes_returns_none_when_anon_absent() -> None:
    """Returns None when memory.stat lacks the 'anon' line."""
    with patch.object(Path, "read_text", return_value=_MOCK_MEMORY_STAT_NO_ANON):
        assert _read_cgroup_v2_anon_bytes() is None


def test_read_cgroup_v2_anon_bytes_returns_none_on_oserror() -> None:
    """Returns None when memory.stat is unreadable."""

    def raise_oserror(self: Path) -> str:
        raise OSError("No such file")

    with patch.object(Path, "read_text", raise_oserror):
        assert _read_cgroup_v2_anon_bytes() is None


# ---------------------------------------------------------------------------
# check_memory_usage — fail-fast (dual-condition: anon share of usage)
# ---------------------------------------------------------------------------


def _v2_full_mock(usage: str = _MOCK_USAGE_AT_96, memory_stat: str = _MOCK_MEMORY_STAT_ANON_HIGH):
    """Return a mock read_text that serves cgroup v2 current/max AND memory.stat."""

    def mock_read_text(self: Path) -> str:
        if self == _CGROUP_V2_CURRENT:
            return usage
        if self == _CGROUP_V2_MAX:
            return _MOCK_LIMIT
        if self == _CGROUP_V2_STAT:
            return memory_stat
        return ""

    return mock_read_text


def test_raises_when_cgroup_critical_and_anon_share_of_usage_above_threshold() -> None:
    """Fail-fast raises when cgroup >= 80% and anon >= 75% of current usage."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_full_mock()),
    ):
        with pytest.raises(AirbyteTracedException) as exc_info:
            monitor.check_memory_usage()
    assert exc_info.value.failure_type == FailureType.system_error
    assert "critical threshold" in (exc_info.value.message or "")
    assert "96%" in (exc_info.value.message or "")
    assert "anon share of usage" in (exc_info.value.internal_message or "")
    # Human-readable byte formatting: 960 MB usage, 1.00 GB limit, 840 MB anon.
    internal = exc_info.value.internal_message or ""
    assert "960.00 MB" in internal
    assert "1.00 GB" in internal
    assert "840.00 MB" in internal
    # Raw byte counts should no longer appear in the message.
    assert "960000000" not in internal
    assert "1000000000" not in internal


def test_no_raise_when_cgroup_critical_but_anon_share_of_usage_below_threshold(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No exception when cgroup >= 80% but anon < 75% of usage; logs once then silences."""
    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.INFO, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_full_mock(memory_stat=_MOCK_MEMORY_STAT_ANON_LOW)),
    ):
        monitor.check_memory_usage()  # Should NOT raise — logs one-shot info
        monitor.check_memory_usage()  # Should NOT log again (_critical_logged is True)
    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    # Exactly one critical-not-raising log (one-shot), plus one high-pressure-mode log
    critical_logs = [r for r in info_records if "not raising" in r.message]
    assert len(critical_logs) == 1
    assert monitor._critical_logged


def test_falls_back_to_process_rssanon_when_cgroup_v2_anon_unavailable() -> None:
    """Uses /proc/self/status RssAnon when memory.stat anon is missing, and still raises."""

    def mock_read_text(self: Path) -> str:
        if self == _CGROUP_V2_CURRENT:
            return _MOCK_USAGE_AT_96
        if self == _CGROUP_V2_MAX:
            return _MOCK_LIMIT
        if self == _CGROUP_V2_STAT:
            return _MOCK_MEMORY_STAT_NO_ANON  # anon line missing
        if self == _PROC_SELF_STATUS:
            return _MOCK_PROC_ANON_HIGH
        return ""

    monitor = MemoryMonitor(check_interval=1)
    with (
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", mock_read_text),
    ):
        with pytest.raises(AirbyteTracedException) as exc_info:
            monitor.check_memory_usage()
    assert "process RssAnon" in (exc_info.value.internal_message or "")


def test_falls_back_to_process_rssanon_low_and_does_not_raise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Uses /proc/self/status RssAnon when memory.stat anon is missing, but does not raise when below threshold."""

    def mock_read_text(self: Path) -> str:
        if self == _CGROUP_V2_CURRENT:
            return _MOCK_USAGE_AT_96
        if self == _CGROUP_V2_MAX:
            return _MOCK_LIMIT
        if self == _CGROUP_V2_STAT:
            return _MOCK_MEMORY_STAT_NO_ANON  # anon line missing
        if self == _PROC_SELF_STATUS:
            return _MOCK_PROC_ANON_LOW  # ~307 MB — 32.0% of 960 MB usage (below 85%)
        return ""

    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.INFO, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", mock_read_text),
    ):
        monitor.check_memory_usage()  # Should NOT raise
    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any("not raising" in r.message for r in info_records)


def test_no_raise_when_anonymous_memory_signal_unavailable_at_critical_usage(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Logs warning once and skips fail-fast when neither anon source is available."""

    def mock_read_text(self: Path) -> str:
        if self == _CGROUP_V2_CURRENT:
            return _MOCK_USAGE_AT_96
        if self == _CGROUP_V2_MAX:
            return _MOCK_LIMIT
        if self == _CGROUP_V2_STAT:
            raise OSError("No such file")
        if self == _PROC_SELF_STATUS:
            raise OSError("No such file")
        return ""

    monitor = MemoryMonitor(check_interval=1)
    with (
        caplog.at_level(logging.WARNING, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", mock_read_text),
    ):
        monitor.check_memory_usage()  # Should NOT raise — logs one-shot warning
        monitor.check_memory_usage()  # Should NOT log again
    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_records) == 1
    assert "anonymous memory signal unavailable" in warning_records[0].message
    assert monitor._critical_logged


def test_switches_to_high_pressure_check_interval_after_crossing_hp_threshold(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Once usage crosses 50%, the monitor tightens polling from 5000 to 5 messages."""
    monitor = MemoryMonitor(check_interval=5000)
    assert not monitor._high_pressure_mode

    with (
        caplog.at_level(logging.INFO, logger="airbyte"),
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_mock_read(usage=_MOCK_USAGE_AT_60)),
    ):
        # Pump 5000 messages to trigger the first real check
        for _ in range(5000):
            monitor.check_memory_usage()

    assert monitor._high_pressure_mode
    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any("tightening check interval" in r.message for r in info_records)

    # After switching to high-pressure mode, checks happen every 5 messages.
    # Verify by pumping 5 messages at critical usage with high anon — should raise.
    with (
        patch.object(Path, "exists", _v2_exists),
        patch.object(Path, "read_text", _v2_full_mock()),
    ):
        with pytest.raises(AirbyteTracedException):
            for _ in range(5):
                monitor.check_memory_usage()
