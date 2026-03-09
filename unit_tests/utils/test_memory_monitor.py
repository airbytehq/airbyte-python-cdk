#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from pathlib import Path
from unittest.mock import patch

import pytest

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.memory_monitor import (
    DEFAULT_CHECK_INTERVAL,
    MemoryLimitExceeded,
    MemoryMonitor,
    _CGROUP_V1_LIMIT,
    _CGROUP_V1_USAGE,
    _CGROUP_V2_CURRENT,
    _CGROUP_V2_MAX,
)


class TestMemoryMonitorInit:
    """Tests for MemoryMonitor initialization and cgroup detection."""

    def test_no_cgroup_files_disables_monitoring(self, tmp_path: Path) -> None:
        """When no cgroup files exist, monitoring should be disabled (no-op)."""
        with patch.object(Path, "exists", return_value=False):
            monitor = MemoryMonitor()
        assert monitor._cgroup_version is None

    def test_cgroup_v2_detected(self, tmp_path: Path) -> None:
        """When cgroup v2 files exist, version should be 2."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V2_CURRENT, _CGROUP_V2_MAX)

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor()
        assert monitor._cgroup_version == 2

    def test_cgroup_v1_detected(self, tmp_path: Path) -> None:
        """When only cgroup v1 files exist, version should be 1."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V1_USAGE, _CGROUP_V1_LIMIT)

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor()
        assert monitor._cgroup_version == 1

    def test_cgroup_v2_preferred_over_v1(self) -> None:
        """When both cgroup v2 and v1 files exist, v2 should be preferred."""
        with patch.object(Path, "exists", return_value=True):
            monitor = MemoryMonitor()
        assert monitor._cgroup_version == 2


class TestMemoryMonitorCheckMemory:
    """Tests for the check_memory_usage method."""

    def test_noop_when_no_cgroup(self) -> None:
        """check_memory_usage should be a no-op when cgroup is unavailable."""
        with patch.object(Path, "exists", return_value=False):
            monitor = MemoryMonitor()
        # Should not raise
        monitor.check_memory_usage()

    def test_noop_when_limit_is_max(self) -> None:
        """When cgroup v2 memory.max is 'max' (unlimited), should be a no-op."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V2_CURRENT, _CGROUP_V2_MAX)

        def mock_read_text(self: Path) -> str:
            if self == _CGROUP_V2_CURRENT:
                return "1000000\n"
            if self == _CGROUP_V2_MAX:
                return "max\n"
            return ""

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor()

        with patch.object(Path, "read_text", mock_read_text):
            # Should not raise even though "usage" is technically present
            monitor.check_memory_usage()

    def test_no_warning_below_threshold(self) -> None:
        """No warning should be emitted when usage is below 85%."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V2_CURRENT, _CGROUP_V2_MAX)

        def mock_read_text(self: Path) -> str:
            if self == _CGROUP_V2_CURRENT:
                return "500000000\n"  # 500MB
            if self == _CGROUP_V2_MAX:
                return "1000000000\n"  # 1GB
            return ""

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor()

        with patch.object(Path, "read_text", mock_read_text):
            monitor.check_memory_usage()

        assert not monitor._warning_emitted
        assert not monitor._critical_raised

    def test_warning_at_85_percent(self) -> None:
        """Warning should be emitted at 85% usage."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V2_CURRENT, _CGROUP_V2_MAX)

        def mock_read_text(self: Path) -> str:
            if self == _CGROUP_V2_CURRENT:
                return "870000000\n"  # 870MB = 87% of 1GB
            if self == _CGROUP_V2_MAX:
                return "1000000000\n"  # 1GB
            return ""

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor()

        with patch.object(Path, "read_text", mock_read_text):
            monitor.check_memory_usage()

        assert monitor._warning_emitted
        assert not monitor._critical_raised

    def test_critical_at_95_percent_raises(self) -> None:
        """MemoryLimitExceeded should be raised at 95% usage."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V2_CURRENT, _CGROUP_V2_MAX)

        def mock_read_text(self: Path) -> str:
            if self == _CGROUP_V2_CURRENT:
                return "960000000\n"  # 960MB = 96% of 1GB
            if self == _CGROUP_V2_MAX:
                return "1000000000\n"  # 1GB
            return ""

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor()

        with patch.object(Path, "read_text", mock_read_text):
            with pytest.raises(MemoryLimitExceeded) as exc_info:
                monitor.check_memory_usage()

        assert exc_info.value.failure_type == FailureType.transient_error
        assert "96%" in (exc_info.value.message or "")
        assert "shut down" in (exc_info.value.message or "")

    def test_warning_emitted_only_once(self) -> None:
        """Warning should only be emitted once even if called multiple times."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V2_CURRENT, _CGROUP_V2_MAX)

        def mock_read_text(self: Path) -> str:
            if self == _CGROUP_V2_CURRENT:
                return "870000000\n"  # 87%
            if self == _CGROUP_V2_MAX:
                return "1000000000\n"
            return ""

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor()

        with patch.object(Path, "read_text", mock_read_text):
            monitor.check_memory_usage()
            assert monitor._warning_emitted

            # Call again — should not error or re-emit
            monitor.check_memory_usage()
            assert monitor._warning_emitted

    def test_critical_raised_only_once(self) -> None:
        """MemoryLimitExceeded should only be raised once."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V2_CURRENT, _CGROUP_V2_MAX)

        def mock_read_text(self: Path) -> str:
            if self == _CGROUP_V2_CURRENT:
                return "960000000\n"
            if self == _CGROUP_V2_MAX:
                return "1000000000\n"
            return ""

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor()

        with patch.object(Path, "read_text", mock_read_text):
            with pytest.raises(MemoryLimitExceeded):
                monitor.check_memory_usage()

            # Second call should NOT raise again
            monitor.check_memory_usage()

    def test_cgroup_v1_reading(self) -> None:
        """Memory reading should work with cgroup v1 paths."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V1_USAGE, _CGROUP_V1_LIMIT)

        def mock_read_text(self: Path) -> str:
            if self == _CGROUP_V1_USAGE:
                return "870000000\n"  # 87%
            if self == _CGROUP_V1_LIMIT:
                return "1000000000\n"
            return ""

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor()
        assert monitor._cgroup_version == 1

        with patch.object(Path, "read_text", mock_read_text):
            monitor.check_memory_usage()

        assert monitor._warning_emitted

    def test_custom_thresholds_warning(self) -> None:
        """Custom warning threshold should be respected."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V2_CURRENT, _CGROUP_V2_MAX)

        def mock_read_text(self: Path) -> str:
            if self == _CGROUP_V2_CURRENT:
                return "750000000\n"  # 75%
            if self == _CGROUP_V2_MAX:
                return "1000000000\n"
            return ""

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor(warning_threshold=0.70, critical_threshold=0.90)

        with patch.object(Path, "read_text", mock_read_text):
            monitor.check_memory_usage()

        assert monitor._warning_emitted
        assert not monitor._critical_raised

    def test_custom_thresholds_critical(self) -> None:
        """Custom critical threshold should be respected."""

        def mock_exists(self: Path) -> bool:
            return self in (_CGROUP_V2_CURRENT, _CGROUP_V2_MAX)

        def mock_read_text(self: Path) -> str:
            if self == _CGROUP_V2_CURRENT:
                return "850000000\n"  # 85%
            if self == _CGROUP_V2_MAX:
                return "1000000000\n"
            return ""

        with patch.object(Path, "exists", mock_exists):
            monitor = MemoryMonitor(warning_threshold=0.70, critical_threshold=0.80)

        with patch.object(Path, "read_text", mock_read_text):
            with pytest.raises(MemoryLimitExceeded):
                monitor.check_memory_usage()


class TestMemoryLimitExceeded:
    """Tests for the MemoryLimitExceeded exception."""

    def test_is_airbyte_traced_exception(self) -> None:
        """MemoryLimitExceeded should be a subclass of AirbyteTracedException."""
        from airbyte_cdk.utils.traced_exception import AirbyteTracedException

        exc = MemoryLimitExceeded(
            internal_message="test",
            message="test message",
            failure_type=FailureType.transient_error,
        )
        assert isinstance(exc, AirbyteTracedException)

    def test_default_attributes(self) -> None:
        """MemoryLimitExceeded should have correct default attributes."""
        exc = MemoryLimitExceeded(
            internal_message="Memory at 96%",
            message="Source exceeded memory limit.",
            failure_type=FailureType.transient_error,
        )
        assert exc.failure_type == FailureType.transient_error
        assert exc.message == "Source exceeded memory limit."
        assert exc.internal_message == "Memory at 96%"


class TestDefaultCheckInterval:
    """Tests for the DEFAULT_CHECK_INTERVAL constant."""

    def test_check_interval_is_positive(self) -> None:
        assert DEFAULT_CHECK_INTERVAL > 0

    def test_check_interval_value(self) -> None:
        assert DEFAULT_CHECK_INTERVAL == 1000
