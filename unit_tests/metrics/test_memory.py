#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""Tests for airbyte_cdk.metrics.memory module."""

from pathlib import Path
from unittest.mock import patch

import pytest

from airbyte_cdk.metrics.memory import (
    MemoryInfo,
    _read_cgroup_v1_memory,
    _read_cgroup_v2_memory,
    _read_rusage_memory,
    get_memory_info,
    get_python_heap_bytes,
)


class TestMemoryInfo:
    def test_usage_percent_with_limit(self) -> None:
        info = MemoryInfo(usage_bytes=500, limit_bytes=1000)
        assert info.usage_percent == 0.5

    def test_usage_percent_no_limit(self) -> None:
        info = MemoryInfo(usage_bytes=500, limit_bytes=None)
        assert info.usage_percent is None

    def test_usage_percent_zero_limit(self) -> None:
        info = MemoryInfo(usage_bytes=500, limit_bytes=0)
        assert info.usage_percent is None

    def test_frozen_dataclass(self) -> None:
        info = MemoryInfo(usage_bytes=100, limit_bytes=200)
        with pytest.raises(AttributeError):
            info.usage_bytes = 300  # type: ignore[misc]


class TestCgroupV2Memory:
    def test_reads_memory_current_and_max(self, tmp_path: Path) -> None:
        current_file = tmp_path / "memory.current"
        max_file = tmp_path / "memory.max"
        current_file.write_text("104857600\n")
        max_file.write_text("209715200\n")

        with (
            patch("airbyte_cdk.metrics.memory.CGROUP_V2_MEMORY_CURRENT", current_file),
            patch("airbyte_cdk.metrics.memory.CGROUP_V2_MEMORY_MAX", max_file),
        ):
            info = _read_cgroup_v2_memory()

        assert info is not None
        assert info.usage_bytes == 104857600
        assert info.limit_bytes == 209715200
        assert info.usage_percent == pytest.approx(0.5)

    def test_memory_max_is_max_string(self, tmp_path: Path) -> None:
        """When cgroup reports 'max', it means no limit is set."""
        current_file = tmp_path / "memory.current"
        max_file = tmp_path / "memory.max"
        current_file.write_text("104857600\n")
        max_file.write_text("max\n")

        with (
            patch("airbyte_cdk.metrics.memory.CGROUP_V2_MEMORY_CURRENT", current_file),
            patch("airbyte_cdk.metrics.memory.CGROUP_V2_MEMORY_MAX", max_file),
        ):
            info = _read_cgroup_v2_memory()

        assert info is not None
        assert info.usage_bytes == 104857600
        assert info.limit_bytes is None
        assert info.usage_percent is None

    def test_returns_none_when_files_missing(self) -> None:
        with patch(
            "airbyte_cdk.metrics.memory.CGROUP_V2_MEMORY_CURRENT", Path("/nonexistent/path")
        ):
            info = _read_cgroup_v2_memory()
        assert info is None

    def test_handles_invalid_content(self, tmp_path: Path) -> None:
        current_file = tmp_path / "memory.current"
        current_file.write_text("not_a_number\n")

        with patch("airbyte_cdk.metrics.memory.CGROUP_V2_MEMORY_CURRENT", current_file):
            info = _read_cgroup_v2_memory()
        assert info is None


class TestCgroupV1Memory:
    def test_reads_usage_and_limit(self, tmp_path: Path) -> None:
        usage_file = tmp_path / "memory.usage_in_bytes"
        limit_file = tmp_path / "memory.limit_in_bytes"
        usage_file.write_text("104857600\n")
        limit_file.write_text("209715200\n")

        with (
            patch("airbyte_cdk.metrics.memory.CGROUP_V1_MEMORY_USAGE", usage_file),
            patch("airbyte_cdk.metrics.memory.CGROUP_V1_MEMORY_LIMIT", limit_file),
        ):
            info = _read_cgroup_v1_memory()

        assert info is not None
        assert info.usage_bytes == 104857600
        assert info.limit_bytes == 209715200

    def test_very_large_limit_treated_as_no_limit(self, tmp_path: Path) -> None:
        """Very large cgroup v1 limits (near 2^63) indicate no real limit."""
        usage_file = tmp_path / "memory.usage_in_bytes"
        limit_file = tmp_path / "memory.limit_in_bytes"
        usage_file.write_text("104857600\n")
        limit_file.write_text(f"{2**63}\n")

        with (
            patch("airbyte_cdk.metrics.memory.CGROUP_V1_MEMORY_USAGE", usage_file),
            patch("airbyte_cdk.metrics.memory.CGROUP_V1_MEMORY_LIMIT", limit_file),
        ):
            info = _read_cgroup_v1_memory()

        assert info is not None
        assert info.limit_bytes is None

    def test_returns_none_when_files_missing(self) -> None:
        with patch("airbyte_cdk.metrics.memory.CGROUP_V1_MEMORY_USAGE", Path("/nonexistent/path")):
            info = _read_cgroup_v1_memory()
        assert info is None


class TestRusageMemory:
    def test_returns_memory_info(self) -> None:
        info = _read_rusage_memory()
        assert info.usage_bytes > 0
        assert info.limit_bytes is None


class TestGetMemoryInfo:
    def test_prefers_cgroup_v2(self, tmp_path: Path) -> None:
        current_file = tmp_path / "memory.current"
        max_file = tmp_path / "memory.max"
        current_file.write_text("100\n")
        max_file.write_text("200\n")

        with (
            patch("airbyte_cdk.metrics.memory.CGROUP_V2_MEMORY_CURRENT", current_file),
            patch("airbyte_cdk.metrics.memory.CGROUP_V2_MEMORY_MAX", max_file),
        ):
            info = get_memory_info()

        assert info.usage_bytes == 100
        assert info.limit_bytes == 200

    def test_falls_back_to_cgroup_v1(self, tmp_path: Path) -> None:
        usage_file = tmp_path / "memory.usage_in_bytes"
        limit_file = tmp_path / "memory.limit_in_bytes"
        usage_file.write_text("300\n")
        limit_file.write_text("600\n")

        with (
            patch("airbyte_cdk.metrics.memory.CGROUP_V2_MEMORY_CURRENT", Path("/nonexistent")),
            patch("airbyte_cdk.metrics.memory.CGROUP_V1_MEMORY_USAGE", usage_file),
            patch("airbyte_cdk.metrics.memory.CGROUP_V1_MEMORY_LIMIT", limit_file),
        ):
            info = get_memory_info()

        assert info.usage_bytes == 300
        assert info.limit_bytes == 600

    def test_falls_back_to_rusage(self) -> None:
        with (
            patch("airbyte_cdk.metrics.memory.CGROUP_V2_MEMORY_CURRENT", Path("/nonexistent")),
            patch("airbyte_cdk.metrics.memory.CGROUP_V1_MEMORY_USAGE", Path("/nonexistent")),
        ):
            info = get_memory_info()

        assert info.usage_bytes > 0
        assert info.limit_bytes is None


class TestGetPythonHeapBytes:
    def test_returns_integer(self) -> None:
        result = get_python_heap_bytes()
        assert result is not None
        assert isinstance(result, int)
        assert result >= 0
