#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
Memory and resource metrics reader for Python source connectors.

Reads container memory usage and limits from cgroup v2 files (standard in K8s pods),
with fallback to resource.getrusage for non-containerized environments.
"""

import logging
import resource
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# cgroup v2 file paths (standard in modern K8s pods)
CGROUP_V2_MEMORY_CURRENT = Path("/sys/fs/cgroup/memory.current")
CGROUP_V2_MEMORY_MAX = Path("/sys/fs/cgroup/memory.max")

# cgroup v1 file paths (legacy, some older environments)
CGROUP_V1_MEMORY_USAGE = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
CGROUP_V1_MEMORY_LIMIT = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")

# "max" in cgroup v2 means no limit is set
CGROUP_NO_LIMIT = "max"

# Threshold for considering a cgroup v1 limit as "no limit" (very large values ~= PAGE_COUNTER_MAX)
# Values near 2^63 typically mean no limit is configured
CGROUP_V1_NO_LIMIT_THRESHOLD = 2**62


@dataclass(frozen=True)
class MemoryInfo:
    """Container memory usage information."""

    usage_bytes: int
    limit_bytes: Optional[int]

    @property
    def usage_percent(self) -> Optional[float]:
        """Return memory usage as a fraction of the limit (0.0 to 1.0), or None if no limit is known."""
        if self.limit_bytes is not None and self.limit_bytes > 0:
            return self.usage_bytes / self.limit_bytes
        return None


def _read_cgroup_file(path: Path) -> Optional[str]:
    """Read a cgroup file and return its stripped contents, or None if unavailable."""
    try:
        return path.read_text().strip()
    except (FileNotFoundError, PermissionError, OSError):
        return None


def _read_cgroup_v2_memory() -> Optional[MemoryInfo]:
    """
    Read memory usage from cgroup v2 files.

    Returns MemoryInfo if cgroup v2 files are available, None otherwise.
    """
    usage_str = _read_cgroup_file(CGROUP_V2_MEMORY_CURRENT)
    if usage_str is None:
        return None

    try:
        usage_bytes = int(usage_str)
    except ValueError:
        logger.debug("Could not parse cgroup v2 memory.current value: %s", usage_str)
        return None

    limit_bytes: Optional[int] = None
    max_str = _read_cgroup_file(CGROUP_V2_MEMORY_MAX)
    if max_str is not None and max_str != CGROUP_NO_LIMIT:
        try:
            limit_bytes = int(max_str)
        except ValueError:
            logger.debug("Could not parse cgroup v2 memory.max value: %s", max_str)

    return MemoryInfo(usage_bytes=usage_bytes, limit_bytes=limit_bytes)


def _read_cgroup_v1_memory() -> Optional[MemoryInfo]:
    """
    Read memory usage from cgroup v1 files (legacy fallback).

    Returns MemoryInfo if cgroup v1 files are available, None otherwise.
    """
    usage_str = _read_cgroup_file(CGROUP_V1_MEMORY_USAGE)
    if usage_str is None:
        return None

    try:
        usage_bytes = int(usage_str)
    except ValueError:
        logger.debug("Could not parse cgroup v1 memory usage value: %s", usage_str)
        return None

    limit_bytes: Optional[int] = None
    limit_str = _read_cgroup_file(CGROUP_V1_MEMORY_LIMIT)
    if limit_str is not None:
        try:
            raw_limit = int(limit_str)
            # Very large values indicate no real limit is set
            if raw_limit < CGROUP_V1_NO_LIMIT_THRESHOLD:
                limit_bytes = raw_limit
        except ValueError:
            logger.debug("Could not parse cgroup v1 memory limit value: %s", limit_str)

    return MemoryInfo(usage_bytes=usage_bytes, limit_bytes=limit_bytes)


def _read_rusage_memory() -> MemoryInfo:
    """
    Fallback: read memory usage via resource.getrusage (works in non-containerized environments).

    Note: ru_maxrss is in kilobytes on Linux, bytes on macOS.
    This fallback cannot determine the container memory limit.
    """
    rusage = resource.getrusage(resource.RUSAGE_SELF)
    # ru_maxrss is in kilobytes on Linux, but bytes on macOS
    if sys.platform == "darwin":
        usage_bytes = rusage.ru_maxrss
    else:
        usage_bytes = rusage.ru_maxrss * 1024
    return MemoryInfo(usage_bytes=usage_bytes, limit_bytes=None)


def get_memory_info() -> MemoryInfo:
    """
    Get current container memory usage information.

    Attempts to read from (in order):
    1. cgroup v2 files (modern K8s pods)
    2. cgroup v1 files (legacy environments)
    3. resource.getrusage (local dev, CI)

    Returns a MemoryInfo dataclass with usage_bytes, limit_bytes, and usage_percent.
    """
    # Try cgroup v2 first (most common in modern K8s)
    info = _read_cgroup_v2_memory()
    if info is not None:
        return info

    # Try cgroup v1 (legacy)
    info = _read_cgroup_v1_memory()
    if info is not None:
        return info

    # Fallback to rusage
    return _read_rusage_memory()
