#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""Source-side memory introspection to log memory usage approaching container limits."""

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("airbyte")

# cgroup v2 paths
_CGROUP_V2_CURRENT = Path("/sys/fs/cgroup/memory.current")
_CGROUP_V2_MAX = Path("/sys/fs/cgroup/memory.max")

# cgroup v1 paths — TODO: remove if all deployments are confirmed cgroup v2
_CGROUP_V1_USAGE = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
_CGROUP_V1_LIMIT = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")

# Log when usage is at or above 90%
_MEMORY_THRESHOLD = 0.90

# Check interval (every N messages)
_DEFAULT_CHECK_INTERVAL = 5000


class MemoryMonitor:
    """Monitors container memory usage via cgroup files and logs warnings when usage is high.

    Lazily probes cgroup v2 then v1 files on the first call to
    ``check_memory_usage()``.  Caches which version exists.
    If neither is found (local dev / CI), all subsequent calls are instant no-ops.

    Logs a WARNING on every check interval (default 5000 messages) when memory
    usage is at or above 90% of the container limit.  This gives breadcrumb
    trails showing whether memory is climbing, plateauing, or sawtoothing.
    """

    def __init__(
        self,
        check_interval: int = _DEFAULT_CHECK_INTERVAL,
    ) -> None:
        if check_interval < 1:
            raise ValueError(f"check_interval must be >= 1, got {check_interval}")
        self._check_interval = check_interval
        self._message_count = 0
        self._cgroup_version: Optional[int] = None
        self._probed = False

    def _probe_cgroup(self) -> None:
        """Detect which cgroup version (if any) is available.

        Called lazily on the first ``check_memory_usage()`` invocation so
        that ``spec`` and ``discover`` commands never incur filesystem I/O.
        """
        if self._probed:
            return
        self._probed = True

        if _CGROUP_V2_CURRENT.exists() and _CGROUP_V2_MAX.exists():
            self._cgroup_version = 2
        elif _CGROUP_V1_USAGE.exists() and _CGROUP_V1_LIMIT.exists():
            self._cgroup_version = 1

        if self._cgroup_version is None:
            logger.debug(
                "No cgroup memory files found. Memory monitoring disabled (likely local dev / CI)."
            )

    def _read_memory(self) -> Optional[tuple[int, int]]:
        """Read current memory usage and limit from cgroup files.

        Returns a tuple of (usage_bytes, limit_bytes) or None if unavailable.
        Best-effort: failures to read memory info never crash a sync.
        """
        if self._cgroup_version is None:
            return None

        try:
            if self._cgroup_version == 2:
                usage_path = _CGROUP_V2_CURRENT
                limit_path = _CGROUP_V2_MAX
            else:
                usage_path = _CGROUP_V1_USAGE
                limit_path = _CGROUP_V1_LIMIT

            limit_text = limit_path.read_text().strip()
            # cgroup v2 memory.max can be the literal string "max" (unlimited)
            if limit_text == "max":
                return None

            usage_bytes = int(usage_path.read_text().strip())
            limit_bytes = int(limit_text)

            if limit_bytes <= 0:
                return None

            return usage_bytes, limit_bytes
        except (OSError, ValueError):
            logger.debug("Failed to read cgroup memory files; skipping memory check.")
            return None

    def check_memory_usage(self) -> None:
        """Check memory usage and log when above 90%.

        Intended to be called on every message. The monitor internally tracks
        a message counter and only reads cgroup files every ``check_interval``
        messages (default 5000) to minimise I/O overhead.

        Logs a WARNING on every check above 90% to provide breadcrumb trails
        showing memory trends over the sync lifetime.

        This method is a no-op if cgroup files are unavailable.
        """
        self._probe_cgroup()
        if self._cgroup_version is None:
            return

        self._message_count += 1
        if self._message_count % self._check_interval != 0:
            return

        memory_info = self._read_memory()
        if memory_info is None:
            return

        usage_bytes, limit_bytes = memory_info
        usage_ratio = usage_bytes / limit_bytes
        usage_percent = int(usage_ratio * 100)
        usage_gb = usage_bytes / (1024**3)
        limit_gb = limit_bytes / (1024**3)

        if usage_ratio >= _MEMORY_THRESHOLD:
            logger.warning(
                "Source memory usage at %d%% of container limit (%.2f / %.2f GB).",
                usage_percent,
                usage_gb,
                limit_gb,
            )
