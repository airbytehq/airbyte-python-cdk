#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""Source-side memory introspection to emit controlled error messages before OOM kills."""

import logging
from pathlib import Path
from typing import Optional

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

logger = logging.getLogger("airbyte")

# cgroup v2 paths
_CGROUP_V2_CURRENT = Path("/sys/fs/cgroup/memory.current")
_CGROUP_V2_MAX = Path("/sys/fs/cgroup/memory.max")

# cgroup v1 paths
_CGROUP_V1_USAGE = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
_CGROUP_V1_LIMIT = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")

# Default thresholds
_DEFAULT_WARNING_THRESHOLD = 0.85
_DEFAULT_CRITICAL_THRESHOLD = 0.95

# Check interval (every N messages)
DEFAULT_CHECK_INTERVAL = 1000


class MemoryLimitExceeded(AirbyteTracedException):
    """Raised when connector memory usage exceeds critical threshold."""

    pass


class MemoryMonitor:
    """Monitors container memory usage via cgroup files and emits warnings before OOM kills.

    Lazily probes cgroup v2 then v1 files on the first call to
    ``check_memory_usage()``.  Caches which version exists.
    If neither is found (local dev / CI), all subsequent calls are instant no-ops.
    """

    def __init__(
        self,
        warning_threshold: float = _DEFAULT_WARNING_THRESHOLD,
        critical_threshold: float = _DEFAULT_CRITICAL_THRESHOLD,
        check_interval: int = DEFAULT_CHECK_INTERVAL,
    ) -> None:
        self._warning_threshold = warning_threshold
        self._critical_threshold = critical_threshold
        self._check_interval = check_interval
        self._message_count = 0
        self._warning_emitted = False
        self._critical_raised = False
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
        """Check memory usage against thresholds.

        Intended to be called on every message. The monitor internally tracks
        a message counter and only reads cgroup files every ``check_interval``
        messages (default 1000) to minimise I/O overhead.

        At the warning threshold (default 85%), logs a warning message.
        At the critical threshold (default 95%), raises MemoryLimitExceeded to
        trigger a graceful shutdown with an actionable error message.

        Each threshold triggers at most once per sync to avoid log spam.
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

        if usage_ratio >= self._critical_threshold and not self._critical_raised:
            self._critical_raised = True
            raise MemoryLimitExceeded(
                internal_message=f"Memory usage is {usage_percent}% ({usage_bytes} / {limit_bytes} bytes). "
                f"Critical threshold is {int(self._critical_threshold * 100)}%.",
                message=f"Source exceeded memory limit ({usage_percent}% used) and must shut down to avoid an out-of-memory crash.",
                failure_type=FailureType.system_error,
            )

        if usage_ratio >= self._warning_threshold and not self._warning_emitted:
            self._warning_emitted = True
            logger.warning(
                "Source memory usage reached %d%% of container limit (%d / %d bytes).",
                usage_percent,
                usage_bytes,
                limit_bytes,
            )
