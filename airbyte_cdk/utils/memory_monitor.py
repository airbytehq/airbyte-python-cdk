#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""Source-side memory introspection with fail-fast shutdown on memory threshold."""

import logging
import os
from pathlib import Path
from typing import Optional

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

logger = logging.getLogger("airbyte")

# cgroup v2 paths
_CGROUP_V2_CURRENT = Path("/sys/fs/cgroup/memory.current")
_CGROUP_V2_MAX = Path("/sys/fs/cgroup/memory.max")

# cgroup v1 paths — TODO: remove if all deployments are confirmed cgroup v2
_CGROUP_V1_USAGE = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
_CGROUP_V1_LIMIT = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")

# Process-level RSS from /proc/self/status (Linux only, no extra dependency)
_PROC_SELF_STATUS = Path("/proc/self/status")

# Log when usage is at or above 90%
_MEMORY_THRESHOLD = 0.90

# Raise AirbyteTracedException when BOTH conditions are met:
#   1. cgroup usage >= critical threshold
#   2. process RSS >= RSS threshold of the container limit
# This dual-condition avoids false positives from reclaimable kernel page cache.
_CRITICAL_THRESHOLD = 0.95
_RSS_THRESHOLD = 0.80

# Check interval (every N messages)
_DEFAULT_CHECK_INTERVAL = 5000

# Environment variable to disable fail-fast (set to "false" to disable)
_ENV_FAIL_FAST = "AIRBYTE_MEMORY_FAIL_FAST"


def _read_process_rss_bytes() -> Optional[int]:
    """Read current process RSS from /proc/self/status.

    Returns RSS in bytes, or None if unavailable (non-Linux, permission error, etc.).
    """
    try:
        status_text = _PROC_SELF_STATUS.read_text()
        for line in status_text.splitlines():
            if line.startswith("VmRSS:"):
                # Format: "VmRSS:     12345 kB"
                parts = line.split()
                if len(parts) >= 2:
                    return int(parts[1]) * 1024  # Convert kB to bytes
        return None
    except (OSError, ValueError):
        return None


class MemoryMonitor:
    """Monitors container memory usage via cgroup files and raises on critical pressure.

    Lazily probes cgroup v2 then v1 files on the first call to
    ``check_memory_usage()``.  Caches which version exists.
    If neither is found (local dev / CI), all subsequent calls are instant no-ops.

    **Logging (always active):** Logs a WARNING on every check interval (default
    5000 messages) when cgroup memory usage is at or above 90% of the container
    limit.

    **Fail-fast (controlled by ``AIRBYTE_MEMORY_FAIL_FAST`` env var, default
    enabled):** Raises ``AirbyteTracedException`` with
    ``FailureType.system_error`` when *both*:

    1. Cgroup usage >= 95% of the container limit (container is near OOM-kill)
    2. Process RSS >= 80% of the container limit (pressure is from non-reclaimable
       Python heap, not elastic kernel page cache)

    This dual-condition avoids false positives from SQLite page cache or other
    kernel-reclaimable memory that inflates cgroup usage but does not represent
    real memory pressure.
    """

    def __init__(
        self,
        check_interval: int = _DEFAULT_CHECK_INTERVAL,
        fail_fast: Optional[bool] = None,
    ) -> None:
        if check_interval < 1:
            raise ValueError(f"check_interval must be >= 1, got {check_interval}")
        self._check_interval = check_interval
        self._message_count = 0
        self._cgroup_version: Optional[int] = None
        self._probed = False

        # Resolve fail-fast setting: explicit arg > env var > default (True)
        if fail_fast is not None:
            self._fail_fast = fail_fast
        else:
            env_val = os.environ.get(_ENV_FAIL_FAST, "true").strip().lower()
            self._fail_fast = env_val != "false"

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
        """Check memory usage; log at 90% and raise at critical dual-condition.

        Intended to be called on every message. The monitor internally tracks
        a message counter and only reads cgroup files every ``check_interval``
        messages (default 5000) to minimise I/O overhead.

        **Logging:** WARNING on every check above 90%.

        **Fail-fast (when enabled):** If cgroup usage >= 95% *and* process RSS
        >= 80% of the container limit, raises ``AirbyteTracedException`` with
        ``FailureType.system_error`` so the platform receives a clear error
        message instead of an opaque OOM-kill.

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

        # Fail-fast: dual-condition check
        if self._fail_fast and usage_ratio >= _CRITICAL_THRESHOLD:
            rss_bytes = _read_process_rss_bytes()
            if rss_bytes is not None:
                rss_ratio = rss_bytes / limit_bytes
                rss_percent = int(rss_ratio * 100)
                if rss_ratio >= _RSS_THRESHOLD:
                    raise AirbyteTracedException(
                        message=f"Source memory usage exceeded critical threshold ({usage_percent}% of container limit).",
                        internal_message=(
                            f"Cgroup memory: {usage_bytes} / {limit_bytes} bytes ({usage_percent}%). "
                            f"Process RSS: {rss_bytes} bytes ({rss_percent}% of limit). "
                            f"Thresholds: cgroup >= {int(_CRITICAL_THRESHOLD * 100)}%, "
                            f"RSS >= {int(_RSS_THRESHOLD * 100)}%."
                        ),
                        failure_type=FailureType.system_error,
                    )
                else:
                    logger.info(
                        "Cgroup usage at %d%% but process RSS only %d%% of limit; "
                        "pressure likely from reclaimable page cache — not raising.",
                        usage_percent,
                        rss_percent,
                    )
            else:
                # Cannot read process RSS (non-Linux?) — fall back to cgroup-only
                raise AirbyteTracedException(
                    message=f"Source memory usage exceeded critical threshold ({usage_percent}% of container limit).",
                    internal_message=(
                        f"Cgroup memory: {usage_bytes} / {limit_bytes} bytes ({usage_percent}%). "
                        f"Process RSS unavailable (non-Linux environment). "
                        f"Cgroup threshold: >= {int(_CRITICAL_THRESHOLD * 100)}%."
                    ),
                    failure_type=FailureType.system_error,
                )
