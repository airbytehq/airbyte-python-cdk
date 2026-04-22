#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""Source-side memory introspection with fail-fast shutdown on memory threshold."""

import logging
from pathlib import Path
from typing import Optional

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

logger = logging.getLogger("airbyte")

# cgroup v2 paths
_CGROUP_V2_CURRENT = Path("/sys/fs/cgroup/memory.current")
_CGROUP_V2_MAX = Path("/sys/fs/cgroup/memory.max")
_CGROUP_V2_STAT = Path("/sys/fs/cgroup/memory.stat")

# cgroup v1 paths — TODO: remove if all deployments are confirmed cgroup v2
_CGROUP_V1_USAGE = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
_CGROUP_V1_LIMIT = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")

# Process-level anonymous RSS from /proc/self/status (Linux only, no extra dependency)
_PROC_SELF_STATUS = Path("/proc/self/status")


def _format_bytes(num_bytes: int) -> str:
    """Render a byte count as a short human-readable string with 2 decimals.

    Uses decimal units (GB = 10^9, MB = 10^6, KB = 10^3) so that raw cgroup
    byte values render close to the way operators describe container limits
    (e.g. a 2_147_483_648-byte limit renders as ``2.15 GB`` rather than the
    binary ``2.00 GiB``).  Values below 1 KB are rendered as plain bytes.
    """
    if num_bytes >= 1_000_000_000:
        return f"{num_bytes / 1_000_000_000:.2f} GB"
    if num_bytes >= 1_000_000:
        return f"{num_bytes / 1_000_000:.2f} MB"
    if num_bytes >= 1_000:
        return f"{num_bytes / 1_000:.2f} KB"
    return f"{num_bytes} B"


# Raise AirbyteTracedException when BOTH conditions are met:
#   1. cgroup usage >= critical threshold
#   2. anonymous memory >= anon-share threshold of *current cgroup usage*
# Comparing anon to usage (not limit) answers the more relevant question:
# "is most of the near-OOM memory actually process-owned anonymous memory?"
#
# Thresholds are deliberately set below the OOM cliff to leave headroom for
# the check-interval race window: between two checks, allocations can jump
# a container past any gate directly into kernel OOM-kill. Firing the fail-
# fast trace well before the cliff is what makes the failure visible to the
# platform instead of appearing as a silent exit.
# WARNING: These values are deliberately aggressive for the test branch only.
# DO NOT MERGE. See PR description for the paired production values.
_CRITICAL_THRESHOLD = 0.80
_ANON_SHARE_OF_USAGE_THRESHOLD = 0.75

# Check interval (every N messages) — tightens after crossing high-pressure threshold
_DEFAULT_CHECK_INTERVAL = 100
_HIGH_PRESSURE_CHECK_INTERVAL = 5
_HIGH_PRESSURE_THRESHOLD = 0.50


def _read_cgroup_v2_anon_bytes() -> Optional[int]:
    """Read cgroup-level anonymous memory from ``/sys/fs/cgroup/memory.stat``.

    The ``anon`` field in ``memory.stat`` accounts for all anonymous pages
    charged to the cgroup, which is a more accurate view of process-private
    memory pressure than per-process ``RssAnon`` in multi-process containers.

    Returns anonymous bytes, or ``None`` if unavailable or malformed.
    """
    try:
        for line in _CGROUP_V2_STAT.read_text().splitlines():
            if line.startswith("anon "):
                return int(line.split()[1])
    except (OSError, ValueError):
        return None
    return None


def _read_process_anon_rss_bytes() -> Optional[int]:
    """Read process-private anonymous resident memory from /proc/self/status.

    Parses the ``RssAnon`` field which represents private anonymous pages — the
    closest proxy for Python-heap memory pressure.  Unlike ``VmRSS`` (which is
    ``RssAnon + RssFile + RssShmem``), ``RssAnon`` is not inflated by mmap'd
    file-backed or shared resident pages.

    Returns anonymous RSS in bytes, or None if unavailable (non-Linux,
    permission error, or ``RssAnon`` field not present in the kernel).
    """
    try:
        status_text = _PROC_SELF_STATUS.read_text()
        for line in status_text.splitlines():
            if line.startswith("RssAnon:"):
                # Format: "RssAnon:     12345 kB"
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

    **Logging (event-based, not periodic):**

    - One INFO when high-pressure mode activates (usage first crosses 50%)
    - One INFO/WARNING when critical threshold (80%) is crossed but we do
      *not* raise (either anon share is below the fail-fast gate or the
      anonymous memory signal is unavailable)
    - No repeated per-check warnings — logging is driven by state
      transitions, not periodic sampling

    **High-pressure polling:** Once cgroup usage first crosses 50%, the check
    interval permanently tightens from the configured ``check_interval``
    (default 100) to 5 messages to narrow the race window near OOM.

    **Fail-fast:** Raises ``AirbyteTracedException`` with
    ``FailureType.system_error`` when *both*:

    1. Cgroup usage >= 80% of the container limit (container is near OOM-kill)
    2. Anonymous memory >= 75% of *current cgroup usage* (most of the charged
       memory is process-private anonymous pages, not file-backed cache)

    The anonymous memory signal is read from cgroup v2 ``memory.stat`` (``anon``
    field) when available, falling back to ``/proc/self/status`` ``RssAnon``.
    Comparing anonymous memory to current usage (not the container limit) answers
    the more relevant question: "is most of the near-OOM memory actually
    process-owned?"  This avoids the brittleness of comparing to the full limit
    where anonymous memory can dominate usage yet still fall short of a
    limit-based percentage threshold.

    If the anonymous memory signal is unavailable, the monitor logs a warning
    and skips fail-fast rather than falling back to cgroup-only raising.
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
        self._high_pressure_mode = False
        self._critical_logged = False
        logger.info(
            "MemoryMonitor instantiated with critical threshold: %d%%, "
            "anon share of usage threshold: %d%%, high-pressure threshold: %d%%, "
            "check interval: %d messages (tightens to %d under high pressure).",
            int(_CRITICAL_THRESHOLD * 100),
            int(_ANON_SHARE_OF_USAGE_THRESHOLD * 100),
            int(_HIGH_PRESSURE_THRESHOLD * 100),
            self._check_interval,
            _HIGH_PRESSURE_CHECK_INTERVAL,
        )

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

    def _read_anon_bytes(self) -> Optional[tuple[int, str]]:
        """Read anonymous memory bytes from the best available source.

        Tries cgroup v2 ``memory.stat`` (``anon`` field) first, then falls back
        to ``/proc/self/status`` ``RssAnon``.  Returns ``(bytes, source_label)``
        or ``None`` if neither is available.
        """
        if self._cgroup_version == 2:
            cgroup_anon = _read_cgroup_v2_anon_bytes()
            if cgroup_anon is not None:
                return cgroup_anon, "cgroup memory.stat anon"

        proc_anon = _read_process_anon_rss_bytes()
        if proc_anon is not None:
            return proc_anon, "process RssAnon"

        return None

    def check_memory_usage(self) -> None:
        """Check memory usage and raise at critical dual-condition.

        Intended to be called on every message. The monitor internally tracks
        a message counter and only reads cgroup files every ``check_interval``
        messages (default 100). Once usage crosses 50%, the interval tightens
        to 5 messages for the remainder of the sync regardless of the
        configured ``check_interval``.

        Logging is event-based (one-shot on state transitions), not periodic.

        This method is a no-op if cgroup files are unavailable.
        """
        self._probe_cgroup()
        if self._cgroup_version is None:
            return

        self._message_count += 1
        interval = (
            _HIGH_PRESSURE_CHECK_INTERVAL if self._high_pressure_mode else self._check_interval
        )
        if self._message_count % interval != 0:
            return

        memory_info = self._read_memory()
        if memory_info is None:
            return

        usage_bytes, limit_bytes = memory_info
        usage_ratio = usage_bytes / limit_bytes
        usage_percent = int(usage_ratio * 100)

        if usage_ratio >= _HIGH_PRESSURE_THRESHOLD and not self._high_pressure_mode:
            self._high_pressure_mode = True
            logger.info(
                "Memory usage crossed %d%%; tightening check interval from %d to %d messages.",
                int(_HIGH_PRESSURE_THRESHOLD * 100),
                self._check_interval,
                _HIGH_PRESSURE_CHECK_INTERVAL,
            )

        # Fail-fast: dual-condition check
        if usage_ratio >= _CRITICAL_THRESHOLD:
            anon_info = self._read_anon_bytes()
            if anon_info is not None:
                anon_bytes, anon_source = anon_info
                anon_share = anon_bytes / usage_bytes
                if anon_share >= _ANON_SHARE_OF_USAGE_THRESHOLD:
                    raise AirbyteTracedException(
                        message=f"Source memory usage exceeded critical threshold ({usage_percent}% of container limit).",
                        internal_message=(
                            f"Cgroup memory: {_format_bytes(usage_bytes)} / "
                            f"{_format_bytes(limit_bytes)} ({usage_percent}%). "
                            f"Anonymous memory ({anon_source}): {_format_bytes(anon_bytes)} "
                            f"({int(anon_share * 100)}% of current cgroup usage). "
                            f"Thresholds: cgroup >= {int(_CRITICAL_THRESHOLD * 100)}%, "
                            f"anon share of usage >= {int(_ANON_SHARE_OF_USAGE_THRESHOLD * 100)}%."
                        ),
                        failure_type=FailureType.system_error,
                    )
                elif not self._critical_logged:
                    self._critical_logged = True
                    logger.info(
                        "Cgroup usage crossed %d%% (%s of %s) but anonymous memory is only %d%% of current cgroup usage; not raising.",
                        int(_CRITICAL_THRESHOLD * 100),
                        _format_bytes(usage_bytes),
                        _format_bytes(limit_bytes),
                        int(anon_share * 100),
                    )
            elif not self._critical_logged:
                self._critical_logged = True
                logger.warning(
                    "Cgroup usage crossed %d%% (%s of %s) but anonymous memory signal unavailable; skipping fail-fast.",
                    int(_CRITICAL_THRESHOLD * 100),
                    _format_bytes(usage_bytes),
                    _format_bytes(limit_bytes),
                )
