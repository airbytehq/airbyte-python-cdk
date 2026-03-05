#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
Metrics module for Python source connectors.

Provides DogStatsD-based metric emission for memory and resource monitoring.
Designed to be a graceful no-op when DD_AGENT_HOST is not set (local dev, CI).
"""

import logging
import os
import time
from typing import Any, Optional

from airbyte_cdk.metrics.memory import MemoryInfo, get_memory_info

logger = logging.getLogger(__name__)

# Metric names
METRIC_MEMORY_USAGE_BYTES = "cdk.memory.usage_bytes"
METRIC_MEMORY_LIMIT_BYTES = "cdk.memory.limit_bytes"
METRIC_MEMORY_USAGE_PERCENT = "cdk.memory.usage_percent"

# Default emission interval in seconds
DEFAULT_EMISSION_INTERVAL_SECONDS = 30.0


class MetricsClient:
    """
    DogStatsD metrics client for Python source connectors.

    Initializes a DogStatsD client when DD_AGENT_HOST is available,
    otherwise all metric calls are silent no-ops.
    """

    def __init__(self) -> None:
        self._statsd: Any = None
        self._tags: list[str] = []
        self._last_emission_time: float = 0.0
        self._initialized = False

    def initialize(self) -> None:
        """
        Initialize the DogStatsD client if DD_AGENT_HOST is available.

        Should be called once during connector startup. Safe to call multiple times.
        """
        if self._initialized:
            return

        self._initialized = True
        dd_agent_host = os.environ.get("DD_AGENT_HOST")
        if not dd_agent_host:
            logger.debug("DD_AGENT_HOST not set; metrics emission disabled")
            return

        port_str = os.environ.get("DD_DOGSTATSD_PORT")
        if not port_str:
            dd_dogstatsd_port = 8125
        else:
            try:
                dd_dogstatsd_port = int(port_str)
            except ValueError:
                logger.warning(
                    "Invalid DD_DOGSTATSD_PORT value %r; falling back to default port 8125",
                    port_str,
                )
                dd_dogstatsd_port = 8125

        try:
            from datadog.dogstatsd import DogStatsd

            self._statsd = DogStatsd(
                host=dd_agent_host,
                port=dd_dogstatsd_port,
                # Disable telemetry to reduce overhead
                disable_telemetry=True,
            )
            logger.info(
                "DogStatsD metrics client initialized (host=%s, port=%d)",
                dd_agent_host,
                dd_dogstatsd_port,
            )
        except ImportError:
            logger.warning(
                "datadog package not installed; metrics emission disabled. "
                "Install with: pip install datadog"
            )
        except Exception:
            logger.warning(
                "Failed to initialize DogStatsD client; metrics emission disabled", exc_info=True
            )

        # Build standard tags from environment
        self._tags = self._build_tags()

    @property
    def enabled(self) -> bool:
        """Return True if the DogStatsD client is active and ready to emit metrics."""
        return self._statsd is not None

    def _build_tags(self) -> list[str]:
        """Build standard metric tags from environment variables."""
        tags: list[str] = []

        # DD_SERVICE and DD_VERSION are set by ConnectorApmSupportHelper
        dd_service = os.environ.get("DD_SERVICE")
        if dd_service:
            tags.append(f"connector:{dd_service}")

        dd_version = os.environ.get("DD_VERSION")
        if dd_version:
            tags.append(f"version:{dd_version}")

        # Connection-level tags from platform env vars
        connection_id = os.environ.get("CONNECTION_ID")
        if connection_id:
            tags.append(f"connection_id:{connection_id}")

        workspace_id = os.environ.get("WORKSPACE_ID")
        if workspace_id:
            tags.append(f"workspace_id:{workspace_id}")

        return tags

    def gauge(self, metric_name: str, value: float, extra_tags: Optional[list[str]] = None) -> None:
        """
        Emit a gauge metric via DogStatsD.

        No-op if the client is not initialized or DD_AGENT_HOST is not set.
        """
        if self._statsd is None:
            return

        tags = self._tags + (extra_tags or [])
        try:
            # _statsd is a DogStatsd instance set during initialize(); call gauge directly
            self._statsd.gauge(metric_name, value, tags=tags)
        except Exception:
            # Never let metric emission failures affect the sync
            logger.debug("Failed to emit metric %s", metric_name, exc_info=True)

    def emit_memory_metrics(self) -> None:
        """
        Read and emit all memory-related metrics.

        Emits:
        - cdk.memory.usage_bytes: Current container memory usage
        - cdk.memory.limit_bytes: Container memory limit (if known)
        - cdk.memory.usage_percent: Usage/limit ratio (if limit is known)

        Also updates the last-emission timestamp so that subsequent calls to
        ``should_emit`` / ``maybe_emit_memory_metrics`` respect the interval.
        """
        if not self.enabled:
            return

        # Update the last-emission timestamp to avoid duplicate snapshots
        self._last_emission_time = time.monotonic()

        try:
            info: MemoryInfo = get_memory_info()

            self.gauge(METRIC_MEMORY_USAGE_BYTES, float(info.usage_bytes))

            if info.limit_bytes is not None:
                self.gauge(METRIC_MEMORY_LIMIT_BYTES, float(info.limit_bytes))

            if info.usage_percent is not None:
                self.gauge(METRIC_MEMORY_USAGE_PERCENT, info.usage_percent)

        except Exception:
            # Never let metric collection failures affect the sync
            logger.debug("Failed to collect memory metrics", exc_info=True)

    def should_emit(self, interval_seconds: float = DEFAULT_EMISSION_INTERVAL_SECONDS) -> bool:
        """
        Check if enough time has passed since the last emission to emit again.

        Returns True if at least interval_seconds have elapsed since the last emission.
        """
        now = time.monotonic()
        if now - self._last_emission_time >= interval_seconds:
            self._last_emission_time = now
            return True
        return False

    def maybe_emit_memory_metrics(
        self, interval_seconds: float = DEFAULT_EMISSION_INTERVAL_SECONDS
    ) -> None:
        """
        Emit memory metrics if the emission interval has elapsed.

        This is the primary method to call periodically during read() — it handles
        both the timing check and the metric emission.
        """
        if self.enabled and self.should_emit(interval_seconds):
            self.emit_memory_metrics()


# Module-level singleton for convenience
_metrics_client: Optional[MetricsClient] = None


def get_metrics_client() -> MetricsClient:
    """
    Get or create the module-level MetricsClient singleton.

    Note: The caller is responsible for calling ``initialize()`` on the
    returned client before emitting metrics.  Construction and initialization
    are separate so that the caller controls *when* environment variables
    are read and the DogStatsD connection is established.
    """
    global _metrics_client
    if _metrics_client is None:
        _metrics_client = MetricsClient()
    return _metrics_client
