#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
Metrics module for Python source connectors.

Provides DogStatsD-based metric emission for memory and resource monitoring.
Designed to be a graceful no-op when DD_AGENT_HOST is not set (local dev, CI).
"""

from airbyte_cdk.metrics.memory import MemoryInfo, get_memory_info
from airbyte_cdk.metrics.metrics_client import (
    DEFAULT_EMISSION_INTERVAL_SECONDS,
    METRIC_MEMORY_LIMIT_BYTES,
    METRIC_MEMORY_USAGE_BYTES,
    MetricsClient,
    get_metrics_client,
    reset_metrics_client,
)

__all__ = [
    "DEFAULT_EMISSION_INTERVAL_SECONDS",
    "METRIC_MEMORY_LIMIT_BYTES",
    "METRIC_MEMORY_USAGE_BYTES",
    "MemoryInfo",
    "MetricsClient",
    "get_memory_info",
    "get_metrics_client",
    "reset_metrics_client",
]
