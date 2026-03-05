#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""Tests for airbyte_cdk.metrics MetricsClient."""

import sys
import time
import types
from unittest.mock import MagicMock, patch

import pytest

from airbyte_cdk.metrics.memory import MemoryInfo

# Ensure mock datadog module is available for tests regardless of whether
# the optional `datadog` package is installed.
_mock_dogstatsd_cls = MagicMock()

if "datadog" not in sys.modules:
    _mock_datadog = types.ModuleType("datadog")
    _mock_dogstatsd_mod = types.ModuleType("datadog.dogstatsd")
    _mock_dogstatsd_mod.DogStatsd = _mock_dogstatsd_cls  # type: ignore[attr-defined]
    _mock_datadog.dogstatsd = _mock_dogstatsd_mod  # type: ignore[attr-defined]
    sys.modules["datadog"] = _mock_datadog
    sys.modules["datadog.dogstatsd"] = _mock_dogstatsd_mod

import airbyte_cdk.metrics as metrics_module  # noqa: E402
from airbyte_cdk.metrics import MetricsClient, get_metrics_client  # noqa: E402


def _make_enabled_client() -> tuple[MetricsClient, MagicMock]:
    """Helper to create an enabled MetricsClient with a mock DogStatsd instance."""
    mock_instance = MagicMock()
    _mock_dogstatsd_cls.reset_mock()
    _mock_dogstatsd_cls.return_value = mock_instance

    client = MetricsClient()
    with (
        patch.dict("os.environ", {"DD_AGENT_HOST": "localhost"}, clear=True),
        patch("datadog.dogstatsd.DogStatsd", _mock_dogstatsd_cls),
    ):
        client.initialize()
    return client, mock_instance


class TestMetricsClientInitialization:
    def test_disabled_when_dd_agent_host_not_set(self) -> None:
        client = MetricsClient()
        with patch.dict("os.environ", {}, clear=True):
            client.initialize()
        assert not client.enabled

    def test_enabled_when_dd_agent_host_set(self) -> None:
        client, _ = _make_enabled_client()
        assert client.enabled

    def test_initialize_idempotent(self) -> None:
        client = MetricsClient()
        with patch.dict("os.environ", {}, clear=True):
            client.initialize()
            client.initialize()  # should not raise
        assert not client.enabled

    def test_disabled_when_datadog_import_fails(self) -> None:
        client = MetricsClient()
        with (
            patch.dict("os.environ", {"DD_AGENT_HOST": "localhost"}),
            patch("datadog.dogstatsd.DogStatsd", side_effect=ImportError("No module")),
        ):
            client.initialize()
        assert not client.enabled


class TestMetricsClientTags:
    def test_builds_tags_from_env(self) -> None:
        mock_instance = MagicMock()
        _mock_dogstatsd_cls.reset_mock()
        _mock_dogstatsd_cls.return_value = mock_instance

        client = MetricsClient()
        env = {
            "DD_AGENT_HOST": "localhost",
            "DD_SERVICE": "airbyte/source-github",
            "DD_VERSION": "1.2.3",
            "CONNECTION_ID": "conn-123",
            "WORKSPACE_ID": "ws-456",
        }
        with (
            patch.dict("os.environ", env, clear=True),
            patch("datadog.dogstatsd.DogStatsd", _mock_dogstatsd_cls),
        ):
            client.initialize()

        assert "connector:airbyte/source-github" in client._tags
        assert "version:1.2.3" in client._tags
        assert "connection_id:conn-123" in client._tags
        assert "workspace_id:ws-456" in client._tags


class TestMetricsClientGauge:
    def test_gauge_noop_when_disabled(self) -> None:
        client = MetricsClient()
        # Should not raise even when not initialized
        client.gauge("test.metric", 42.0)

    def test_gauge_emits_when_enabled(self) -> None:
        client, mock_instance = _make_enabled_client()

        client.gauge("test.metric", 42.0)
        mock_instance.gauge.assert_called_once_with("test.metric", 42.0, tags=client._tags)

    def test_gauge_with_extra_tags(self) -> None:
        client, mock_instance = _make_enabled_client()

        client.gauge("test.metric", 42.0, extra_tags=["stream:users"])
        call_tags = mock_instance.gauge.call_args[1]["tags"]
        assert "stream:users" in call_tags

    def test_gauge_swallows_exceptions(self) -> None:
        client, mock_instance = _make_enabled_client()
        mock_instance.gauge.side_effect = Exception("network error")

        # Should not raise
        client.gauge("test.metric", 42.0)


class TestEmitMemoryMetrics:
    def test_emits_all_metrics_when_enabled(self) -> None:
        client, mock_instance = _make_enabled_client()

        mock_info = MemoryInfo(usage_bytes=100_000_000, limit_bytes=200_000_000)
        with patch("airbyte_cdk.metrics.get_memory_info", return_value=mock_info):
            client.emit_memory_metrics()

        gauge_calls = {call[0][0]: call[0][1] for call in mock_instance.gauge.call_args_list}
        assert gauge_calls["cdk.memory.usage_bytes"] == 100_000_000.0
        assert gauge_calls["cdk.memory.limit_bytes"] == 200_000_000.0
        assert gauge_calls["cdk.memory.usage_percent"] == pytest.approx(0.5)

    def test_skips_limit_when_unknown(self) -> None:
        client, mock_instance = _make_enabled_client()

        mock_info = MemoryInfo(usage_bytes=100_000_000, limit_bytes=None)
        with patch("airbyte_cdk.metrics.get_memory_info", return_value=mock_info):
            client.emit_memory_metrics()

        metric_names = [call[0][0] for call in mock_instance.gauge.call_args_list]
        assert "cdk.memory.usage_bytes" in metric_names
        assert "cdk.memory.limit_bytes" not in metric_names
        assert "cdk.memory.usage_percent" not in metric_names

    def test_emits_python_heap_when_tracemalloc_enabled(self) -> None:
        client, mock_instance = _make_enabled_client()

        mock_info = MemoryInfo(usage_bytes=100_000_000, limit_bytes=200_000_000)
        with (
            patch("airbyte_cdk.metrics.get_memory_info", return_value=mock_info),
            patch("airbyte_cdk.metrics.get_python_heap_bytes", return_value=5_000_000),
        ):
            client.emit_memory_metrics()

        gauge_calls = {call[0][0]: call[0][1] for call in mock_instance.gauge.call_args_list}
        assert gauge_calls["cdk.memory.python_heap_bytes"] == 5_000_000.0

    def test_skips_python_heap_when_tracemalloc_disabled(self) -> None:
        client, mock_instance = _make_enabled_client()

        mock_info = MemoryInfo(usage_bytes=100_000_000, limit_bytes=200_000_000)
        with (
            patch("airbyte_cdk.metrics.get_memory_info", return_value=mock_info),
            patch("airbyte_cdk.metrics.get_python_heap_bytes", return_value=None),
        ):
            client.emit_memory_metrics()

        metric_names = [call[0][0] for call in mock_instance.gauge.call_args_list]
        assert "cdk.memory.python_heap_bytes" not in metric_names

    def test_noop_when_disabled(self) -> None:
        client = MetricsClient()
        # Should not raise
        client.emit_memory_metrics()


class TestShouldEmit:
    def test_emits_on_first_call(self) -> None:
        client = MetricsClient()
        assert client.should_emit(interval_seconds=30.0)

    def test_does_not_emit_before_interval(self) -> None:
        client = MetricsClient()
        assert client.should_emit(interval_seconds=30.0)
        assert not client.should_emit(interval_seconds=30.0)

    def test_emits_after_interval(self) -> None:
        client = MetricsClient()
        assert client.should_emit(interval_seconds=0.01)
        time.sleep(0.02)
        assert client.should_emit(interval_seconds=0.01)


class TestMaybeEmitMemoryMetrics:
    def test_emits_on_interval(self) -> None:
        client, mock_instance = _make_enabled_client()

        mock_info = MemoryInfo(usage_bytes=100, limit_bytes=200)
        with patch("airbyte_cdk.metrics.get_memory_info", return_value=mock_info):
            client.maybe_emit_memory_metrics(interval_seconds=0.0)
            first_call_count = mock_instance.gauge.call_count

            # Should not emit again immediately with a long interval
            client.maybe_emit_memory_metrics(interval_seconds=9999.0)
            assert mock_instance.gauge.call_count == first_call_count

    def test_noop_when_disabled(self) -> None:
        client = MetricsClient()
        # Should not raise
        client.maybe_emit_memory_metrics()


class TestGetMetricsClient:
    def test_returns_singleton(self) -> None:
        # Reset the singleton for test isolation
        metrics_module._metrics_client = None
        try:
            client1 = get_metrics_client()
            client2 = get_metrics_client()
            assert client1 is client2
        finally:
            metrics_module._metrics_client = None
