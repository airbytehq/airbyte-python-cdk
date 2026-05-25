#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock, patch

import pytest

from airbyte_cdk.sources.file_based.file_based_source import (
    DEFAULT_CONCURRENCY,
    MAX_CONCURRENCY,
    FileBasedSource,
)


class _ConcreteFBSource(FileBasedSource):
    """Minimal concrete subclass so we can instantiate FileBasedSource."""

    _concurrency_level = None

    @property
    def name(self) -> str:
        return "test-source"

    def check_connection(self, logger, config):
        return True, None

    def streams(self, config):
        return []


@pytest.mark.parametrize(
    "concurrency_level, expected_num_workers, expected_initial_partitions",
    [
        pytest.param(None, MAX_CONCURRENCY, MAX_CONCURRENCY // 2, id="none_uses_max"),
        pytest.param(100, 100, 50, id="default_concurrency"),
        pytest.param(20, 20, 10, id="reduced_concurrency"),
        pytest.param(2, 2, 1, id="minimal_concurrency"),
        pytest.param(200, MAX_CONCURRENCY, MAX_CONCURRENCY // 2, id="capped_at_max"),
    ],
)
def test_concurrency_level_controls_thread_pool_size(
    concurrency_level, expected_num_workers, expected_initial_partitions
):
    _ConcreteFBSource._concurrency_level = concurrency_level

    with patch(
        "airbyte_cdk.sources.file_based.file_based_source.ConcurrentSource.create"
    ) as mock_create:
        mock_create.return_value = MagicMock()
        try:
            _ConcreteFBSource(
                stream_reader=MagicMock(),
                spec_class=MagicMock(),
                catalog=None,
                config=None,
                state=None,
            )
        except Exception:
            pass  # Other init errors are fine; we only care about the ConcurrentSource.create call

        mock_create.assert_called_once()
        call_args = mock_create.call_args
        actual_num_workers = call_args[0][0]
        actual_initial_partitions = call_args[0][1]
        assert actual_num_workers == expected_num_workers
        assert actual_initial_partitions == expected_initial_partitions
