#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, Optional, Union
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.partition_routers import (
    GroupingPartitionRouter,
    SubstreamPartitionRouter,
)
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import (
    ParentStreamConfig,
)
from airbyte_cdk.sources.types import StreamSlice, Record
from unit_tests.sources.declarative.partition_routers.test_substream_partition_router import (
    MockStream,
    parent_slices,
)  # Reuse MockStream and parent_slices
from unit_tests.sources.streams.concurrent.scenarios.thread_based_concurrent_stream_source_builder import \
    InMemoryPartition

_EMPTY_SLICE = StreamSlice(partition={}, cursor_slice={})


def _build_records_for_slice(records: List[Mapping[str, Any]], _slice: StreamSlice):
    return [Record(record, "stream_name", _slice) for record in records]


@pytest.fixture
def mock_config():
    return {}


@pytest.fixture
def mock_underlying_router(mock_config):
    """Fixture for a simple underlying router with predefined slices and extra fields."""
    parent_stream = MockStream(
        [
            InMemoryPartition(
                "partition_name",
                "first_stream",
                _EMPTY_SLICE,
                _build_records_for_slice(
                    [
                        {"board_id": 0, "name": "Board 0", "owner": "User0"},
                        {
                            "board_id": 0,
                            "name": "Board 0 Duplicate",
                            "owner": "User0 Duplicate",
                        },
                    ]  # Duplicate board_id
                    + [{"board_id": i, "name": f"Board {i}", "owner": f"User{i}"} for i in range(1, 5)],
                    _EMPTY_SLICE
                )
            )
        ],
        "first_stream"
    )
    return SubstreamPartitionRouter(
        parent_stream_configs=[
            ParentStreamConfig(
                stream=parent_stream,
                parent_key="board_id",
                partition_field="board_ids",
                config=mock_config,
                parameters={},
                extra_fields=[["name"], ["owner"]],
            )
        ],
        config=mock_config,
        parameters={},
    )


@pytest.fixture
def mock_underlying_router_with_parent_slices(mock_config):
    """Fixture with varied parent slices for testing non-empty parent_slice."""
    parent_stream = MockStream(
        [
            InMemoryPartition(
                "partition_1",
                "first_stream",
                parent_slices[0],
                _build_records_for_slice(
                    [{"board_id": 1, "name": "Board 1", "owner": "User1", "slice": "first"}],
                    parent_slices[0],
                ),
            ),
            InMemoryPartition(
                "partition_2",
                "first_stream",
                parent_slices[1],
                _build_records_for_slice(
                    [{"board_id": 2, "name": "Board 2", "owner": "User2", "slice": "second"}],
                    parent_slices[1],
                ),
            ),
            InMemoryPartition(
                "partition_3",
                "first_stream",
                parent_slices[2],
                _build_records_for_slice(
                    [{"board_id": 3, "name": "Board 3", "owner": "User3", "slice": "third"}],
                    parent_slices[2],
                ),
            ),
        ],
        "first_stream"
    )
    return SubstreamPartitionRouter(
        parent_stream_configs=[
            ParentStreamConfig(
                stream=parent_stream,
                parent_key="board_id",
                partition_field="board_ids",
                config=mock_config,
                parameters={},
                extra_fields=[["name"], ["owner"]],
            )
        ],
        config=mock_config,
        parameters={},
    )


@pytest.mark.parametrize(
    "group_size, deduplicate, expected_slices",
    [
        (
            2,
            True,
            [
                StreamSlice(
                    partition={"board_ids": [0, 1], "parent_slice": [{}, {}]},
                    cursor_slice={},
                    extra_fields={"name": ["Board 0", "Board 1"], "owner": ["User0", "User1"]},
                ),
                StreamSlice(
                    partition={"board_ids": [2, 3], "parent_slice": [{}, {}]},
                    cursor_slice={},
                    extra_fields={"name": ["Board 2", "Board 3"], "owner": ["User2", "User3"]},
                ),
                StreamSlice(
                    partition={"board_ids": [4], "parent_slice": [{}]},
                    cursor_slice={},
                    extra_fields={"name": ["Board 4"], "owner": ["User4"]},
                ),
            ],
        ),
        (
            3,
            True,
            [
                StreamSlice(
                    partition={"board_ids": [0, 1, 2], "parent_slice": [{}, {}, {}]},
                    cursor_slice={},
                    extra_fields={
                        "name": ["Board 0", "Board 1", "Board 2"],
                        "owner": ["User0", "User1", "User2"],
                    },
                ),
                StreamSlice(
                    partition={"board_ids": [3, 4], "parent_slice": [{}, {}]},
                    cursor_slice={},
                    extra_fields={"name": ["Board 3", "Board 4"], "owner": ["User3", "User4"]},
                ),
            ],
        ),
        (
            2,
            False,
            [
                StreamSlice(
                    partition={"board_ids": [0, 0], "parent_slice": [{}, {}]},
                    cursor_slice={},
                    extra_fields={
                        "name": ["Board 0", "Board 0 Duplicate"],
                        "owner": ["User0", "User0 Duplicate"],
                    },
                ),
                StreamSlice(
                    partition={"board_ids": [1, 2], "parent_slice": [{}, {}]},
                    cursor_slice={},
                    extra_fields={"name": ["Board 1", "Board 2"], "owner": ["User1", "User2"]},
                ),
                StreamSlice(
                    partition={"board_ids": [3, 4], "parent_slice": [{}, {}]},
                    cursor_slice={},
                    extra_fields={"name": ["Board 3", "Board 4"], "owner": ["User3", "User4"]},
                ),
            ],
        ),
    ],
    ids=["group_size_2_deduplicate", "group_size_3_deduplicate", "group_size_2_no_deduplicate"],
)
def test_stream_slices_grouping(
    mock_config, mock_underlying_router, group_size, deduplicate, expected_slices
):
    """Test basic grouping behavior with different group sizes and deduplication settings."""
    router = GroupingPartitionRouter(
        group_size=group_size,
        underlying_partition_router=mock_underlying_router,
        deduplicate=deduplicate,
        config=mock_config,
    )
    slices = list(router.stream_slices())
    assert slices == expected_slices


def test_stream_slices_empty_underlying_router(mock_config):
    """Test behavior when the underlying router yields no slices."""
    parent_stream = MockStream(
        [
            InMemoryPartition(
                "partition_name",
                "first_stream",
                _EMPTY_SLICE,
                [],
            )
        ],
        "first_stream"
    )
    underlying_router = SubstreamPartitionRouter(
        parent_stream_configs=[
            ParentStreamConfig(
                stream=parent_stream,
                parent_key="board_id",
                partition_field="board_ids",
                config=mock_config,
                parameters={},
                extra_fields=[["name"]],
            )
        ],
        config=mock_config,
        parameters={},
    )
    router = GroupingPartitionRouter(
        group_size=2,
        underlying_partition_router=underlying_router,
        config=mock_config,
    )
    slices = list(router.stream_slices())
    assert slices == []


def test_stream_slices_lazy_iteration(mock_config, mock_underlying_router):
    """Test that stream_slices processes partitions lazily, iterating the underlying router as an iterator."""

    # Custom iterator to track yields and simulate underlying stream_slices
    class ControlledIterator:
        def __init__(self):
            self.slices = [
                StreamSlice(
                    partition={"board_ids": 0},
                    cursor_slice={},
                    extra_fields={"name": "Board 0", "owner": "User0"},
                ),
                StreamSlice(
                    partition={"board_ids": 1},
                    cursor_slice={},
                    extra_fields={"name": "Board 1", "owner": "User1"},
                ),
                StreamSlice(
                    partition={"board_ids": 2},
                    cursor_slice={},
                    extra_fields={"name": "Board 2", "owner": "User2"},
                ),
                StreamSlice(
                    partition={"board_ids": 3},
                    cursor_slice={},
                    extra_fields={"name": "Board 3", "owner": "User3"},
                ),
                StreamSlice(
                    partition={"board_ids": 4},
                    cursor_slice={},
                    extra_fields={"name": "Board 4", "owner": "User4"},
                ),
            ]
            self.index = 0
            self.yield_count = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.index < len(self.slices):
                self.yield_count += 1
                slice = self.slices[self.index]
                self.index += 1
                return slice
            raise StopIteration

    # Replace the underlying router's stream_slices with the controlled iterator
    controlled_iter = ControlledIterator()
    mock_underlying_router.stream_slices = MagicMock(return_value=controlled_iter)

    router = GroupingPartitionRouter(
        group_size=2,
        underlying_partition_router=mock_underlying_router,
        config=mock_config,
        deduplicate=True,
    )
    slices_iter = router.stream_slices()

    # Before iteration, no slices should be yielded
    assert controlled_iter.yield_count == 0, "No slices should be yielded before iteration starts"

    # Get the first slice
    first_slice = next(slices_iter)
    assert first_slice == StreamSlice(
        partition={"board_ids": [0, 1]},
        cursor_slice={},
        extra_fields={"name": ["Board 0", "Board 1"], "owner": ["User0", "User1"]},
    )
    assert controlled_iter.yield_count == 2, (
        "Only 2 slices should be yielded to form the first group"
    )

    # Get the second slice
    second_slice = next(slices_iter)
    assert second_slice == StreamSlice(
        partition={"board_ids": [2, 3]},
        cursor_slice={},
        extra_fields={"name": ["Board 2", "Board 3"], "owner": ["User2", "User3"]},
    )
    assert controlled_iter.yield_count == 4, (
        "Only 4 slices should be yielded up to the second group"
    )

    # Exhaust the iterator
    remaining_slices = list(slices_iter)
    assert remaining_slices == [
        StreamSlice(
            partition={"board_ids": [4]},
            cursor_slice={},
            extra_fields={"name": ["Board 4"], "owner": ["User4"]},
        )
    ]
    assert controlled_iter.yield_count == 5, (
        "All 5 slices should be yielded after exhausting the iterator"
    )


def test_set_initial_state_delegation(mock_config, mock_underlying_router):
    """Test that set_initial_state delegates to the underlying router."""
    router = GroupingPartitionRouter(
        group_size=2,
        underlying_partition_router=mock_underlying_router,
        config=mock_config,
    )
    mock_state = {"some_key": "some_value"}
    mock_underlying_router.set_initial_state = MagicMock()

    router.set_initial_state(mock_state)
    mock_underlying_router.set_initial_state.assert_called_once_with(mock_state)


def test_stream_slices_extra_fields_varied(mock_config):
    """Test grouping with varied extra fields across partitions."""
    parent_stream = MockStream(
        [
            InMemoryPartition(
                "partition_name",
                "first_stream",
                _EMPTY_SLICE,
                _build_records_for_slice(
                    [
                        {"board_id": 1, "name": "Board 1", "owner": "User1"},
                        {"board_id": 2, "name": "Board 2"},  # Missing owner
                        {"board_id": 3, "owner": "User3"},  # Missing name
                    ],
                    _EMPTY_SLICE,
                ),
            )
        ],
        "first_stream"
    )
    underlying_router = SubstreamPartitionRouter(
        parent_stream_configs=[
            ParentStreamConfig(
                stream=parent_stream,
                parent_key="board_id",
                partition_field="board_ids",
                config=mock_config,
                parameters={},
                extra_fields=[["name"], ["owner"]],
            )
        ],
        config=mock_config,
        parameters={},
    )
    router = GroupingPartitionRouter(
        group_size=2,
        underlying_partition_router=underlying_router,
        config=mock_config,
        deduplicate=True,
    )
    expected_slices = [
        StreamSlice(
            partition={"board_ids": [1, 2], "parent_slice": [{}, {}]},
            cursor_slice={},
            extra_fields={"name": ["Board 1", "Board 2"], "owner": ["User1", None]},
        ),
        StreamSlice(
            partition={"board_ids": [3], "parent_slice": [{}]},
            cursor_slice={},
            extra_fields={"name": [None], "owner": ["User3"]},
        ),
    ]
    slices = list(router.stream_slices())
    assert slices == expected_slices


def test_grouping_with_complex_partitions_and_extra_fields(mock_config):
    """Test grouping with partitions containing multiple keys and extra fields."""
    parent_stream = MockStream(
        [
            InMemoryPartition(
                "partition_name",
                "first_stream",
                _EMPTY_SLICE,
                _build_records_for_slice(
                    [{"board_id": i, "extra": f"extra_{i}", "name": f"Board {i}"} for i in range(3)],
                    _EMPTY_SLICE,
                ),
            )
        ],
        "first_stream"
    )
    underlying_router = SubstreamPartitionRouter(
        parent_stream_configs=[
            ParentStreamConfig(
                stream=parent_stream,
                parent_key="board_id",
                partition_field="board_ids",
                config=mock_config,
                parameters={},
                extra_fields=[["extra"], ["name"]],
            )
        ],
        config=mock_config,
        parameters={},
    )
    router = GroupingPartitionRouter(
        group_size=2,
        underlying_partition_router=underlying_router,
        config=mock_config,
    )
    expected_slices = [
        StreamSlice(
            partition={"board_ids": [0, 1], "parent_slice": [{}, {}]},
            cursor_slice={},
            extra_fields={"extra": ["extra_0", "extra_1"], "name": ["Board 0", "Board 1"]},
        ),
        StreamSlice(
            partition={"board_ids": [2], "parent_slice": [{}]},
            cursor_slice={},
            extra_fields={"extra": ["extra_2"], "name": ["Board 2"]},
        ),
    ]
    slices = list(router.stream_slices())
    assert slices == expected_slices


def test_stream_slices_with_non_empty_parent_slice(
    mock_config, mock_underlying_router_with_parent_slices
):
    """Test grouping with non-empty parent_slice values from the underlying router."""
    router = GroupingPartitionRouter(
        group_size=2,
        underlying_partition_router=mock_underlying_router_with_parent_slices,
        config=mock_config,
        deduplicate=True,
    )
    expected_slices = [
        StreamSlice(
            partition={
                "board_ids": [1, 2],
                "parent_slice": [{"slice": "first"}, {"slice": "second"}],
            },
            cursor_slice={},
            extra_fields={"name": ["Board 1", "Board 2"], "owner": ["User1", "User2"]},
        ),
        StreamSlice(
            partition={"board_ids": [3], "parent_slice": [{"slice": "third"}]},
            cursor_slice={},
            extra_fields={"name": ["Board 3"], "owner": ["User3"]},
        ),
    ]
    slices = list(router.stream_slices())
    assert slices == expected_slices


def test_get_request_params_default(mock_config, mock_underlying_router):
    """Test that get_request_params returns an empty dict by default."""
    router = GroupingPartitionRouter(
        group_size=2,
        underlying_partition_router=mock_underlying_router,
        config=mock_config,
    )
    params = router.get_request_params(
        stream_slice=StreamSlice(
            partition={"board_ids": [1, 2], "parent_slice": [{}, {}]}, cursor_slice={}
        )
    )
    assert params == {}


def test_stream_slices_resume_from_state(mock_config, mock_underlying_router):
    """Test that stream_slices resumes correctly from a previous state."""

    # Simulate underlying router state handling
    class MockPartitionRouter:
        def __init__(self):
            self.slices = [
                StreamSlice(
                    partition={"board_ids": i},
                    cursor_slice={},
                    extra_fields={"name": f"Board {i}", "owner": f"User{i}"},
                )
                for i in range(5)
            ]
            self.state = {"last_board_id": 0}  # Initial state

        def set_initial_state(self, state):
            self.state = state

        def get_stream_state(self):
            return self.state

        def stream_slices(self):
            last_board_id = self.state.get("last_board_id", -1)
            for slice in self.slices:
                board_id = slice.partition["board_ids"]
                if board_id <= last_board_id:
                    continue
                self.state = {"last_board_id": board_id}
                yield slice

    underlying_router = MockPartitionRouter()
    router = GroupingPartitionRouter(
        group_size=2,
        underlying_partition_router=underlying_router,
        config=mock_config,
        deduplicate=True,
    )

    # First sync: process first two slices
    router.set_initial_state({"last_board_id": 0})
    slices_iter = router.stream_slices()
    first_batch = next(slices_iter)
    assert first_batch == StreamSlice(
        partition={"board_ids": [1, 2]},
        cursor_slice={},
        extra_fields={"name": ["Board 1", "Board 2"], "owner": ["User1", "User2"]},
    )
    state_after_first = router.get_stream_state()
    assert state_after_first == {"last_board_id": 2}, "State should reflect last processed board_id"

    # Simulate a new sync resuming from the previous state
    new_router = GroupingPartitionRouter(
        group_size=2,
        underlying_partition_router=MockPartitionRouter(),
        config=mock_config,
        deduplicate=True,
    )
    new_router.set_initial_state(state_after_first)
    resumed_slices = list(new_router.stream_slices())
    assert resumed_slices == [
        StreamSlice(
            partition={"board_ids": [3, 4]},
            cursor_slice={},
            extra_fields={"name": ["Board 3", "Board 4"], "owner": ["User3", "User4"]},
        )
    ], "Should resume from board_id 3"
