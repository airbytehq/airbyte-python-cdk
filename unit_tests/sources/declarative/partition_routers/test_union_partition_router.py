#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.partition_routers import (
    ListPartitionRouter,
    UnionPartitionRouter,
)
from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.types import StreamSlice


class _StaticPartitionRouter(PartitionRouter):
    """A test partition router that emits a fixed list of slices and exposes a fixed state."""

    def __init__(self, slices, state=None):
        self._slices = slices
        self._state = state

    def stream_slices(self):
        yield from self._slices

    def get_request_params(self, *, stream_state=None, stream_slice=None, next_page_token=None):
        return {}

    def get_request_headers(self, *, stream_state=None, stream_slice=None, next_page_token=None):
        return {}

    def get_request_body_data(self, *, stream_state=None, stream_slice=None, next_page_token=None):
        return {}

    def get_request_body_json(self, *, stream_state=None, stream_slice=None, next_page_token=None):
        return {}

    def get_stream_state(self):
        return self._state


def _list_router(values, cursor_field="repository"):
    return ListPartitionRouter(values=values, cursor_field=cursor_field, config={}, parameters={})


def test_stream_slices_deduplicates_across_children():
    router = UnionPartitionRouter(
        partition_routers=[
            _list_router(["org/a", "org/b"]),
            _list_router(["org/b", "org/c"]),
        ],
        partition_field="repository",
        parameters={},
    )

    slices = list(router.stream_slices())

    assert slices == [
        StreamSlice(partition={"repository": "org/a"}, cursor_slice={}),
        StreamSlice(partition={"repository": "org/b"}, cursor_slice={}),
        StreamSlice(partition={"repository": "org/c"}, cursor_slice={}),
    ]


def test_stream_slices_first_occurrence_wins():
    child_a = _StaticPartitionRouter(
        [
            StreamSlice(
                partition={"repository": "org/b"},
                cursor_slice={},
                extra_fields={"origin": "a"},
            )
        ]
    )
    child_b = _StaticPartitionRouter(
        [
            StreamSlice(
                partition={"repository": "org/b"},
                cursor_slice={},
                extra_fields={"origin": "b"},
            )
        ]
    )

    slices = list(
        UnionPartitionRouter(
            partition_routers=[child_a, child_b],
            partition_field="repository",
            parameters={},
        ).stream_slices()
    )

    assert len(slices) == 1
    assert slices[0].extra_fields["origin"] == "a"


def test_stream_slices_normalizes_partition_and_moves_extra_keys_to_extra_fields():
    substream_like_child = _StaticPartitionRouter(
        [
            StreamSlice(
                partition={"repository": "org/a", "parent_slice": {"organization": "org"}},
                cursor_slice={},
                extra_fields={"full_name": "org/a"},
            )
        ]
    )

    slices = list(
        UnionPartitionRouter(
            partition_routers=[substream_like_child],
            partition_field="repository",
            parameters={},
        ).stream_slices()
    )

    assert slices[0].partition == {"repository": "org/a"}
    assert slices[0].extra_fields == {
        "full_name": "org/a",
        "parent_slice": {"organization": "org"},
    }


def test_stream_slices_raises_when_child_does_not_emit_partition_field():
    child = _StaticPartitionRouter(
        [StreamSlice(partition={"other_field": "value"}, cursor_slice={})]
    )
    router = UnionPartitionRouter(
        partition_routers=[child], partition_field="repository", parameters={}
    )

    with pytest.raises(ValueError, match="partition field 'repository'"):
        list(router.stream_slices())


def test_nested_union_partition_router():
    inner = UnionPartitionRouter(
        partition_routers=[
            _list_router(["org/a"]),
            _list_router(["org/b"]),
        ],
        partition_field="repository",
        parameters={},
    )
    outer = UnionPartitionRouter(
        partition_routers=[inner, _list_router(["org/b", "org/c"])],
        partition_field="repository",
        parameters={},
    )

    assert [s.partition for s in outer.stream_slices()] == [
        {"repository": "org/a"},
        {"repository": "org/b"},
        {"repository": "org/c"},
    ]


@pytest.mark.parametrize(
    "child_states, expected_state",
    [
        pytest.param([None, None], None, id="all_children_without_state"),
        pytest.param(
            [{"parent_a": {"updated_at": "2024-01-01"}}, None],
            {"parent_a": {"updated_at": "2024-01-01"}},
            id="one_child_with_state",
        ),
        pytest.param(
            [
                {"parent_a": {"updated_at": "2024-01-01"}},
                {"parent_b": {"updated_at": "2024-02-01"}},
            ],
            {
                "parent_a": {"updated_at": "2024-01-01"},
                "parent_b": {"updated_at": "2024-02-01"},
            },
            id="merges_states_of_all_children",
        ),
    ],
)
def test_get_stream_state_merges_children_states(child_states, expected_state):
    router = UnionPartitionRouter(
        partition_routers=[_StaticPartitionRouter([], state=state) for state in child_states],
        partition_field="repository",
        parameters={},
    )

    assert router.get_stream_state() == expected_state


def test_request_options_are_empty():
    router = UnionPartitionRouter(
        partition_routers=[_list_router(["org/a"])],
        partition_field="repository",
        parameters={},
    )
    stream_slice = StreamSlice(partition={"repository": "org/a"}, cursor_slice={})

    assert router.get_request_params(stream_slice=stream_slice) == {}
    assert router.get_request_headers(stream_slice=stream_slice) == {}
    assert router.get_request_body_data(stream_slice=stream_slice) == {}
    assert router.get_request_body_json(stream_slice=stream_slice) == {}
