#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""Review regressions for PR 1234 at c51857fa; six cases fail on that commit."""

from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    GroupingPartitionRouter,
    ListPartitionRouter,
    UnionPartitionRouter,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)
from unit_tests.sources.declarative.incremental.test_sync_token_cursor_coverage import (
    DATETIME_CURSOR,
    TOKEN_CURSOR,
    _parent_child_manifest,
)
from unit_tests.sources.declarative.incremental.test_sync_token_cursor_integration import (
    _incoming_state,
)


def wrap_router(child, wrapper):
    router = child["retriever"]["partition_router"]
    if wrapper == "union":
        child["retriever"]["partition_router"] = {
            "type": "UnionPartitionRouter",
            "partition_field": "account",
            "partition_routers": [
                router,
                {"type": "ListPartitionRouter", "cursor_field": "account", "values": ["fallback"]},
            ],
        }
    elif wrapper == "grouping":
        child["retriever"]["partition_router"] = {
            "type": "GroupingPartitionRouter",
            "group_size": 2,
            "underlying_partition_router": router,
        }


@pytest.mark.parametrize("wrapper", ["direct", "union", "grouping"])
@pytest.mark.parametrize("has_saved_state", [False, True])
def test_token_parent_dependency_is_rejected_through_wrappers(wrapper, has_saved_state):
    manifest = _parent_child_manifest(
        parent_sync=TOKEN_CURSOR,
        child_sync=DATETIME_CURSOR,
        incremental_dependency=True,
    )
    child = manifest["streams"][1]
    wrap_router(child, wrapper)
    manifest["streams"] = [child]
    state = _incoming_state({"created_at": "2024-01-01T00:00:00Z"}) if has_saved_state else None
    source = ConcurrentDeclarativeSource(
        source_config=manifest, config={}, catalog=None, state=state
    )
    with pytest.raises(ValueError, match="SyncTokenCursor parent"):
        source.discover(logger=MagicMock(), config={})


@pytest.mark.parametrize("wrapper", ["direct", "union"])
@pytest.mark.parametrize("setting", ["incremental_dependency", "lazy_read_pointer"])
def test_token_child_checks_reach_union_members(wrapper, setting):
    manifest = _parent_child_manifest(
        parent_sync=DATETIME_CURSOR,
        child_sync=TOKEN_CURSOR,
        incremental_dependency=setting == "incremental_dependency",
    )
    child = manifest["streams"][1]
    if setting == "lazy_read_pointer":
        child["retriever"]["partition_router"]["parent_stream_configs"][0][setting] = ["items"]
    wrap_router(child, wrapper)
    manifest["streams"] = [child]
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)
    with pytest.raises(ValueError, match=setting):
        source.discover(logger=MagicMock(), config={})


def _wrap_router_in_grouping_over_union(child):
    router = child["retriever"]["partition_router"]
    child["retriever"]["partition_router"] = {
        "type": "GroupingPartitionRouter",
        "group_size": 2,
        "underlying_partition_router": {
            "type": "UnionPartitionRouter",
            "partition_field": "account",
            "partition_routers": [
                router,
                {"type": "ListPartitionRouter", "cursor_field": "account", "values": ["fallback"]},
            ],
        },
    }


def test_token_parent_dependency_is_rejected_through_grouping_and_union():
    manifest = _parent_child_manifest(
        parent_sync=TOKEN_CURSOR,
        child_sync=DATETIME_CURSOR,
        incremental_dependency=True,
    )
    child = manifest["streams"][1]
    _wrap_router_in_grouping_over_union(child)
    manifest["streams"] = [child]
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)

    with pytest.raises(ValueError, match="SyncTokenCursor parent"):
        source.discover(logger=MagicMock(), config={})


def test_token_child_rejects_grouping_wrapping_union_and_substream():
    manifest = _parent_child_manifest(
        parent_sync=DATETIME_CURSOR,
        child_sync=TOKEN_CURSOR,
        incremental_dependency=True,
    )
    child = manifest["streams"][1]
    _wrap_router_in_grouping_over_union(child)
    manifest["streams"] = [child]
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)

    with pytest.raises(ValueError, match="GroupingPartitionRouter"):
        source.discover(logger=MagicMock(), config={})


def test_token_child_rejects_reserved_parent_partition_field_through_union():
    manifest = _parent_child_manifest(parent_sync=DATETIME_CURSOR, child_sync=TOKEN_CURSOR)
    child = manifest["streams"][1]
    child["retriever"]["partition_router"]["parent_stream_configs"][0]["partition_field"] = "token"
    wrap_router(child, "union")
    manifest["streams"] = [child]
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)

    with pytest.raises(ValueError, match="parent partition field 'token'"):
        source.discover(logger=MagicMock(), config={})


def test_token_child_union_with_valid_substream_and_list_router_builds_and_discovers():
    manifest = _parent_child_manifest(parent_sync=DATETIME_CURSOR, child_sync=TOKEN_CURSOR)
    child = manifest["streams"][1]
    wrap_router(child, "union")
    manifest["streams"] = [child]
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)

    discovered = source.discover(logger=MagicMock(), config={})

    assert [stream.name for stream in discovered.streams] == ["events"]


def test_datetime_child_with_incremental_dependency_through_union_is_valid():
    manifest = _parent_child_manifest(
        parent_sync=DATETIME_CURSOR,
        child_sync=DATETIME_CURSOR,
        incremental_dependency=True,
    )
    child = manifest["streams"][1]
    wrap_router(child, "union")
    manifest["streams"] = [child]
    source = ConcurrentDeclarativeSource(source_config=manifest, config={}, catalog=None)

    discovered = source.discover(logger=MagicMock(), config={})

    assert [stream.name for stream in discovered.streams] == ["events"]


def test_flatten_partition_router_models_returns_empty_list_for_none():
    assert ModelToComponentFactory._flatten_partition_router_models(None) == []


def test_flatten_partition_router_models_flattens_lists():
    first_router = object()
    second_router = object()

    assert ModelToComponentFactory._flatten_partition_router_models(
        [first_router, [second_router]]
    ) == [first_router, second_router]


def test_flatten_partition_router_models_includes_union_and_children():
    child_router = ListPartitionRouter(
        type="ListPartitionRouter", cursor_field="account", values=["one"]
    )
    union_router = UnionPartitionRouter(
        type="UnionPartitionRouter",
        partition_field="account",
        partition_routers=[child_router],
    )

    assert ModelToComponentFactory._flatten_partition_router_models(union_router) == [
        union_router,
        child_router,
    ]


def test_flatten_partition_router_models_includes_grouping_and_underlying_router():
    child_router = ListPartitionRouter(
        type="ListPartitionRouter", cursor_field="account", values=["one"]
    )
    grouping_router = GroupingPartitionRouter(
        type="GroupingPartitionRouter",
        group_size=2,
        underlying_partition_router=child_router,
    )

    assert ModelToComponentFactory._flatten_partition_router_models(grouping_router) == [
        grouping_router,
        child_router,
    ]
