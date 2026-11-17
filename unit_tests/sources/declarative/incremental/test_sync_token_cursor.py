#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import json
import logging
from unittest.mock import MagicMock

import pytest
import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from airbyte_cdk.sources.declarative.incremental.sync_token_cursor import SyncTokenCursor
from airbyte_cdk.sources.declarative.partition_routers.single_partition_router import (
    SinglePartitionRouter,
)
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import PageCompleted
from airbyte_cdk.sources.message.repository import InMemoryMessageRepository
from airbyte_cdk.sources.types import StreamSlice
from airbyte_cdk.utils import AirbyteTracedException


class Router:
    def __init__(self, slices):
        self.slices = slices

    def stream_slices(self):
        yield from self.slices


def _response(body):
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(body).encode()
    response.headers = {}
    return response


def _cursor(*, state=None, router=None, max_partitions=1000, completion_condition=None):
    return SyncTokenCursor(
        stream_name="events",
        stream_namespace=None,
        stream_state=state,
        message_repository=InMemoryMessageRepository(),
        connector_state_manager=ConnectorStateManager(),
        partition_router=router or SinglePartitionRouter(parameters={}),
        token_path=["syncToken"],
        decoder=JsonDecoder(parameters={}),
        config={},
        parameters={},
        completion_condition=completion_condition,
        max_partitions=max_partitions,
    )


def _partition(partition):
    result = MagicMock()
    result.to_slice.return_value = {"partition": partition, "cursor_slice": {}}
    return result


def test_t_token_preserves_string_and_converts_integer_but_rejects_other_types():
    cursor = _cursor()

    for value in ("001", "000123", '"quoted"', "None"):
        assert cursor._coerce_token(value, ["syncToken"]) == value
    for value in (0, 17, 2**63):
        assert cursor._coerce_token(value, ["syncToken"]) == str(value)
    assert cursor._coerce_token(None, ["syncToken"]) is None
    assert cursor._coerce_token("", ["syncToken"]) is None

    for value in (True, 1.5, {}, []):
        with pytest.raises(AirbyteTracedException) as exc:
            cursor._coerce_token(value, ["syncToken"])
        assert exc.value.failure_type == FailureType.system_error
        if isinstance(value, list):
            expected = (
                'Stream events: token_path ["syncToken"] holds a list, '
                "not a string or integer. Check token_path."
            )
            assert exc.value.message == expected
            assert exc.value.internal_message == expected


def test_t_boot_and_t_inc_commit_only_after_partition_close_and_resume_from_state():
    cursor = _cursor()
    stream_slice = StreamSlice(partition={}, cursor_slice={})
    cursor.on_page_completed(
        PageCompleted(stream_slice, _response({"items": [], "syncToken": "token-1"}), True)
    )

    assert cursor.state == {"state_type": "sync_token"}
    cursor.close_partition(_partition({}))
    assert cursor.state["state_type"] == "sync_token"
    assert cursor.state["token"] == "token-1"
    assert cursor.state["committed_at"].endswith("Z")

    resumed = _cursor(state=cursor.state)
    slices = list(resumed.stream_slices())
    assert len(slices) == 1
    assert slices[0].cursor_slice == {"token": "token-1"}


def test_t_noecho_and_t_empty_leave_committed_token_unchanged():
    cursor = _cursor(
        state={
            "state_type": "sync_token",
            "token": "previous",
            "committed_at": "2026-01-01T00:00:00Z",
        }
    )
    stream_slice = StreamSlice(partition={}, cursor_slice={})
    cursor.on_page_completed(
        PageCompleted(stream_slice, _response({"syncToken": "intermediate"}), False)
    )
    cursor.on_page_completed(PageCompleted(stream_slice, _response({"syncToken": ""}), True))
    cursor.close_partition(_partition({}))

    assert cursor.state == {
        "state_type": "sync_token",
        "token": "previous",
        "committed_at": "2026-01-01T00:00:00Z",
    }


def test_t_completion_condition_must_pass_before_a_token_is_committed():
    cursor = _cursor(completion_condition="{{ response.get('complete') }}")
    stream_slice = StreamSlice(partition={}, cursor_slice={})

    cursor.on_page_completed(
        PageCompleted(
            stream_slice,
            _response({"syncToken": "incomplete", "complete": False}),
            True,
        )
    )
    cursor.close_partition(_partition({}))
    assert cursor.state == {"state_type": "sync_token"}

    cursor.on_page_completed(
        PageCompleted(
            stream_slice,
            _response({"syncToken": "complete", "complete": True}),
            True,
        )
    )
    cursor.close_partition(_partition({}))
    assert cursor.state["token"] == "complete"


def test_t_fail_discards_pending_token_when_pass_does_not_complete():
    cursor = _cursor()
    stream_slice = StreamSlice(partition={}, cursor_slice={})
    cursor.on_page_completed(
        PageCompleted(stream_slice, _response({"syncToken": "uncommitted"}), False)
    )
    cursor.close_partition(_partition({}))

    assert cursor.state == {"state_type": "sync_token"}


def test_t_par_keeps_tokens_partition_scoped():
    router = Router(
        [
            StreamSlice(partition={"account": "a"}, cursor_slice={}),
            StreamSlice(partition={"account": "b"}, cursor_slice={}),
            StreamSlice(partition={"account": "c"}, cursor_slice={}),
        ]
    )
    cursor = _cursor(
        router=router,
        state={
            "state_type": "sync_token",
            "states": [
                {
                    "partition": {"account": "c"},
                    "cursor": {"token": "token-c", "committed_at": "2026-01-01T00:00:00Z"},
                }
            ],
        },
    )
    for stream_slice, token in (
        (router.slices[1], "token-b-new"),
        (router.slices[0], "token-a-new"),
    ):
        cursor.on_page_completed(PageCompleted(stream_slice, _response({"syncToken": token}), True))
        cursor.close_partition(_partition(stream_slice.partition))

    assert cursor.state["state_type"] == "sync_token"
    assert {
        (item["partition"]["account"], item["cursor"]["token"]) for item in cursor.state["states"]
    } == {("a", "token-a-new"), ("b", "token-b-new"), ("c", "token-c")}


def test_t_stream_slices_deduplicates_partitions_and_uses_only_the_cursor_token():
    router = Router(
        [
            StreamSlice(
                partition={"account": "a"},
                cursor_slice={"router": "value"},
                extra_fields={"query": "first"},
            ),
            StreamSlice(
                partition={"account": "a"},
                cursor_slice={"router": "other"},
                extra_fields={"query": "duplicate"},
            ),
        ]
    )
    cursor = _cursor(router=router)

    slices = list(cursor.stream_slices())

    assert len(slices) == 1
    assert slices[0].cursor_slice == {}
    assert slices[0].extra_fields == {"query": "first"}


def test_t_cap_retains_tokens_after_failed_router_enumeration_and_drops_vanished_partitions():
    state = {
        "state_type": "sync_token",
        "states": [
            {
                "partition": {"account": "a"},
                "cursor": {"token": "token-a", "committed_at": "2026-01-01T00:00:00Z"},
            },
            {
                "partition": {"account": "b"},
                "cursor": {"token": "token-b", "committed_at": "2026-01-01T00:00:00Z"},
            },
        ],
    }

    class FailingRouter(Router):
        def stream_slices(self):
            yield self.slices[0]
            raise RuntimeError("router failed")

    failing_cursor = _cursor(
        router=FailingRouter([StreamSlice(partition={"account": "a"}, cursor_slice={})]),
        state=state,
    )
    with pytest.raises(RuntimeError, match="router failed"):
        list(failing_cursor.stream_slices())
    assert {item["cursor"]["token"] for item in failing_cursor.state["states"]} == {
        "token-a",
        "token-b",
    }

    current_cursor = _cursor(
        router=Router([StreamSlice(partition={"account": "a"}, cursor_slice={})]),
        state=state,
    )
    list(current_cursor.stream_slices())
    assert {item["partition"]["account"] for item in current_cursor.state["states"]} == {"a"}


def test_t_cap_does_not_evict_existing_partitions():
    router = Router(
        [
            StreamSlice(partition={"account": "a"}, cursor_slice={}),
            StreamSlice(partition={"account": "b"}, cursor_slice={}),
        ]
    )
    cursor = _cursor(router=router, max_partitions=1)
    for stream_slice, token in zip(router.slices, ("token-a", "token-b")):
        cursor.on_page_completed(PageCompleted(stream_slice, _response({"syncToken": token}), True))
        cursor.close_partition(_partition(stream_slice.partition))

    assert len(cursor.state["states"]) == 1
    assert cursor.state["states"][0]["partition"] == {"account": "a"}
    assert cursor.state["states"][0]["cursor"]["token"] == "token-a"


def test_t_foreign_ignores_state_from_another_cursor_type():
    cursor = _cursor(state={"state_type": "datetime", "token": "not-a-token"})

    assert cursor.state == {"state_type": "sync_token"}


@pytest.mark.parametrize(
    "state",
    [
        {"state_type": "datetime", "token": "not-a-token"},
        {"__ab_no_cursor_state_message": True},
        {
            "state_type": "datetime",
            "states": [
                {
                    "partition": {"account": "a"},
                    "cursor": {"created_at": "2024-01-01T00:00:00Z"},
                }
            ],
        },
    ],
)
def test_t_foreign_datetime_and_missing_cursor_states_bootstrap(state):
    router = Router([StreamSlice(partition={"account": "a"}, cursor_slice={})])
    cursor = _cursor(router=router, state=state)

    assert list(cursor.stream_slices())[0].cursor_slice == {}
    assert cursor.state["state_type"] == "sync_token"
    assert cursor.state["states"] == []


def test_partition_state_load_fails_on_malformed_entries():
    router = Router(
        [
            StreamSlice(partition={"account": "a"}, cursor_slice={}),
            StreamSlice(partition={"account": "b"}, cursor_slice={}),
        ]
    )
    cursor = _cursor(
        router=router,
        state={
            "state_type": "sync_token",
            "states": [
                {
                    "partition": {"account": "a"},
                    "cursor": {"token": "A1", "committed_at": "2026-01-01T00:00:00Z"},
                },
                None,
                {
                    "partition": {"account": "b"},
                    "cursor": {"token": ["invalid"]},
                },
            ],
        },
    )

    with pytest.raises(AirbyteTracedException):
        list(cursor.stream_slices())


def test_t_cap_warns_once_at_final_emission_with_over_cap_partition_count(caplog):
    router = Router(
        [
            StreamSlice(partition={"account": account}, cursor_slice={})
            for account in ("a", "b", "c", "d", "e")
        ]
    )
    state = {
        "state_type": "sync_token",
        "states": [
            {
                "partition": {"account": account},
                "cursor": {"token": account.upper(), "committed_at": "2026-01-01T00:00:00Z"},
            }
            for account in ("a", "b", "c")
        ],
    }
    cursor = _cursor(router=router, state=state, max_partitions=3)

    with caplog.at_level(logging.INFO):
        list(cursor.stream_slices())
        assert "partitions already store tokens" not in caplog.text
        cursor.ensure_at_least_one_state_emitted()
        cursor.ensure_at_least_one_state_emitted()

    assert (
        caplog.text.count(
            "Stream events: 3 partitions already store tokens; 2 more read in full on every sync."
        )
        == 1
    )
    assert caplog.text.count("reason: partition cap") == 1


def test_bootstrap_and_replay_observability_are_deduplicated_and_aged(caplog):
    router = Router(
        [StreamSlice(partition={"account": account}, cursor_slice={}) for account in ("a", "b")]
    )
    cursor = _cursor(router=router)
    with caplog.at_level(logging.INFO):
        list(cursor.stream_slices())
    assert caplog.text.count("reason: none") == 1

    caplog.clear()
    replay = _cursor(
        state={
            "state_type": "sync_token",
            "token": "T1",
            "committed_at": "2024-01-01T00:00:00Z",
        }
    )
    with caplog.at_level(logging.INFO):
        list(replay.stream_slices())
    assert "Stream events: replaying the sync token committed " in caplog.text
    assert " ago." in caplog.text


def test_commit_observability_counts_completed_pages_per_partition(caplog):
    cursor = _cursor()
    stream_slice = StreamSlice(partition={}, cursor_slice={})
    with caplog.at_level(logging.DEBUG, logger="airbyte"):
        for exhausted, body in (
            (False, {"nextCursor": "p2"}),
            (False, {"nextCursor": "p3"}),
            (True, {"syncToken": "T1"}),
        ):
            cursor.on_page_completed(PageCompleted(stream_slice, _response(body), exhausted))
        cursor.close_partition(_partition({}))

    assert "Stream events: committed sync token T1 for partition {} after 3 pages." in caplog.text


def test_t_valid_and_t_init_use_partition_router_and_initial_token_slice():
    router = Router([StreamSlice(partition={"account": "a"}, cursor_slice={})])
    cursor = _cursor(router=router)

    slices = list(cursor.stream_slices())
    assert slices[0].partition == {"account": "a"}
    assert slices[0].cursor_slice == {}


@pytest.mark.parametrize(
    "state",
    [
        pytest.param({"state_type": "sync_token", "token": {"bad": "T1"}}, id="object-token"),
        pytest.param({"state_type": "sync_token", "token": 123}, id="integer-token"),
        pytest.param({"state_type": "sync_token", "token": ""}, id="empty-token"),
        pytest.param({"state_type": "sync_token", "token": None}, id="null-token"),
        pytest.param({"state_type": "sync_token", "states": []}, id="partitioned-shape"),
        pytest.param(
            {"state_type": "sync_token", "token": "T1", "after_reset": "true"},
            id="invalid-after-reset",
        ),
    ],
)
def test_malformed_sync_token_state_fails_the_stream_instead_of_reading_everything(state):
    cursor = _cursor(state=state)

    with pytest.raises(AirbyteTracedException):
        list(cursor.stream_slices())


@pytest.mark.parametrize(
    "state",
    [
        pytest.param({"state_type": "sync_token", "token": "T1"}, id="unpartitioned-shape"),
        pytest.param(
            {
                "state_type": "sync_token",
                "states": [{"partition": {"account": "a"}, "cursor": {"token": 7}}],
            },
            id="integer-token-entry",
        ),
        pytest.param(
            {
                "state_type": "sync_token",
                "states": [
                    {
                        "partition": {"account": "a"},
                        "cursor": {"token": "A1", "after_reset": "true"},
                    }
                ],
            },
            id="invalid-after-reset-entry",
        ),
        pytest.param({"state_type": "sync_token", "token": "T1", "states": []}, id="both-shapes"),
        pytest.param({"state_type": "sync_token", "states": None}, id="null-states"),
        pytest.param(
            {"state_type": "sync_token", "states": [{"partition": "a", "cursor": {"token": "A1"}}]},
            id="non-object-partition",
        ),
        pytest.param(
            {
                "state_type": "sync_token",
                "states": [{"partition": {"account": "a"}, "cursor": None}],
            },
            id="null-cursor",
        ),
        pytest.param(
            {
                "state_type": "sync_token",
                "states": [{"partition": {"account": "a"}, "cursor": {"token": ""}}],
            },
            id="empty-token-entry",
        ),
    ],
)
def test_malformed_partitioned_sync_token_state_fails(state):
    router = Router([StreamSlice(partition={"account": "a"}, cursor_slice={})])
    cursor = _cursor(state=state, router=router)

    with pytest.raises(AirbyteTracedException) as error:
        list(cursor.stream_slices())
    assert error.value.failure_type == FailureType.system_error
    assert cursor.state == {"state_type": "sync_token", "states": []}


def test_malformed_sync_token_state_is_not_overwritten_when_the_stream_fails():
    state = {"state_type": "sync_token", "token": 123}
    cursor = _cursor(state=state)
    with pytest.raises(AirbyteTracedException):
        list(cursor.stream_slices())

    # ConcurrentReadProcessor._on_stream_is_done calls this for failed streams too.
    cursor.ensure_at_least_one_state_emitted()

    emitted = [
        message.state.stream.stream_state.__dict__
        for message in cursor._message_repository.consume_queue()
    ]
    assert emitted in ([], [state])


@pytest.mark.parametrize(
    "state",
    [
        pytest.param(None, id="no-state"),
        pytest.param({}, id="empty-state"),
        pytest.param({"state_type": "sync_token"}, id="nothing-committed-yet"),
    ],
)
def test_empty_state_still_bootstraps(state):
    cursor = _cursor(state=state)

    assert [stream_slice.cursor_slice for stream_slice in cursor.stream_slices()] == [{}]


def test_empty_partitioned_state_still_bootstraps():
    router = Router([StreamSlice(partition={"account": "a"}, cursor_slice={})])
    cursor = _cursor(state={"state_type": "sync_token", "states": []}, router=router)

    assert [stream_slice.cursor_slice for stream_slice in cursor.stream_slices()] == [{}]


@pytest.mark.parametrize("partitioned", [False, True])
def test_state_without_token_or_states_bootstraps_for_either_partition_shape(partitioned):
    router = (
        Router([StreamSlice(partition={"account": "a"}, cursor_slice={})]) if partitioned else None
    )
    cursor = _cursor(state={"state_type": "sync_token"}, router=router)

    assert [stream_slice.cursor_slice for stream_slice in cursor.stream_slices()] == [{}]


@pytest.mark.parametrize(
    "state",
    [
        pytest.param({"updated_at": "2026-10-01T00:00:00Z"}, id="datetime-cursor"),
        pytest.param({"state_type": "date-range", "slices": []}, id="concurrent-datetime-cursor"),
        pytest.param({"state_type": "wrong_type", "token": "T1"}, id="unknown-state-type"),
    ],
)
def test_state_from_another_cursor_type_bootstraps_with_a_warning(state, caplog):
    caplog.set_level(logging.INFO)
    cursor = _cursor(state=state)

    assert [stream_slice.cursor_slice for stream_slice in cursor.stream_slices()] == [{}]
    assert any(record.levelno == logging.WARNING for record in caplog.records)
