#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import threading
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.models import AirbyteLogMessage, AirbyteMessage, AirbyteRecordMessage, Level, Type
from airbyte_cdk.sources.declarative.expanders.record_expander import (
    ParentFieldPath,
    RecordExpander,
)
from airbyte_cdk.sources.message import InMemoryMessageRepository
from airbyte_cdk.sources.types import Record, StreamSlice

config = {}
parameters = {}


def _make_retriever(records):
    retriever = MagicMock()
    retriever.read_records.return_value = iter(records)
    return retriever


def _event(lines, has_more, total_count):
    return {
        "id": "evt_1",
        "data": {
            "object": {
                "id": "in_1",
                "lines": {
                    "data": lines,
                    "has_more": has_more,
                    "total_count": total_count,
                    "url": "/v1/invoices/in_1/lines",
                },
            }
        },
    }


def test_truncated_list_is_fetched_via_retriever():
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    complete = [{"id": f"il_{i}"} for i in range(15)]
    retriever = _make_retriever([Record(data=item, stream_name="test") for item in complete])
    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        remain_original_record=True,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
        truncated_list_retriever=retriever,
    )

    parent = _event(embedded, has_more=True, total_count=15)
    records = list(expander.expand_record(parent))

    assert len(records) == 15
    assert [record["id"] for record in records] == [f"il_{i}" for i in range(15)]
    assert all(record["original_record"] == parent for record in records)
    stream_slice = retriever.read_records.call_args.kwargs["stream_slice"]
    assert stream_slice == StreamSlice(partition={"parent_record": parent}, cursor_slice={})


def test_no_retriever_call_when_not_truncated():
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    retriever = _make_retriever([])
    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
        truncated_list_retriever=retriever,
    )

    records = list(expander.expand_record(_event(embedded, has_more=False, total_count=10)))

    assert len(records) == 10
    retriever.read_records.assert_not_called()


def test_no_retriever_call_when_indicator_missing():
    embedded = [{"id": "il_0"}]
    retriever = _make_retriever([])
    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
        truncated_list_retriever=retriever,
    )

    parent = _event(embedded, has_more=False, total_count=1)
    del parent["data"]["object"]["lines"]["has_more"]
    records = list(expander.expand_record(parent))

    assert len(records) == 1
    retriever.read_records.assert_not_called()


def test_falls_back_to_embedded_items_when_retriever_returns_nothing():
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    retriever = _make_retriever([])
    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
        truncated_list_retriever=retriever,
    )

    records = list(expander.expand_record(_event(embedded, has_more=True, total_count=15)))

    assert len(records) == 10
    retriever.read_records.assert_called_once()


def test_retriever_requires_truncation_indicator_path():
    with pytest.raises(ValueError):
        RecordExpander(
            expand_records_from_field=["lines", "data"],
            config=config,
            parameters=parameters,
            truncated_list_retriever=_make_retriever([]),
        )


def test_wildcard_rejected_with_truncation_handling():
    with pytest.raises(ValueError):
        RecordExpander(
            expand_records_from_field=["sections", "*", "items"],
            config=config,
            parameters=parameters,
            truncation_indicator_path=["lines", "has_more"],
            truncated_list_retriever=_make_retriever([]),
        )


@pytest.mark.parametrize("segment", ["*", "has_*", "has_more?", "[hl]ines", "**"])
def test_glob_metacharacters_rejected_in_truncation_indicator_path(segment):
    with pytest.raises(ValueError):
        RecordExpander(
            expand_records_from_field=["lines", "data"],
            config=config,
            parameters=parameters,
            truncation_indicator_path=[segment, "has_more"],
        )


@pytest.mark.parametrize("segment", ["sect?ons", "[s]ections", "sec*"])
def test_glob_metacharacters_rejected_in_expand_path_with_retriever(segment):
    with pytest.raises(ValueError):
        RecordExpander(
            expand_records_from_field=[segment, "items"],
            config=config,
            parameters=parameters,
            truncation_indicator_path=["lines", "has_more"],
            truncated_list_retriever=_make_retriever([]),
        )


def test_glob_metacharacters_allowed_in_expand_path_without_retriever():
    expander = RecordExpander(
        expand_records_from_field=["sections", "*", "items"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["has_more"],
    )
    records = list(
        expander.expand_record({"sections": [{"items": [{"id": 1}]}], "has_more": False})
    )
    assert records == [{"id": 1}]


def test_interpolated_glob_rejected():
    with pytest.raises(ValueError):
        RecordExpander(
            expand_records_from_field=["lines", "data"],
            config={"indicator": "has_*"},
            parameters=parameters,
            truncation_indicator_path=["lines", "{{ config['indicator'] }}"],
        )


def test_interpolated_indicator_path_is_used():
    expander = RecordExpander(
        expand_records_from_field=["lines", "data"],
        config={"indicator": "has_more"},
        parameters=parameters,
        truncation_indicator_path=["lines", "{{ config['indicator'] }}"],
        truncated_list_retriever=_make_retriever([{"id": 2}]),
    )
    records = list(expander.expand_record({"lines": {"data": [{"id": 1}], "has_more": True}}))
    assert records == [{"id": 2}]


def test_indicator_path_through_non_mapping_is_not_truncated():
    expander = RecordExpander(
        expand_records_from_field=["lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["lines", "has_more", "nested"],
    )
    records = list(expander.expand_record({"lines": {"data": [{"id": 1}], "has_more": True}}))
    assert records == [{"id": 1}]


def test_fetched_scalar_items_are_yielded_like_embedded_items():
    retriever = _make_retriever(["a", "b"])
    expander = RecordExpander(
        expand_records_from_field=["lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["lines", "has_more"],
        truncated_list_retriever=retriever,
    )
    records = list(expander.expand_record({"lines": {"data": ["a"], "has_more": True}}))
    assert records == ["a", "b"]


def test_fetched_scalar_items_are_wrapped_when_remain_original_record():
    retriever = _make_retriever(["a", "b"])
    parent = {"lines": {"data": ["a"], "has_more": True}}
    expander = RecordExpander(
        expand_records_from_field=["lines", "data"],
        config=config,
        parameters=parameters,
        remain_original_record=True,
        truncation_indicator_path=["lines", "has_more"],
        truncated_list_retriever=retriever,
    )
    records = list(expander.expand_record(parent))
    assert records == [
        {"value": "a", "original_record": parent},
        {"value": "b", "original_record": parent},
    ]


def test_retriever_errors_propagate():
    retriever = MagicMock()
    retriever.read_records.side_effect = RuntimeError("boom")
    expander = RecordExpander(
        expand_records_from_field=["lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["lines", "has_more"],
        truncated_list_retriever=retriever,
    )
    with pytest.raises(RuntimeError):
        list(expander.expand_record({"lines": {"data": [{"id": 1}], "has_more": True}}))


def _retriever_expander(retriever, message_repository=None):
    return RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
        truncated_list_retriever=retriever,
        message_repository=message_repository,
    )


def test_protocol_messages_from_retriever_are_not_treated_as_child_records():
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    log_message = AirbyteMessage(
        type=Type.LOG, log=AirbyteLogMessage(level=Level.INFO, message="custom retriever log")
    )
    record_message = AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(stream="lines", data={"id": "il_1"}, emitted_at=0),
    )
    bare_log_message = AirbyteLogMessage(level=Level.DEBUG, message="request/response log")
    retriever = _make_retriever(
        [
            {"id": "il_0"},
            log_message,
            bare_log_message,
            record_message,
            Record(data={"id": "il_2"}, stream_name="t"),
        ]
    )
    expander = _retriever_expander(retriever)

    records = list(expander.expand_record(_event(embedded, has_more=True, total_count=3)))

    assert records == [{"id": "il_0"}, {"id": "il_1"}, {"id": "il_2"}]


def test_warns_once_when_retriever_fetches_fewer_than_total_count(caplog):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    first_page_only = [{"id": f"il_{i}"} for i in range(12)]
    retriever = MagicMock()
    retriever.read_records.side_effect = lambda **_: iter(first_page_only)
    expander = _retriever_expander(retriever)

    with caplog.at_level("WARNING", logger="airbyte"):
        first = list(expander.expand_record(_event(embedded, has_more=True, total_count=15)))
        second = list(expander.expand_record(_event(embedded, has_more=True, total_count=15)))

    assert len(first) == len(second) == 12
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "`truncated_list_retriever`" in message
    assert "returned 12 record(s)" in message
    assert "reports 15" in message
    assert "['data', 'object', 'lines', 'data']" in message
    assert "il_" not in message


@pytest.mark.parametrize(
    "suppress_incomplete_fetch_warning, expected_warning_count",
    [
        pytest.param(True, 0, id="test_read_page_cap_suppresses_warning"),
        pytest.param(False, 1, id="no_page_cap_still_warns"),
    ],
)
def test_incomplete_fetch_warning_under_test_read_page_cap(
    caplog, suppress_incomplete_fetch_warning, expected_warning_count
):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    capped_pages = [{"id": f"il_{i}"} for i in range(200)]
    expander = _retriever_expander(_make_retriever(capped_pages))
    expander.suppress_incomplete_fetch_warning = suppress_incomplete_fetch_warning

    with caplog.at_level("WARNING", logger="airbyte"):
        records = list(expander.expand_record(_event(embedded, has_more=True, total_count=250)))

    assert len(records) == 200
    messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(messages) == expected_warning_count
    for message in messages:
        assert "returned 200 record(s)" in message
        assert "reports 250" in message


def test_test_read_page_cap_flag_does_not_suppress_no_retriever_warning(caplog):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
        suppress_incomplete_fetch_warning=True,
    )

    with caplog.at_level("WARNING", logger="airbyte"):
        records = list(expander.expand_record(_event(embedded, has_more=True, total_count=250)))

    assert len(records) == 10
    messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(messages) == 1
    assert "no `truncated_list_retriever` is configured" in messages[0]


def test_no_retriever_warning_is_emitted_even_when_consumer_stops_after_first_child(caplog):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
    )

    with caplog.at_level("WARNING", logger="airbyte"):
        first = next(iter(expander.expand_record(_event(embedded, True, 15))))

    assert first == {"id": "il_0"}
    messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(messages) == 1
    assert "no `truncated_list_retriever` is configured" in messages[0]
    assert "10 embedded item(s) of 15 total" in messages[0]


def test_fetched_children_are_streamed_not_buffered():
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    consumed = []

    def stream():
        for i in range(3):
            consumed.append(i)
            yield {"id": f"il_{i}"}

    retriever = MagicMock()
    retriever.read_records.return_value = stream()
    expander = _retriever_expander(retriever)

    records = expander.expand_record(_event(embedded, has_more=True, total_count=3))
    assert next(records) == {"id": "il_0"}
    assert consumed == [0]


@pytest.mark.parametrize("total_count", [15, None, True, "15"])
def test_no_incomplete_fetch_warning_when_count_matches_or_total_unknown(caplog, total_count):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    complete = [{"id": f"il_{i}"} for i in range(15)]
    expander = _retriever_expander(_make_retriever(complete))

    with caplog.at_level("WARNING", logger="airbyte"):
        records = list(
            expander.expand_record(_event(embedded, has_more=True, total_count=total_count))
        )

    assert len(records) == 15
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def test_warns_when_retriever_returns_nothing_but_total_count_is_positive(caplog):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    expander = _retriever_expander(_make_retriever([]))

    with caplog.at_level("WARNING", logger="airbyte"):
        records = list(expander.expand_record(_event(embedded, has_more=True, total_count=15)))

    assert [record["id"] for record in records] == [f"il_{i}" for i in range(10)]
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "returned 0 record(s)" in message
    assert "reports 15" in message
    assert "embedded items were expanded as a fallback" in message


def test_no_warning_when_retriever_returns_nothing_and_total_count_unknown(caplog):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    expander = _retriever_expander(_make_retriever([]))

    with caplog.at_level("WARNING", logger="airbyte"):
        records = list(expander.expand_record(_event(embedded, has_more=True, total_count=None)))

    assert len(records) == 10
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def test_warning_is_emitted_once_across_concurrent_expansions():
    repository = InMemoryMessageRepository()
    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
        message_repository=repository,
    )
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    barrier = threading.Barrier(8)

    def expand():
        barrier.wait()
        list(expander.expand_record(_event(embedded, has_more=True, total_count=15)))

    threads = [threading.Thread(target=expand) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(list(repository.consume_queue())) == 1


def test_warnings_go_to_message_repository_instead_of_logger_when_configured(caplog):
    repository = InMemoryMessageRepository()
    expander = _retriever_expander(_make_retriever([]), message_repository=repository)
    embedded = [{"id": f"il_{i}"} for i in range(10)]

    with caplog.at_level("WARNING", logger="airbyte"):
        list(expander.expand_record(_event(embedded, has_more=True, total_count=15)))

    assert len(list(repository.consume_queue())) == 1
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def test_warnings_are_emitted_through_message_repository():
    repository = InMemoryMessageRepository()
    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
        message_repository=repository,
    )
    embedded = [{"id": f"il_{i}"} for i in range(10)]

    list(expander.expand_record(_event(embedded, has_more=True, total_count=15)))

    messages = list(repository.consume_queue())
    assert len(messages) == 1
    assert messages[0].type == Type.LOG
    assert messages[0].log.level == Level.WARN
    assert "10 embedded item(s) of 15 total" in messages[0].log.message


def _indicator_only_expander():
    return RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
    )


def test_warns_when_truncated_and_no_retriever_configured(caplog):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    expander = _indicator_only_expander()

    with caplog.at_level("WARNING", logger="airbyte"):
        records = list(expander.expand_record(_event(embedded, has_more=True, total_count=15)))

    assert len(records) == 10
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "['data', 'object', 'lines', 'data']" in message
    assert "['data', 'object', 'lines', 'has_more']" in message
    assert "10 embedded item(s)" in message
    assert "of 15 total" in message


def test_warning_emitted_once_per_stream_instance(caplog):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    expander = _indicator_only_expander()

    with caplog.at_level("WARNING", logger="airbyte"):
        list(expander.expand_record(_event(embedded, has_more=True, total_count=15)))
        list(expander.expand_record(_event(embedded, has_more=True, total_count=20)))
        list(expander.expand_record(_event(embedded, has_more=True, total_count=25)))

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1


def test_warning_omits_total_when_no_total_count_sibling(caplog):
    embedded = [{"id": "il_0"}]
    expander = _indicator_only_expander()
    parent = _event(embedded, has_more=True, total_count=5)
    del parent["data"]["object"]["lines"]["total_count"]

    with caplog.at_level("WARNING", logger="airbyte"):
        records = list(expander.expand_record(parent))

    assert len(records) == 1
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "total" not in warnings[0].getMessage().split("embedded item(s)")[1].split(" were")[0]


def test_no_warning_when_indicator_falsy(caplog):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    expander = _indicator_only_expander()

    with caplog.at_level("WARNING", logger="airbyte"):
        records = list(expander.expand_record(_event(embedded, has_more=False, total_count=10)))

    assert len(records) == 10
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def test_no_warning_when_retriever_configured(caplog):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    complete = [{"id": f"il_{i}"} for i in range(15)]
    retriever = _make_retriever([Record(data=item, stream_name="test") for item in complete])
    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
        truncated_list_retriever=retriever,
    )

    with caplog.at_level("WARNING", logger="airbyte"):
        records = list(expander.expand_record(_event(embedded, has_more=True, total_count=15)))

    assert len(records) == 15
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def _parent_field(parent_path, record_path):
    return ParentFieldPath(
        parent_path=parent_path,
        record_path=record_path,
        config=config,
        parameters=parameters,
    )


def _reviews_parent():
    return {
        "number": 7,
        "url": "https://github.com/airbytehq/airbyte/pull/7",
        "reviews": {"nodes": [{"id": "PRR_1"}, {"id": "PRR_2"}]},
    }


def test_parent_fields_copies_the_named_value_onto_every_item():
    expander = RecordExpander(
        expand_records_from_field=["reviews", "nodes"],
        parent_fields=[_parent_field(["url"], ["pull_request_url"])],
        config=config,
        parameters=parameters,
    )

    assert list(expander.expand_record(_reviews_parent())) == [
        {"id": "PRR_1", "pull_request_url": "https://github.com/airbytehq/airbyte/pull/7"},
        {"id": "PRR_2", "pull_request_url": "https://github.com/airbytehq/airbyte/pull/7"},
    ]


def test_parent_fields_does_not_embed_the_parent():
    """The reason to use `parent_fields`: no `original_record`, so no deep copy per item."""
    expander = RecordExpander(
        expand_records_from_field=["reviews", "nodes"],
        parent_fields=[_parent_field(["url"], ["pull_request_url"])],
        config=config,
        parameters=parameters,
    )

    for record in expander.expand_record(_reviews_parent()):
        assert "original_record" not in record


def test_parent_fields_and_remain_original_record_are_independent():
    expander = RecordExpander(
        expand_records_from_field=["reviews", "nodes"],
        remain_original_record=True,
        parent_fields=[_parent_field(["number"], ["pull_request_number"])],
        config=config,
        parameters=parameters,
    )

    records = list(expander.expand_record(_reviews_parent()))

    assert [record["pull_request_number"] for record in records] == [7, 7]
    assert all(record["original_record"]["number"] == 7 for record in records)


def test_parent_fields_reads_and_writes_nested_paths():
    expander = RecordExpander(
        expand_records_from_field=["items"],
        parent_fields=[_parent_field(["repository", "name"], ["parent", "repository_name"])],
        config=config,
        parameters=parameters,
    )

    parent = {"repository": {"name": "airbyte"}, "items": [{"id": 1}]}

    assert list(expander.expand_record(parent)) == [
        {"id": 1, "parent": {"repository_name": "airbyte"}}
    ]


def test_parent_fields_overwrites_an_existing_value_on_the_item():
    expander = RecordExpander(
        expand_records_from_field=["items"],
        parent_fields=[_parent_field(["id"], ["id"])],
        config=config,
        parameters=parameters,
    )

    assert list(expander.expand_record({"id": "parent", "items": [{"id": "child"}]})) == [
        {"id": "parent"}
    ]


def test_parent_fields_copies_none_when_the_parent_lacks_the_field():
    """Matches `parent.get(field)` in the connector classes this replaces."""
    expander = RecordExpander(
        expand_records_from_field=["items"],
        parent_fields=[_parent_field(["missing"], ["copied"])],
        config=config,
        parameters=parameters,
    )

    assert list(expander.expand_record({"items": [{"id": 1}]})) == [{"id": 1, "copied": None}]


def test_parent_fields_copies_a_present_null_the_same_way():
    expander = RecordExpander(
        expand_records_from_field=["items"],
        parent_fields=[_parent_field(["maybe"], ["copied"])],
        config=config,
        parameters=parameters,
    )

    assert list(expander.expand_record({"maybe": None, "items": [{"id": 1}]})) == [
        {"id": 1, "copied": None}
    ]


def test_several_parent_fields_are_applied_in_order():
    expander = RecordExpander(
        expand_records_from_field=["items"],
        parent_fields=[
            _parent_field(["a"], ["copied"]),
            _parent_field(["b"], ["copied"]),
        ],
        config=config,
        parameters=parameters,
    )

    assert list(expander.expand_record({"a": 1, "b": 2, "items": [{}]})) == [{"copied": 2}]


def test_parent_fields_does_not_mutate_the_parent_record():
    expander = RecordExpander(
        expand_records_from_field=["reviews", "nodes"],
        parent_fields=[_parent_field(["url"], ["pull_request_url"])],
        config=config,
        parameters=parameters,
    )
    parent = _reviews_parent()

    list(expander.expand_record(parent))

    assert parent == _reviews_parent()


def test_parent_fields_wraps_scalar_items():
    expander = RecordExpander(
        expand_records_from_field=["items"],
        parent_fields=[_parent_field(["id"], ["parent_id"])],
        config=config,
        parameters=parameters,
    )

    assert list(expander.expand_record({"id": 9, "items": ["a", "b"]})) == [
        {"value": "a", "parent_id": 9},
        {"value": "b", "parent_id": 9},
    ]


def test_scalar_items_stay_bare_without_parent_context():
    expander = RecordExpander(
        expand_records_from_field=["items"],
        config=config,
        parameters=parameters,
    )

    assert list(expander.expand_record({"items": ["a", "b"]})) == ["a", "b"]


def test_parent_fields_apply_to_items_fetched_by_the_truncated_list_retriever():
    expander = RecordExpander(
        expand_records_from_field=["reviews", "nodes"],
        truncation_indicator_path=["reviews", "has_more"],
        truncated_list_retriever=_make_retriever([{"id": "PRR_9"}]),
        parent_fields=[_parent_field(["url"], ["pull_request_url"])],
        config=config,
        parameters=parameters,
    )
    parent = {
        "url": "https://github.com/airbytehq/airbyte/pull/7",
        "reviews": {"nodes": [{"id": "PRR_1"}], "has_more": True},
    }

    assert list(expander.expand_record(parent)) == [
        {"id": "PRR_9", "pull_request_url": "https://github.com/airbytehq/airbyte/pull/7"}
    ]


@pytest.mark.parametrize("path", [["*"], ["a", "?"], ["a[0]"]])
def test_globs_are_rejected_in_parent_path(path):
    with pytest.raises(ValueError, match="Glob characters"):
        _parent_field(path, ["copied"])


@pytest.mark.parametrize("path", [["*"], ["a", "?"], ["a[0]"]])
def test_globs_are_rejected_in_record_path(path):
    with pytest.raises(ValueError, match="Glob characters"):
        _parent_field(["id"], path)


def test_glob_rejection_message_does_not_mention_truncation_handling():
    with pytest.raises(ValueError) as error:
        _parent_field(["*"], ["copied"])

    assert "truncation" not in str(error.value)
    assert "`parent_path`" in str(error.value)


@pytest.mark.parametrize("parent_path,record_path", [([], ["a"]), (["a"], [])])
def test_empty_paths_are_rejected(parent_path, record_path):
    with pytest.raises(ValueError, match="cannot be empty"):
        _parent_field(parent_path, record_path)


def test_parent_field_paths_interpolate_config():
    parent_field = ParentFieldPath(
        parent_path=["{{ config['from'] }}"],
        record_path=["{{ config['to'] }}"],
        config={"from": "url", "to": "pull_request_url"},
        parameters=parameters,
    )
    child = {}

    parent_field.copy_onto({"url": "https://example.com/7"}, child)

    assert child == {"pull_request_url": "https://example.com/7"}


def _mailchimp_parent():
    return {
        "email_id": "e1",
        "list_id": "l1",
        "activity": [
            {"action": "open", "timestamp": "t1"},
            {"action": "click", "timestamp": "t2"},
        ],
    }


def _merge_expander(expand_records_from_field, **kwargs):
    return RecordExpander(
        expand_records_from_field=expand_records_from_field,
        merge_parent=True,
        config=config,
        parameters=parameters,
        **kwargs,
    )


def test_merge_parent_flattens_the_parent_into_every_item():
    """The source-mailchimp `email_activity` shape: `{**record, **activity_item}` per item."""
    expander = _merge_expander(["activity"])

    records = list(expander.expand_record(_mailchimp_parent()))

    assert records == [
        {"email_id": "e1", "list_id": "l1", "action": "open", "timestamp": "t1"},
        {"email_id": "e1", "list_id": "l1", "action": "click", "timestamp": "t2"},
    ]
    assert all("activity" not in record for record in records)


def test_merge_parent_lets_the_item_win_on_collision():
    expander = _merge_expander(["items"])

    assert list(expander.expand_record({"id": "parent", "items": [{"id": "child"}]})) == [
        {"id": "child"}
    ]


def test_merge_parent_removes_only_the_expanded_list_on_a_multi_segment_path():
    expander = _merge_expander(["reviews", "nodes"])
    parent = {
        "number": 7,
        "reviews": {"nodes": [{"id": "PRR_1"}], "totalCount": 1},
    }

    assert list(expander.expand_record(parent)) == [
        {"number": 7, "reviews": {"totalCount": 1}, "id": "PRR_1"}
    ]


def test_merge_parent_removes_every_list_matched_by_a_glob_path():
    expander = _merge_expander(["sections", "*", "items"])
    parent = {
        "id": 1,
        "sections": {
            "a": {"items": [{"n": 1}], "title": "A"},
            "b": {"items": [{"n": 2}], "title": "B"},
        },
    }

    assert list(expander.expand_record(parent)) == [
        {"id": 1, "sections": {"a": {"title": "A"}, "b": {"title": "B"}}, "n": 1},
        {"id": 1, "sections": {"a": {"title": "A"}, "b": {"title": "B"}}, "n": 2},
    ]


def test_merge_parent_removes_lists_nested_in_a_list_of_sections():
    expander = _merge_expander(["sections", "*", "items"])
    parent = {"sections": [{"items": [{"n": 1}], "k": "s0"}, {"items": [{"n": 2}], "k": "s1"}]}

    assert list(expander.expand_record(parent)) == [
        {"sections": [{"k": "s0"}, {"k": "s1"}], "n": 1},
        {"sections": [{"k": "s0"}, {"k": "s1"}], "n": 2},
    ]


def test_parent_fields_applied_after_merge_parent_overwrite_a_merged_value():
    expander = _merge_expander(
        ["items"],
        parent_fields=[_parent_field(["id"], ["id"])],
    )

    assert list(expander.expand_record({"id": "parent", "items": [{"id": "child"}]})) == [
        {"id": "parent"}
    ]


def test_merge_parent_and_remain_original_record_are_independent():
    expander = _merge_expander(["activity"], remain_original_record=True)

    records = list(expander.expand_record(_mailchimp_parent()))

    assert records == [
        {
            "email_id": "e1",
            "list_id": "l1",
            "action": "open",
            "timestamp": "t1",
            "original_record": _mailchimp_parent(),
        },
        {
            "email_id": "e1",
            "list_id": "l1",
            "action": "click",
            "timestamp": "t2",
            "original_record": _mailchimp_parent(),
        },
    ]


def test_merge_parent_wraps_scalar_items():
    expander = _merge_expander(["items"])

    assert list(expander.expand_record({"id": 9, "items": ["a", "b"]})) == [
        {"id": 9, "value": "a"},
        {"id": 9, "value": "b"},
    ]


def test_merge_parent_does_not_mutate_the_parent_record():
    expander = _merge_expander(["reviews", "nodes"])
    parent = _reviews_parent()

    records = list(expander.expand_record(parent))

    assert parent == _reviews_parent()
    assert records[0]["reviews"] == {}


def test_merge_parent_gives_each_item_its_own_copy_of_nested_values():
    expander = _merge_expander(["items"])
    metadata = {"k": "v"}
    parent = {"metadata": metadata, "items": [{"id": 1}, {"id": 2}]}

    records = list(expander.expand_record(parent))

    assert records[0]["metadata"] is not metadata
    assert records[0]["metadata"] is not records[1]["metadata"]
    # A downstream transformation writing into one item cannot reach the parent or its siblings.
    records[0]["metadata"]["written"] = True
    assert records[1]["metadata"] == {"k": "v"}
    assert metadata == {"k": "v"}


def test_parent_fields_gives_each_item_its_own_copy_of_a_copied_container():
    expander = RecordExpander(
        expand_records_from_field=["reviews", "nodes"],
        parent_fields=[_parent_field(["repository"], ["repository"])],
        config=config,
        parameters=parameters,
    )
    repository = {"name": "airbyte"}
    parent = {"repository": repository, "reviews": {"nodes": [{"id": 1}, {"id": 2}]}}

    records = list(expander.expand_record(parent))

    assert records[0]["repository"] is not repository
    assert records[0]["repository"] is not records[1]["repository"]
    records[0]["repository"]["review_id"] = 1
    assert records[1]["repository"] == {"name": "airbyte"}
    assert repository == {"name": "airbyte"}


def test_merge_parent_applies_to_items_fetched_by_the_truncated_list_retriever():
    expander = _merge_expander(
        ["reviews", "nodes"],
        truncation_indicator_path=["reviews", "has_more"],
        truncated_list_retriever=_make_retriever([{"id": "PRR_9"}, "scalar"]),
    )
    parent = {
        "url": "https://github.com/airbytehq/airbyte/pull/7",
        "reviews": {"nodes": [{"id": "PRR_1"}], "has_more": True},
    }

    assert list(expander.expand_record(parent)) == [
        {
            "url": "https://github.com/airbytehq/airbyte/pull/7",
            "reviews": {"has_more": True},
            "id": "PRR_9",
        },
        {
            "url": "https://github.com/airbytehq/airbyte/pull/7",
            "reviews": {"has_more": True},
            "value": "scalar",
        },
    ]


def test_merge_parent_unset_leaves_items_unchanged():
    expander = RecordExpander(
        expand_records_from_field=["activity"],
        config=config,
        parameters=parameters,
    )

    assert expander.merge_parent is False
    assert list(expander.expand_record(_mailchimp_parent())) == [
        {"action": "open", "timestamp": "t1"},
        {"action": "click", "timestamp": "t2"},
    ]
