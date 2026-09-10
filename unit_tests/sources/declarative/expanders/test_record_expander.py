#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import threading
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.models import AirbyteLogMessage, AirbyteMessage, AirbyteRecordMessage, Level, Type
from airbyte_cdk.sources.declarative.expanders.record_expander import RecordExpander
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


def test_warnings_are_emitted_even_when_consumer_stops_after_first_child(caplog):
    embedded = [{"id": f"il_{i}"} for i in range(10)]
    no_retriever = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config=config,
        parameters=parameters,
        truncation_indicator_path=["data", "object", "lines", "has_more"],
    )
    short_fetch = _retriever_expander(_make_retriever([{"id": f"il_{i}"} for i in range(12)]))

    with caplog.at_level("WARNING", logger="airbyte"):
        first_embedded = next(iter(no_retriever.expand_record(_event(embedded, True, 15))))
        first_fetched = next(iter(short_fetch.expand_record(_event(embedded, True, 15))))

    assert first_embedded == first_fetched == {"id": "il_0"}
    messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(messages) == 2
    assert "no `truncated_list_retriever` is configured" in messages[0]
    assert "10 embedded item(s) of 15 total" in messages[0]
    assert "returned 12 record(s)" in messages[1]


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
