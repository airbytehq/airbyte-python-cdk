#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.expanders.record_expander import RecordExpander
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
