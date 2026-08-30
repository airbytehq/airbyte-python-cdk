#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import io
import json
from typing import Dict, List, Union

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders import CompositeRawDecoder, Decoder
from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import JsonLineParser
from airbyte_cdk.sources.declarative.decoders.json_decoder import (
    IterableDecoder,
    JsonDecoder,
)
from airbyte_cdk.sources.declarative.expanders.record_expander import OnNoRecords, RecordExpander
from airbyte_cdk.sources.declarative.extractors.dpath_extractor import DpathExtractor

config = {"field": "record_array"}
parameters = {"parameters_field": "record_array"}

decoder_json = JsonDecoder(parameters={})
decoder_jsonl = CompositeRawDecoder(parser=JsonLineParser(), stream_response=True)
decoder_iterable = IterableDecoder(parameters={})


def create_response(body: Union[Dict, bytes]):
    response = requests.Response()
    response.raw = io.BytesIO(body if isinstance(body, bytes) else json.dumps(body).encode("utf-8"))
    return response


@pytest.mark.parametrize(
    "field_path, decoder, body, expected_records",
    [
        (["data"], decoder_json, {"data": [{"id": 1}, {"id": 2}]}, [{"id": 1}, {"id": 2}]),
        (["data"], decoder_json, {"data": {"id": 1}}, [{"id": 1}]),
        ([], decoder_json, {"id": 1}, [{"id": 1}]),
        ([], decoder_json, [{"id": 1}, {"id": 2}], [{"id": 1}, {"id": 2}]),
        (
            ["data", "records"],
            decoder_json,
            {"data": {"records": [{"id": 1}, {"id": 2}]}},
            [{"id": 1}, {"id": 2}],
        ),
        (
            ["{{ config['field'] }}"],
            decoder_json,
            {"record_array": [{"id": 1}, {"id": 2}]},
            [{"id": 1}, {"id": 2}],
        ),
        (
            ["{{ parameters['parameters_field'] }}"],
            decoder_json,
            {"record_array": [{"id": 1}, {"id": 2}]},
            [{"id": 1}, {"id": 2}],
        ),
        (["record"], decoder_json, {"id": 1}, []),
        (["list", "*", "item"], decoder_json, {"list": [{"item": {"id": "1"}}]}, [{"id": "1"}]),
        (
            ["data", "*", "list", "data2", "*"],
            decoder_json,
            {
                "data": [
                    {"list": {"data2": [{"id": 1}, {"id": 2}]}},
                    {"list": {"data2": [{"id": 3}, {"id": 4}]}},
                ]
            },
            [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
        ),
        ([], decoder_jsonl, {"id": 1}, [{"id": 1}]),
        ([], decoder_jsonl, [{"id": 1}, {"id": 2}], [{"id": 1}, {"id": 2}]),
        (["data"], decoder_jsonl, b'{"data": [{"id": 1}, {"id": 2}]}', [{"id": 1}, {"id": 2}]),
        (
            ["data"],
            decoder_jsonl,
            b'{"data": [{"id": 1}, {"id": 2}]}\n{"data": [{"id": 3}, {"id": 4}]}',
            [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
        ),
        (
            ["data"],
            decoder_jsonl,
            b'{"data": [{"id": 1, "text_field": "This is a text\\n. New paragraph start here."}]}\n{"data": [{"id": 2, "text_field": "This is another text\\n. New paragraph start here."}]}',
            [
                {"id": 1, "text_field": "This is a text\n. New paragraph start here."},
                {"id": 2, "text_field": "This is another text\n. New paragraph start here."},
            ],
        ),
        (
            [],
            decoder_iterable,
            b"user1@example.com\nuser2@example.com",
            [{"record": "user1@example.com"}, {"record": "user2@example.com"}],
        ),
    ],
    ids=[
        "test_extract_from_array",
        "test_extract_single_record",
        "test_extract_single_record_from_root",
        "test_extract_from_root_array",
        "test_nested_field",
        "test_field_in_config",
        "test_field_in_parameters",
        "test_field_does_not_exist",
        "test_nested_list",
        "test_complex_nested_list",
        "test_extract_single_record_from_root_jsonl",
        "test_extract_from_root_jsonl",
        "test_extract_from_array_jsonl",
        "test_extract_from_array_multiline_jsonl",
        "test_extract_from_array_multiline_with_escape_character_jsonl",
        "test_extract_from_string_per_line_iterable",
    ],
)
def test_dpath_extractor(field_path: List, decoder: Decoder, body, expected_records: List):
    extractor = DpathExtractor(
        field_path=field_path, config=config, decoder=decoder, parameters=parameters
    )

    response = create_response(body)
    actual_records = list(extractor.extract_records(response))

    assert actual_records == expected_records


def test_dpath_extractor_interpolated_expand_path():
    cfg = {"nested": "items"}
    record_expander = RecordExpander(
        expand_records_from_field=["{{ config['nested'] }}", "data"],
        config=cfg,
        parameters=parameters,
        remain_original_record=True,
    )
    extractor = DpathExtractor(
        field_path=["data", "object"],
        config=cfg,
        decoder=decoder_json,
        parameters=parameters,
        record_expander=record_expander,
    )

    body = {"data": {"object": {"id": "parent", "items": {"data": [{"id": "child"}]}}}}
    response = create_response(body)
    actual_records = list(extractor.extract_records(response))
    assert actual_records == [
        {"id": "child", "original_record": {"id": "parent", "items": {"data": [{"id": "child"}]}}},
    ]


def test_dpath_extractor_expands_non_mapping_safely():
    record_expander = RecordExpander(
        expand_records_from_field=["items"],
        config=config,
        parameters=parameters,
    )
    extractor = DpathExtractor(
        field_path=["value"],
        config=config,
        decoder=decoder_json,
        parameters=parameters,
        record_expander=record_expander,
    )

    response = create_response({"value": 3})
    actual_records = list(extractor.extract_records(response))
    assert actual_records == [3]


def test_dpath_extractor_non_dict_items_with_parent_context():
    parent = {"items": [1, "a"], "meta": "m"}
    record_expander = RecordExpander(
        expand_records_from_field=["items"],
        config=config,
        parameters=parameters,
        remain_original_record=True,
    )
    extractor = DpathExtractor(
        field_path=["data"],
        config=config,
        decoder=decoder_json,
        parameters=parameters,
        record_expander=record_expander,
    )

    response = create_response({"data": parent})
    actual_records = list(extractor.extract_records(response))
    assert actual_records == [
        {"value": 1, "original_record": parent},
        {"value": "a", "original_record": parent},
    ]


@pytest.mark.parametrize(
    "field_path, expand_records_from_field, remain_original_record, body, expected_records",
    [
        (
            ["data", "object"],
            ["lines", "data"],
            False,
            {
                "data": {
                    "object": {
                        "id": "in_123",
                        "created": 1234567890,
                        "lines": {
                            "data": [
                                {"id": "il_1", "amount": 100},
                                {"id": "il_2", "amount": 200},
                            ]
                        },
                    }
                }
            },
            [
                {"id": "il_1", "amount": 100},
                {"id": "il_2", "amount": 200},
            ],
        ),
        (
            ["data", "object"],
            ["lines", "data"],
            True,
            {
                "data": {
                    "object": {
                        "id": "in_123",
                        "created": 1234567890,
                        "lines": {
                            "data": [
                                {"id": "il_1", "amount": 100},
                            ]
                        },
                    }
                }
            },
            [
                {
                    "id": "il_1",
                    "amount": 100,
                    "original_record": {
                        "id": "in_123",
                        "created": 1234567890,
                        "lines": {"data": [{"id": "il_1", "amount": 100}]},
                    },
                },
            ],
        ),
        (
            ["data"],
            ["items"],
            False,
            {"data": {"id": "parent_1", "items": []}},
            [],
        ),
        (
            ["data"],
            ["items"],
            False,
            {"data": {"id": "parent_1"}},
            [],
        ),
        (
            ["data"],
            ["items"],
            False,
            {"data": {"id": "parent_1", "items": "not_an_array"}},
            [],
        ),
        (
            ["data"],
            ["nested", "array"],
            False,
            {
                "data": {
                    "id": "parent_1",
                    "nested": {"array": [{"id": "child_1"}, {"id": "child_2"}]},
                }
            },
            [{"id": "child_1"}, {"id": "child_2"}],
        ),
        (
            ["data"],
            ["items"],
            False,
            {"data": {"id": "parent_1", "items": [1, 2, "string", {"id": "dict_item"}]}},
            [1, 2, "string", {"id": "dict_item"}],
        ),
        (
            [],
            ["items"],
            False,
            [
                {"id": "parent_1", "items": [{"id": "child_1"}]},
                {"id": "parent_2", "items": [{"id": "child_2"}, {"id": "child_3"}]},
            ],
            [{"id": "child_1"}, {"id": "child_2"}, {"id": "child_3"}],
        ),
        (
            ["data"],
            ["sections", "*", "items"],
            False,
            {
                "data": {
                    "sections": [
                        {"name": "section1", "items": [{"id": "item_1"}, {"id": "item_2"}]},
                        {"name": "section2", "items": [{"id": "item_3"}]},
                    ]
                }
            },
            [{"id": "item_1"}, {"id": "item_2"}, {"id": "item_3"}],
        ),
        (
            ["data"],
            ["sections", "*", "items"],
            True,
            {
                "data": {
                    "sections": [
                        {"name": "section1", "items": [{"id": "item_1"}]},
                    ]
                }
            },
            [
                {
                    "id": "item_1",
                    "original_record": {
                        "sections": [
                            {"name": "section1", "items": [{"id": "item_1"}]},
                        ]
                    },
                }
            ],
        ),
        (
            ["data"],
            ["sections", "*", "items"],
            False,
            {
                "data": {
                    "sections": [
                        {"name": "section1", "items": []},
                        {"name": "section2", "items": []},
                    ]
                }
            },
            [],
        ),
        (
            ["data"],
            ["sections", "*", "items"],
            False,
            {
                "data": {
                    "sections": [
                        {"name": "section1"},
                        {"name": "section2", "items": "not_an_array"},
                    ]
                }
            },
            [],
        ),
        (
            ["data"],
            ["*", "items"],
            False,
            {
                "data": {
                    "group1": {"items": [{"id": "item_1"}]},
                    "group2": {"items": [{"id": "item_2"}, {"id": "item_3"}]},
                }
            },
            [{"id": "item_1"}, {"id": "item_2"}, {"id": "item_3"}],
        ),
    ],
    ids=[
        "test_expand_nested_array",
        "test_expand_with_original_record",
        "test_expand_empty_array_yields_nothing",
        "test_expand_missing_path_yields_nothing",
        "test_expand_non_array_yields_nothing",
        "test_expand_deeply_nested_path",
        "test_expand_mixed_types_in_array",
        "test_expand_multiple_parent_records",
        "test_expand_wildcard_multiple_lists",
        "test_expand_wildcard_with_original_record",
        "test_expand_wildcard_all_empty_arrays",
        "test_expand_wildcard_no_list_matches",
        "test_expand_wildcard_dict_values",
    ],
)
def test_dpath_extractor_with_expansion(
    field_path: List,
    expand_records_from_field: List,
    remain_original_record: bool,
    body,
    expected_records: List,
):
    record_expander = RecordExpander(
        expand_records_from_field=expand_records_from_field,
        config=config,
        parameters=parameters,
        remain_original_record=remain_original_record,
    )
    extractor = DpathExtractor(
        field_path=field_path,
        config=config,
        decoder=decoder_json,
        parameters=parameters,
        record_expander=record_expander,
    )

    response = create_response(body)
    actual_records = list(extractor.extract_records(response))

    assert actual_records == expected_records


def test_record_expander_shares_parent_reference_for_dict_items():
    """Expanded dict records share the same parent reference instead of deep-copying.

    This prevents O(N²) memory usage when a parent record contains a large nested
    array (e.g., a Stripe invoice event with many line items). Each expanded child
    record's `original_record` should be the exact same object as all siblings.
    """
    parent = {
        "id": "evt_123",
        "type": "invoice.updated",
        "data": {
            "object": {
                "id": "in_456",
                "lines": {
                    "data": [
                        {"id": "il_1", "amount": 100},
                        {"id": "il_2", "amount": 200},
                        {"id": "il_3", "amount": 300},
                    ]
                },
            }
        },
    }

    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config={},
        parameters={},
        remain_original_record=True,
    )

    records = list(expander.expand_record(parent))
    assert len(records) == 3

    # All expanded records should share the same parent reference (identity, not just equality)
    for rec in records:
        assert rec["original_record"] is parent

    # Values are still correct
    assert records[0]["id"] == "il_1"
    assert records[1]["id"] == "il_2"
    assert records[2]["id"] == "il_3"


def test_record_expander_shares_parent_reference_for_non_dict_items():
    """Non-dict expanded records also share the same parent reference."""
    parent = {"items": [1, "a", 3.14], "meta": "info"}

    expander = RecordExpander(
        expand_records_from_field=["items"],
        config={},
        parameters={},
        remain_original_record=True,
    )

    records = list(expander.expand_record(parent))
    assert len(records) == 3

    for rec in records:
        assert rec["original_record"] is parent


def test_record_expander_large_expansion_memory_efficient():
    """Expanding a record with many children should not multiply parent memory.

    Simulates a Stripe-like scenario: an event with 500 line items. With deep
    copy this would create 500 copies of the full event (O(N²) memory). With
    shared references, memory usage stays O(N).
    """
    n_items = 500
    parent = {
        "id": "evt_large",
        "type": "invoice.updated",
        "data": {
            "object": {
                "id": "in_big",
                "lines": {"data": [{"id": f"il_{i}", "amount": i * 100} for i in range(n_items)]},
            }
        },
    }

    expander = RecordExpander(
        expand_records_from_field=["data", "object", "lines", "data"],
        config={},
        parameters={},
        remain_original_record=True,
    )

    records = list(expander.expand_record(parent))
    assert len(records) == n_items

    # All share the same parent object — no deep copies
    for rec in records:
        assert rec["original_record"] is parent
    assert records[0]["id"] == "il_0"
    assert records[-1]["id"] == f"il_{n_items - 1}"


@pytest.mark.parametrize(
    "field_path, expand_records_from_field, on_no_records, body, expected_records",
    [
        pytest.param(
            ["data"],
            ["items"],
            OnNoRecords.skip,
            {"data": {"id": "parent_1"}},
            [],
            id="on_no_records_skip_missing_path",
        ),
        pytest.param(
            ["data"],
            ["items"],
            OnNoRecords.skip,
            {"data": {"id": "parent_1", "items": []}},
            [],
            id="on_no_records_skip_empty_array",
        ),
        pytest.param(
            ["data"],
            ["items"],
            OnNoRecords.emit_parent,
            {"data": {"id": "parent_1"}},
            [{"id": "parent_1"}],
            id="on_no_records_emit_parent_missing_path",
        ),
        pytest.param(
            ["data"],
            ["items"],
            OnNoRecords.emit_parent,
            {"data": {"id": "parent_1", "items": []}},
            [{"id": "parent_1", "items": []}],
            id="on_no_records_emit_parent_empty_array",
        ),
        pytest.param(
            ["data"],
            ["items"],
            OnNoRecords.emit_parent,
            {"data": {"id": "parent_1", "items": "not_an_array"}},
            [{"id": "parent_1", "items": "not_an_array"}],
            id="on_no_records_emit_parent_non_array",
        ),
        pytest.param(
            ["data"],
            ["items"],
            OnNoRecords.emit_parent,
            {"data": {"id": "parent_1", "items": [{"id": "child_1"}]}},
            [{"id": "child_1"}],
            id="on_no_records_emit_parent_has_items_extracts_normally",
        ),
    ],
)
def test_dpath_extractor_on_no_records(
    field_path: List,
    expand_records_from_field: List,
    on_no_records: OnNoRecords,
    body,
    expected_records: List,
):
    record_expander = RecordExpander(
        expand_records_from_field=expand_records_from_field,
        config=config,
        parameters=parameters,
        on_no_records=on_no_records,
    )
    extractor = DpathExtractor(
        field_path=field_path,
        config=config,
        decoder=decoder_json,
        parameters=parameters,
        record_expander=record_expander,
    )

    response = create_response(body)
    actual_records = list(extractor.extract_records(response))

    assert actual_records == expected_records
