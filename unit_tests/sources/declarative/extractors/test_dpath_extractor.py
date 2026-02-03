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
from airbyte_cdk.sources.declarative.expanders.record_expander import (
    ParentFieldMapping,
    RecordExpander,
)
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


@pytest.mark.parametrize(
    "field_path, expand_records_from_field, on_no_records, body, expected_records",
    [
        pytest.param(
            ["data"],
            ["items"],
            "skip",
            {"data": {"id": "parent_1"}},
            [],
            id="on_no_records_skip_missing_path",
        ),
        pytest.param(
            ["data"],
            ["items"],
            "skip",
            {"data": {"id": "parent_1", "items": []}},
            [],
            id="on_no_records_skip_empty_array",
        ),
        pytest.param(
            ["data"],
            ["items"],
            "emit_parent",
            {"data": {"id": "parent_1"}},
            [{"id": "parent_1"}],
            id="on_no_records_emit_parent_missing_path",
        ),
        pytest.param(
            ["data"],
            ["items"],
            "emit_parent",
            {"data": {"id": "parent_1", "items": []}},
            [{"id": "parent_1", "items": []}],
            id="on_no_records_emit_parent_empty_array",
        ),
        pytest.param(
            ["data"],
            ["items"],
            "emit_parent",
            {"data": {"id": "parent_1", "items": "not_an_array"}},
            [{"id": "parent_1", "items": "not_an_array"}],
            id="on_no_records_emit_parent_non_array",
        ),
        pytest.param(
            ["data"],
            ["items"],
            "emit_parent",
            {"data": {"id": "parent_1", "items": [{"id": "child_1"}]}},
            [{"id": "child_1"}],
            id="on_no_records_emit_parent_has_items_extracts_normally",
        ),
    ],
)
def test_dpath_extractor_on_no_records(
    field_path: List,
    expand_records_from_field: List,
    on_no_records: str,
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


@pytest.mark.parametrize(
    "field_path, expand_records_from_field, parent_fields_to_copy, body, expected_records",
    [
        pytest.param(
            ["data"],
            ["items", "data"],
            [
                ParentFieldMapping(
                    source_field_path=["id"],
                    target_field="parent_id",
                    config=config,
                    parameters=parameters,
                ),
            ],
            {
                "data": {
                    "id": "sub_123",
                    "created": 1234567890,
                    "items": {"data": [{"id": "si_1"}, {"id": "si_2"}]},
                }
            },
            [
                {"id": "si_1", "parent_id": "sub_123"},
                {"id": "si_2", "parent_id": "sub_123"},
            ],
            id="copy_single_parent_field",
        ),
        pytest.param(
            ["data"],
            ["items", "data"],
            [
                ParentFieldMapping(
                    source_field_path=["id"],
                    target_field="subscription_id",
                    config=config,
                    parameters=parameters,
                ),
                ParentFieldMapping(
                    source_field_path=["created"],
                    target_field="subscription_updated",
                    config=config,
                    parameters=parameters,
                ),
            ],
            {
                "data": {
                    "id": "sub_123",
                    "created": 1234567890,
                    "items": {"data": [{"id": "si_1"}]},
                }
            },
            [{"id": "si_1", "subscription_id": "sub_123", "subscription_updated": 1234567890}],
            id="copy_multiple_parent_fields",
        ),
        pytest.param(
            ["data"],
            ["items", "data"],
            [
                ParentFieldMapping(
                    source_field_path=["metadata", "timestamp"],
                    target_field="parent_timestamp",
                    config=config,
                    parameters=parameters,
                ),
            ],
            {
                "data": {
                    "id": "parent_1",
                    "metadata": {"timestamp": 9999},
                    "items": {"data": [{"id": "child_1"}]},
                }
            },
            [{"id": "child_1", "parent_timestamp": 9999}],
            id="copy_nested_parent_field",
        ),
        pytest.param(
            ["data"],
            ["items", "data"],
            [
                ParentFieldMapping(
                    source_field_path=["missing_field"],
                    target_field="should_not_exist",
                    config=config,
                    parameters=parameters,
                ),
            ],
            {"data": {"id": "parent_1", "items": {"data": [{"id": "child_1"}]}}},
            [{"id": "child_1"}],
            id="copy_missing_parent_field_ignored",
        ),
    ],
)
def test_dpath_extractor_parent_fields_to_copy(
    field_path: List,
    expand_records_from_field: List,
    parent_fields_to_copy: List[ParentFieldMapping],
    body,
    expected_records: List,
):
    record_expander = RecordExpander(
        expand_records_from_field=expand_records_from_field,
        config=config,
        parameters=parameters,
        parent_fields_to_copy=parent_fields_to_copy,
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


@pytest.mark.parametrize(
    "field_path, expand_records_from_field, remain_original_record, parent_fields_to_copy, body, expected_records",
    [
        pytest.param(
            ["data"],
            ["items", "data"],
            True,
            [
                ParentFieldMapping(
                    source_field_path=["id"],
                    target_field="parent_id",
                    config=config,
                    parameters=parameters,
                ),
            ],
            {"data": {"id": "parent_1", "items": {"data": [{"id": "child_1"}]}}},
            [
                {
                    "id": "child_1",
                    "parent_id": "parent_1",
                    "original_record": {
                        "id": "parent_1",
                        "items": {"data": [{"id": "child_1"}]},
                    },
                }
            ],
            id="combine_remain_original_record_and_parent_fields_to_copy",
        ),
    ],
)
def test_dpath_extractor_combined_features(
    field_path: List,
    expand_records_from_field: List,
    remain_original_record: bool,
    parent_fields_to_copy: List[ParentFieldMapping],
    body,
    expected_records: List,
):
    record_expander = RecordExpander(
        expand_records_from_field=expand_records_from_field,
        config=config,
        parameters=parameters,
        remain_original_record=remain_original_record,
        parent_fields_to_copy=parent_fields_to_copy,
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
