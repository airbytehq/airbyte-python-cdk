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
            [{"id": "parent_1"}],
        ),
        (
            ["data"],
            ["items"],
            False,
            {"data": {"id": "parent_1", "items": "not_an_array"}},
            [{"id": "parent_1", "items": "not_an_array"}],
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
    ],
    ids=[
        "test_expand_nested_array",
        "test_expand_with_original_record",
        "test_expand_empty_array_yields_nothing",
        "test_expand_missing_path_yields_original",
        "test_expand_non_array_yields_original",
        "test_expand_deeply_nested_path",
        "test_expand_mixed_types_in_array",
        "test_expand_multiple_parent_records",
    ],
)
def test_dpath_extractor_with_expansion(
    field_path: List,
    expand_records_from_field: List,
    remain_original_record: bool,
    body,
    expected_records: List,
):
    extractor = DpathExtractor(
        field_path=field_path,
        config=config,
        decoder=decoder_json,
        parameters=parameters,
        expand_records_from_field=expand_records_from_field,
        remain_original_record=remain_original_record,
    )

    response = create_response(body)
    actual_records = list(extractor.extract_records(response))

    assert actual_records == expected_records
