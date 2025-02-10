#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import io
import json
from typing import Dict, List, Union

import pytest
import requests

from airbyte_cdk import Decoder
from airbyte_cdk.sources.declarative.decoders.json_decoder import (
    IterableDecoder,
    JsonDecoder,
    JsonlDecoder,
)
from airbyte_cdk.sources.declarative.extractors.dpath_enhancing_extractor import (
    SERVICE_KEY_PARENT,
    SERVICE_KEY_ROOT,
    DpathEnhancingExtractor,
)
from airbyte_cdk.sources.declarative.extractors.record_extractor import (
    SERVICE_KEY_PREFIX,
)

ROOT = SERVICE_KEY_PREFIX + SERVICE_KEY_ROOT
PARENT = SERVICE_KEY_PREFIX + SERVICE_KEY_PARENT

config = {"field": "record_array"}
parameters = {"parameters_field": "record_array"}

decoder_json = JsonDecoder(parameters={})
decoder_jsonl = JsonlDecoder(parameters={})
decoder_iterable = IterableDecoder(parameters={})


def create_response(body: Union[Dict, bytes]):
    response = requests.Response()
    response.raw = io.BytesIO(body if isinstance(body, bytes) else json.dumps(body).encode("utf-8"))
    return response


@pytest.mark.parametrize(
    "field_path, decoder, body, expected_records",
    [
        ([], decoder_json, b"", [{}]),  # The JSON contract is irregular, compare with JSONL
        ([], decoder_json, {}, [{}]),
        ([], decoder_json, [], []),
        ([], decoder_json, {"id": 1}, [{"id": 1, ROOT: {"id": 1}}]),
        (
            [],
            decoder_json,
            [{"id": 1}, {"id": 2}],
            [{"id": 1, ROOT: {"id": 1}}, {"id": 2, ROOT: {"id": 2}}],
        ),
        (
            [],
            decoder_json,
            [{"id": 1, "nested": {"id2": 2}}],
            [
                {
                    "id": 1,
                    "nested": {"id2": 2, PARENT: {"id": 1}},
                    ROOT: {"id": 1, "nested": {"id2": 2}},
                }
            ],
        ),
        (
            [],
            decoder_json,
            [{"id": 1, "nested": {"id2": 2, "id3": 3}}],
            [
                {
                    "id": 1,
                    "nested": {"id2": 2, "id3": 3, PARENT: {"id": 1}},
                    ROOT: {"id": 1, "nested": {"id2": 2, "id3": 3}},
                }
            ],
        ),
        (
            [],
            decoder_json,
            [{"id": 1, "nested": {"id2": 2}}, {"id": 3, "nested": {"id4": 4}}],
            [
                {
                    "id": 1,
                    "nested": {"id2": 2, PARENT: {"id": 1}},
                    ROOT: {"id": 1, "nested": {"id2": 2}},
                },
                {
                    "id": 3,
                    "nested": {"id4": 4, PARENT: {"id": 3}},
                    ROOT: {"id": 3, "nested": {"id4": 4}},
                },
            ],
        ),
        (
            [],
            decoder_json,
            [{"id": 1, "nested": {"id2": 2, "id3": 3}}, {"id": 3, "nested": {"id4": 4, "id5": 5}}],
            [
                {
                    "id": 1,
                    "nested": {"id2": 2, "id3": 3, PARENT: {"id": 1}},
                    ROOT: {"id": 1, "nested": {"id2": 2, "id3": 3}},
                },
                {
                    "id": 3,
                    "nested": {"id4": 4, "id5": 5, PARENT: {"id": 3}},
                    ROOT: {"id": 3, "nested": {"id4": 4, "id5": 5}},
                },
            ],
        ),
        (["data"], decoder_json, {"data": {"id": 1}}, [{"id": 1, ROOT: {"data": {"id": 1}}}]),
        (
            ["data"],
            decoder_json,
            {"data": [{"id": 1}, {"id": 2}]},
            [
                {"id": 1, ROOT: {"data": [{"id": 1}, {"id": 2}]}},
                {"id": 2, ROOT: {"data": [{"id": 1}, {"id": 2}]}},
            ],
        ),
        (
            ["data"],
            decoder_json,
            {"data": [{"id": 1}, {"id": 2}], "id3": 3},
            [
                {"id": 1, PARENT: {"id3": 3}, ROOT: {"data": [{"id": 1}, {"id": 2}], "id3": 3}},
                {"id": 2, PARENT: {"id3": 3}, ROOT: {"data": [{"id": 1}, {"id": 2}], "id3": 3}},
            ],
        ),
        (
            ["data"],
            decoder_json,
            {"data": [{"id": 1, "nested": {"id2": 2}}]},
            [
                {
                    "id": 1,
                    "nested": {"id2": 2, PARENT: {"id": 1}},
                    ROOT: {"data": [{"id": 1, "nested": {"id2": 2}}]},
                }
            ],
        ),
        (
            ["data"],
            decoder_json,
            {"data": [{"id": 1, "nested": {"id2": 2}}], "id3": 3},
            [
                {
                    "id": 1,
                    "nested": {"id2": 2, PARENT: {"id": 1, PARENT: {"id3": 3}}},
                    PARENT: {"id3": 3},
                    ROOT: {"data": [{"id": 1, "nested": {"id2": 2}}], "id3": 3},
                }
            ],
        ),
        (
            ["data"],
            decoder_json,
            {"data": [{"id": 1, "nested": {"id2": 2, "id3": 3}}]},
            [
                {
                    "id": 1,
                    "nested": {"id2": 2, "id3": 3, PARENT: {"id": 1}},
                    ROOT: {"data": [{"id": 1, "nested": {"id2": 2, "id3": 3}}]},
                }
            ],
        ),
        (
            ["data"],
            decoder_json,
            {"data": [{"id": 1, "nested": {"id2": 2}}, {"id": 3, "nested": {"id4": 4}}]},
            [
                {
                    "id": 1,
                    "nested": {"id2": 2, PARENT: {"id": 1}},
                    ROOT: {
                        "data": [{"id": 1, "nested": {"id2": 2}}, {"id": 3, "nested": {"id4": 4}}]
                    },
                },
                {
                    "id": 3,
                    "nested": {"id4": 4, PARENT: {"id": 3}},
                    ROOT: {
                        "data": [{"id": 1, "nested": {"id2": 2}}, {"id": 3, "nested": {"id4": 4}}]
                    },
                },
            ],
        ),
        (
            ["data"],
            decoder_json,
            {
                "data": [
                    {"id": 1, "nested": {"id2": 2, "id3": 3}},
                    {"id": 3, "nested": {"id4": 4, "id5": 5}},
                ]
            },
            [
                {
                    "id": 1,
                    "nested": {"id2": 2, "id3": 3, PARENT: {"id": 1}},
                    ROOT: {
                        "data": [
                            {"id": 1, "nested": {"id2": 2, "id3": 3}},
                            {"id": 3, "nested": {"id4": 4, "id5": 5}},
                        ]
                    },
                },
                {
                    "id": 3,
                    "nested": {"id4": 4, "id5": 5, PARENT: {"id": 3}},
                    ROOT: {
                        "data": [
                            {"id": 1, "nested": {"id2": 2, "id3": 3}},
                            {"id": 3, "nested": {"id4": 4, "id5": 5}},
                        ]
                    },
                },
            ],
        ),
        (
            ["data", "records"],
            decoder_json,
            {"data": {"records": [{"id": 1}, {"id": 2}]}},
            [
                {"id": 1, ROOT: {"data": {"records": [{"id": 1}, {"id": 2}]}}},
                {"id": 2, ROOT: {"data": {"records": [{"id": 1}, {"id": 2}]}}},
            ],
        ),
        (
            ["{{ config['field'] }}"],
            decoder_json,
            {"record_array": [{"id": 1}, {"id": 2}]},
            [
                {"id": 1, ROOT: {"record_array": [{"id": 1}, {"id": 2}]}},
                {"id": 2, ROOT: {"record_array": [{"id": 1}, {"id": 2}]}},
            ],
        ),
        (
            ["{{ parameters['parameters_field'] }}"],
            decoder_json,
            {"record_array": [{"id": 1}, {"id": 2}]},
            [
                {"id": 1, ROOT: {"record_array": [{"id": 1}, {"id": 2}]}},
                {"id": 2, ROOT: {"record_array": [{"id": 1}, {"id": 2}]}},
            ],
        ),
        (["record"], decoder_json, {"id": 1}, []),
        (
            ["list", "*", "item"],
            decoder_json,
            {"list": [{"item": {"id": "1"}}]},
            [{"id": "1", ROOT: {"list": [{"item": {"id": "1"}}]}}],
        ),
        (
            ["data", "*", "list", "data2", "*"],
            decoder_json,
            {
                "data": [
                    {"list": {"data2": [{"id": 1}, {"id": 2}]}},
                    {"list": {"data2": [{"id": 3}, {"id": 4}]}},
                ]
            },
            [
                {
                    "id": 1,
                    ROOT: {
                        "data": [
                            {"list": {"data2": [{"id": 1}, {"id": 2}]}},
                            {"list": {"data2": [{"id": 3}, {"id": 4}]}},
                        ]
                    },
                },
                {
                    "id": 2,
                    ROOT: {
                        "data": [
                            {"list": {"data2": [{"id": 1}, {"id": 2}]}},
                            {"list": {"data2": [{"id": 3}, {"id": 4}]}},
                        ]
                    },
                },
                {
                    "id": 3,
                    ROOT: {
                        "data": [
                            {"list": {"data2": [{"id": 1}, {"id": 2}]}},
                            {"list": {"data2": [{"id": 3}, {"id": 4}]}},
                        ]
                    },
                },
                {
                    "id": 4,
                    ROOT: {
                        "data": [
                            {"list": {"data2": [{"id": 1}, {"id": 2}]}},
                            {"list": {"data2": [{"id": 3}, {"id": 4}]}},
                        ]
                    },
                },
            ],
        ),
    ],
    ids=[
        "test_extract_from_empty_string",
        "test_extract_from_empty_object",
        "test_extract_from_empty_array",
        "test_extract_from_nonempty_object",
        "test_extract_from_nonempty_array",
        "test_extract_from_nonempty_array_with_nested_array",
        "test_extract_from_nonempty_array_with_nested_array2",
        "test_extract_from_nonempty_array2_with_nested_array",
        "test_extract_from_nonempty_array2_with_nested_array2",
        "test_extract_single_record_from_root_empty_parent",
        "test_extract_single_record_from_root",
        "test_extract_from_root_array",
        "test_extract_path_from_nonempty_array_with_nested_array_empty_parent",
        "test_extract_path_from_nonempty_array_with_nested_array",
        "test_extract_path_from_nonempty_array_with_nested_array2",
        "test_extract_path_from_nonempty_array2_with_nested_array",
        "test_extract_path_from_nonempty_array2_with_nested_array2",
        "test_nested_field",
        "test_field_in_config",
        "test_field_in_parameters",
        "test_field_does_not_exist",
        "test_nested_list",
        "test_complex_nested_list",
    ],
)
def test_dpath_extractor(field_path: List, decoder: Decoder, body, expected_records: List):
    extractor = DpathEnhancingExtractor(
        field_path=field_path, config=config, decoder=decoder, parameters=parameters
    )

    response = create_response(body)
    actual_records = list(extractor.extract_records(response))

    assert actual_records == expected_records
