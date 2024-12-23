#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json

import pytest

from airbyte_cdk.sources.declarative.decoders.parsers import JsonParser


@pytest.mark.parametrize(
    "raw_data, expected",
    [
        (json.dumps({"data-type": "string"}), {"data-type": "string"}),
        (json.dumps({"data-type": "bytes"}).encode("utf-8"), {"data-type": "bytes"}),
        (
            bytearray(json.dumps({"data-type": "bytearray"}).encode("utf-8")),
            {"data-type": "bytearray"},
        ),
        (json.dumps([{"id": 1}, {"id": 2}]), [{"id": 1}, {"id": 2}]),
    ],
    ids=[
        "test_with_str",
        "test_with_bytes",
        "test_with_bytearray",
        "test_with_string_data_containing_list",
    ],
)
def test_json_parser_with_valid_data(raw_data, expected):
    for i, actual in enumerate(JsonParser().parse(raw_data)):
        if isinstance(expected, list):
            assert actual == expected[i]
        else:
            assert actual == expected
