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
            (bytearray(json.dumps({"data-type": "bytearray"}).encode("utf-8")), {"data-type": "bytearray"}),
        ],
        ids=["test_with_str", "test_with_bytes", "test_with_bytearray"]
)
def test_json_parser_with_valid_data(raw_data, expected):
    assert next(JsonParser().parse(raw_data)) == expected
