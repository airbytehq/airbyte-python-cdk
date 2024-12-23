#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import gzip
import json
import zipfile
from io import BytesIO
from typing import Union

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders import ZipfileDecoder
from airbyte_cdk.sources.declarative.decoders.parsers import JsonParser


def create_zip_from_dict(data: Union[dict, list]):
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w") as zip_file:
        zip_file.writestr("data.json", data)
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


@pytest.mark.parametrize(
    "json_data",
    [
        {"test": "test"},
        [{"id": 1}, {"id": 2}],
    ],
)
def test_zipfile_decoder_with_valid_response(requests_mock, json_data):
    zipfile_decoder = ZipfileDecoder(parameters={}, parser=JsonParser)
    compressed_data = gzip.compress(json.dumps(json_data).encode())
    zipped_data = create_zip_from_dict(compressed_data)
    requests_mock.register_uri("GET", "https://airbyte.io/", content=zipped_data)
    response = requests.get("https://airbyte.io/")

    if isinstance(json_data, list):
        for i, actual in enumerate(zipfile_decoder.decode(response=response)):
            assert actual == json_data[i]
    else:
        assert next(zipfile_decoder.decode(response=response)) == json_data
