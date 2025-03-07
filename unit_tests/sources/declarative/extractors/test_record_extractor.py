#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from typing import Mapping

import pytest

from airbyte_cdk.sources.declarative.extractors.record_extractor import (
    SERVICE_KEY_PREFIX,
    exclude_service_keys,
    is_service_key,
)


@pytest.mark.parametrize(
    "original, expected",
    [
        ({}, {}),
        ({"k": "v"}, {"k": "v"}),
        ({"k": "v", "k2": "v"}, {"k": "v", "k2": "v"}),
        ({SERVICE_KEY_PREFIX + "k": "v"}, {}),
        ({SERVICE_KEY_PREFIX + "k": "v", "k": "v"}, {"k": "v"}),
        ({SERVICE_KEY_PREFIX + "k": "v", "k": "v", "k2": "v"}, {"k": "v", "k2": "v"}),
    ],
)
def test_exclude_service_keys(original: Mapping, expected: Mapping):
    assert exclude_service_keys(original) == expected


def test_service_field():
    assert is_service_key(SERVICE_KEY_PREFIX + "name")


def test_regular_field():
    assert not is_service_key("name")
