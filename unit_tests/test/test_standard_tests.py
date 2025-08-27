# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for FAST Airbyte Standard Tests."""

from typing import Any

import pytest

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.source import Source
from airbyte_cdk.test.standard_tests._job_runner import IConnector


@pytest.mark.parametrize(
    "input, expected",
    [
        (ConcurrentDeclarativeSource, True),
        (Source, True),
        (None, False),
        ("", False),
        ([], False),
        ({}, False),
        (object(), False),
    ],
)
def test_is_iconnector_check(input: Any, expected: bool) -> None:
    """Assert whether inputs are valid as an IConnector object or class."""
    if isinstance(input, type):
        assert issubclass(input, IConnector) == expected
        return

    assert isinstance(input, IConnector) == expected
