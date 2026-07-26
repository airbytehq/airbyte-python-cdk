# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for FAST Airbyte Standard Tests."""

import logging
from collections.abc import Iterable, Mapping
from typing import Any

import pytest

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteMessage,
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.source import Source
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.standard_tests._job_runner import IConnector
from airbyte_cdk.test.standard_tests.connector_base import ConnectorTestSuiteBase


class LegacyFileBasedConnector(Source):
    def __init__(
        self,
        catalog: ConfiguredAirbyteCatalog | None,
        config: dict[str, Any] | None,
        state: list[AirbyteStateMessage] | None,
    ) -> None:
        self.catalog = catalog
        self.config = config
        self.state = state

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> None:
        pass

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        return AirbyteCatalog(streams=[])

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterable[AirbyteMessage]:
        return []


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


def test_create_connector_instantiates_legacy_file_based_sources_with_runtime_args() -> None:
    class TestSuite(ConnectorTestSuiteBase):
        connector = LegacyFileBasedConnector

    catalog = ConfiguredAirbyteCatalog(streams=[])
    connector = TestSuite.create_connector(
        ConnectorTestScenario(config_dict={"folder_url": "https://example.com"}),
        catalog,
    )

    assert isinstance(connector, LegacyFileBasedConnector)
    assert connector.catalog == catalog
    assert connector.config == {"folder_url": "https://example.com"}
    assert connector.state is None
