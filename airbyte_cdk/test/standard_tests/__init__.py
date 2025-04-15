# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Declarative test suites.

Here we have base classes for a robust set of declarative connector test suites.
"""

from airbyte_cdk.test.standard_tests.connector_base import (
    ConnectorTestScenario,
    ConnectorTestSuiteBase,
)
from airbyte_cdk.test.standard_tests.declarative_sources import (
    DeclarativeSourceTestSuite,
)
from airbyte_cdk.test.standard_tests.destination_base import DestinationTestSuiteBase
from airbyte_cdk.test.standard_tests.source_base import SourceTestSuiteBase

__all__ = [
    "ConnectorTestScenario",
    "ConnectorTestSuiteBase",
    "DeclarativeSourceTestSuite",
    "DestinationTestSuiteBase",
    "SourceTestSuiteBase",
]
