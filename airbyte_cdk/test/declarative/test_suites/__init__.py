# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Declarative test suites.

Here we have base classes for a robust set of declarative connector test suites.
"""

from airbyte_cdk.test.declarative.test_suites.connector_base import ConnectorTestSuiteBase
from airbyte_cdk.test.declarative.test_suites.destination_base import DestinationTestSuiteBase
from airbyte_cdk.test.declarative.test_suites.source_base import SourceTestSuiteBase

__all__ = [
    "ConnectorTestSuiteBase",
    "DestinationTestSuiteBase",
    "SourceTestSuiteBase",
]
