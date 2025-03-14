# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for destination test suites."""

from airbyte_cdk.test.declarative.test_suites.connector_base import ConnectorTestSuiteBase


class DestinationTestSuiteBase(ConnectorTestSuiteBase):
    """Base class for destination test suites.

    This class provides a base set of functionality for testing destination connectors, and it
    inherits all generic connector tests from the `ConnectorTestSuiteBase` class.
    """
