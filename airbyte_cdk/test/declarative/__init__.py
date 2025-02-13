# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Declarative tests framework.

This module provides fixtures and utilities for testing Airbyte sources and destinations
in a declarative way.
"""

from airbyte_cdk.test.declarative.test_suites import (
    ConnectorTestSuiteBase,
    DestinationTestSuiteBase,
    SourceTestSuiteBase,
)

__all__ = [
    "ConnectorTestSuiteBase",
    "DestinationTestSuiteBase",
    "SourceTestSuiteBase",
]
