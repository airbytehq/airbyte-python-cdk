# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""FAST Airbyte Standard Tests for the source_pokeapi_w_components source."""

from airbyte_cdk.test.standard_tests import DeclarativeSourceTestSuite

pytest_plugins = [
    "airbyte_cdk.test.standard_tests.pytest_hooks",
]


class TestSuiteSourcePokeAPI(DeclarativeSourceTestSuite):
    """Test suite for the source_pokeapi_w_components source.

    This class inherits from SourceTestSuiteBase and implements all of the tests in the suite.

    As long as the class name starts with "Test", pytest will automatically discover and run the
    tests in this class.
    """
