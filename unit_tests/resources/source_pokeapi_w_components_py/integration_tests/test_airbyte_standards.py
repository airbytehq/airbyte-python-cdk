# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""FAST Airbyte Standard Tests for the source_pokeapi_w_components source."""

from airbyte_cdk.test.declarative.test_suites import (
    DeclarativeSourceTestSuite,
    generate_tests,
)


def pytest_generate_tests(metafunc) -> None:
    generate_tests(metafunc)


class TestSuiteSourcePokeAPI(DeclarativeSourceTestSuite):
    """Test suite for the source_pokeapi_w_components source.

    This class inherits from SourceTestSuiteBase and implements all of the tests in the suite.

    As long as the class name starts with "Test", pytest will automatically discover and run the
    tests in this class.
    """
