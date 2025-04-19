# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for connector test suites."""

from __future__ import annotations

import abc
import importlib
import inspect
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import cast

import yaml
from boltons.typeutils import classproperty

from airbyte_cdk.models import (
    AirbyteMessage,
    Type,
)
from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.standard_tests._job_runner import IConnector, run_test_job
from airbyte_cdk.test.standard_tests.models import (
    ConnectorTestScenario,
)
from airbyte_cdk.test.standard_tests.test_resources import (
    ACCEPTANCE_TEST_CONFIG,
    find_connector_root,
)


class ConnectorTestSuiteBase(abc.ABC):
    """Base class for connector test suites."""

    connector: type[IConnector] | Callable[[], IConnector] | None  # type: ignore [reportRedeclaration]
    """The connector class or a factory function that returns an scenario of IConnector."""

    @classproperty  # type: ignore [no-redef]
    def connector(cls) -> type[IConnector] | Callable[[], IConnector] | None:
        """Get the connector class for the test suite.

        This assumes a python connector and should be overridden by subclasses to provide the
        specific connector class to be tested.
        """
        connector_root = cls.get_connector_root_dir()
        connector_name = connector_root.absolute().name

        expected_module_name = connector_name.replace("-", "_").lower()
        expected_class_name = connector_name.replace("-", "_").title().replace("_", "")

        # dynamically import and get the connector class: <expected_module_name>.<expected_class_name>

        cwd_snapshot = Path().absolute()
        os.chdir(connector_root)

        # Dynamically import the module
        try:
            module = importlib.import_module(expected_module_name)
        except ModuleNotFoundError as e:
            raise ImportError(f"Could not import module '{expected_module_name}'.") from e
        finally:
            # Change back to the original working directory
            os.chdir(cwd_snapshot)

        # Dynamically get the class from the module
        try:
            return cast(type[IConnector], getattr(module, expected_class_name))
        except AttributeError as e:
            # We did not find it based on our expectations, so let's check if we can find it
            # with a case-insensitive match.
            matching_class_name = next(
                (name for name in dir(module) if name.lower() == expected_class_name.lower()),
                None,
            )
            if not matching_class_name:
                raise ImportError(
                    f"Module '{expected_module_name}' does not have a class named '{expected_class_name}'."
                ) from e
            return cast(type[IConnector], getattr(module, matching_class_name))

    @classmethod
    def get_test_class_dir(cls) -> Path:
        """Get the file path that contains the class."""
        module = sys.modules[cls.__module__]
        # Get the directory containing the test file
        return Path(inspect.getfile(module)).parent

    @classmethod
    def create_connector(
        cls,
        scenario: ConnectorTestScenario,
    ) -> IConnector:
        """Instantiate the connector class."""
        connector = cls.connector  # type: ignore
        if connector:
            if callable(connector) or isinstance(connector, type):
                # If the connector is a class or factory function, instantiate it:
                return cast(IConnector, connector())  # type: ignore [redundant-cast]

        # Otherwise, we can't instantiate the connector. Fail with a clear error message.
        raise NotImplementedError(
            "No connector class or connector factory function provided. "
            "Please provide a class or factory function in `cls.connector`, or "
            "override `cls.create_connector()` to define a custom initialization process."
        )

    # Test Definitions

    def test_check(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Run `connection` acceptance tests."""
        result: entrypoint_wrapper.EntrypointOutput = run_test_job(
            self.create_connector(scenario),
            "check",
            test_scenario=scenario,
        )
        conn_status_messages: list[AirbyteMessage] = [
            msg for msg in result._messages if msg.type == Type.CONNECTION_STATUS
        ]  # noqa: SLF001  # Non-public API
        assert len(conn_status_messages) == 1, (
            f"Expected exactly one CONNECTION_STATUS message. Got: {result._messages}"
        )

    @classmethod
    def get_connector_root_dir(cls) -> Path:
        """Get the root directory of the connector."""
        return find_connector_root([cls.get_test_class_dir(), Path.cwd()])

    @classproperty
    def acceptance_test_config_path(cls) -> Path:
        """Get the path to the acceptance test config file."""
        result = cls.get_connector_root_dir() / ACCEPTANCE_TEST_CONFIG
        if result.exists():
            return result

        raise FileNotFoundError(f"Acceptance test config file not found at: {str(result)}")

    @classmethod
    def get_scenarios(
        cls,
    ) -> list[ConnectorTestScenario]:
        """Get acceptance tests for a given category.

        This has to be a separate function because pytest does not allow
        parametrization of fixtures with arguments from the test class itself.
        """
        category = "connection"
        all_tests_config = yaml.safe_load(cls.acceptance_test_config_path.read_text())
        if "acceptance_tests" not in all_tests_config:
            raise ValueError(
                f"Acceptance tests config not found in {cls.acceptance_test_config_path}."
                f" Found only: {str(all_tests_config)}."
            )
        if category not in all_tests_config["acceptance_tests"]:
            return []
        if "tests" not in all_tests_config["acceptance_tests"][category]:
            raise ValueError(f"No tests found for category {category}")

        tests_scenarios = [
            ConnectorTestScenario.model_validate(test)
            for test in all_tests_config["acceptance_tests"][category]["tests"]
            if "iam_role" not in test["config_path"]
        ]
        connector_root = cls.get_connector_root_dir().absolute()
        for test in tests_scenarios:
            if test.config_path:
                test.config_path = connector_root / test.config_path
            if test.configured_catalog_path:
                test.configured_catalog_path = connector_root / test.configured_catalog_path

        return tests_scenarios
