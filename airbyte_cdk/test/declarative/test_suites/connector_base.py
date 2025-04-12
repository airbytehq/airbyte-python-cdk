# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for connector test suites."""

from __future__ import annotations

import abc
import inspect
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, cast

import pytest
import yaml
from boltons.typeutils import classproperty

from airbyte_cdk import Connector
from airbyte_cdk.models import (
    AirbyteMessage,
    Type,
)
from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.declarative.models import (
    ConnectorTestScenario,
)
from airbyte_cdk.test.declarative.utils.job_runner import IConnector, run_test_job

ACCEPTANCE_TEST_CONFIG = "acceptance-test-config.yml"
MANIFEST_YAML = "manifest.yaml"


class JavaClass(str):
    """A string that represents a Java class."""


class DockerImage(str):
    """A string that represents a Docker image."""


class RunnableConnector(abc.ABC):
    """A connector that can be run in a test scenario."""

    @abc.abstractmethod
    def launch(cls, args: list[str] | None) -> None: ...


def generate_tests(metafunc: pytest.Metafunc) -> None:
    """
    A helper for pytest_generate_tests hook.

    If a test method (in a class subclassed from our base class)
    declares an argument 'instance', this function retrieves the
    'scenarios' attribute from the test class and parametrizes that
    test with the values from 'scenarios'.

    ## Usage

    ```python
    from airbyte_cdk.test.declarative.test_suites.connector_base import (
        generate_tests,
        ConnectorTestSuiteBase,
    )

    def pytest_generate_tests(metafunc):
        generate_tests(metafunc)

    class TestMyConnector(ConnectorTestSuiteBase):
        ...

    ```
    """
    # Check if the test function requires an 'instance' argument
    if "instance" in metafunc.fixturenames:
        # Retrieve the test class
        test_class = metafunc.cls
        if test_class is None:
            raise ValueError("Expected a class here.")
        # Get the 'scenarios' attribute from the class
        scenarios_attr = getattr(test_class, "get_scenarios", None)
        if scenarios_attr is None:
            raise ValueError(
                f"Test class {test_class} does not have a 'scenarios' attribute. "
                "Please define the 'scenarios' attribute in the test class."
            )

        scenarios = test_class.get_scenarios()
        ids = [str(scenario) for scenario in scenarios]
        metafunc.parametrize("instance", scenarios, ids=ids)


class ConnectorTestSuiteBase(abc.ABC):
    """Base class for connector test suites."""

    connector: type[IConnector] | Callable[[], IConnector] | None = None
    """The connector class or a factory function that returns an instance of IConnector."""

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

    def run_test_scenario(
        self,
        verb: Literal["read", "check", "discover"],
        test_scenario: ConnectorTestScenario,
        *,
        catalog: dict[str, Any] | None = None,
    ) -> entrypoint_wrapper.EntrypointOutput:
        """Run a test job from provided CLI args and return the result."""
        return run_test_job(
            self.create_connector(test_scenario),
            verb,
            test_instance=test_scenario,
            catalog=catalog,
        )

    # Test Definitions

    def test_check(
        self,
        instance: ConnectorTestScenario,
    ) -> None:
        """Run `connection` acceptance tests."""
        result = self.run_test_scenario(
            "check",
            test_scenario=instance,
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
        for parent in cls.get_test_class_dir().parents:
            if (parent / MANIFEST_YAML).exists():
                return parent
            if (parent / ACCEPTANCE_TEST_CONFIG).exists():
                return parent
            if parent.name == "airbyte_cdk":
                break
        # If we reach here, we didn't find the manifest file in any parent directory
        # Check if the manifest file exists in the current directory
        for parent in Path.cwd().parents:
            if (parent / MANIFEST_YAML).exists():
                return parent
            if (parent / ACCEPTANCE_TEST_CONFIG).exists():
                return parent
            if parent.name == "airbyte_cdk":
                break

        raise FileNotFoundError(
            "Could not find connector root directory relative to "
            f"'{str(cls.get_test_class_dir())}' or '{str(Path.cwd())}'."
        )

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
