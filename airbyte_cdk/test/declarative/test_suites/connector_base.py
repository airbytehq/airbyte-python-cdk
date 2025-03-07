# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for connector test suites."""

from __future__ import annotations

import abc
import functools
import inspect
import sys
from pathlib import Path
from typing import Any, Callable, Literal

import pytest
import yaml
from pydantic import BaseModel
from typing_extensions import override

from airbyte_cdk import Connector
from airbyte_cdk.models import (
    AirbyteMessage,
    Type,
)
from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.declarative.models import (
    ConnectorTestScenario,
)
from airbyte_cdk.test.declarative.utils.job_runner import run_test_job

ACCEPTANCE_TEST_CONFIG = "acceptance-test-config.yml"


class JavaClass(str):
    """A string that represents a Java class."""


class DockerImage(str):
    """A string that represents a Docker image."""


class RunnableConnector(abc.ABC):
    """A connector that can be run in a test scenario."""

    @abc.abstractmethod
    def launch(cls, args: list[str] | None): ...


def generate_tests(metafunc):
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

    acceptance_test_file_path = Path("./acceptance-test-config.json")
    """The path to the acceptance test config file.

    By default, this is set to the `acceptance-test-config.json` file in
    the root of the connector source directory.
    """

    connector: type[Connector] | Path | JavaClass | DockerImage | None = None
    """The connector class or path to the connector to test."""

    working_dir: Path | None = None
    """The root directory of the connector source code."""

    @classmethod
    def create_connector(
        cls, scenario: ConnectorTestScenario
    ) -> Source | AbstractSource | ConcurrentDeclarativeSource | RunnableConnector:
        """Instantiate the connector class."""
        raise NotImplementedError("Subclasses must implement this method.")

    def run_test_scenario(
        self,
        verb: Literal["read", "check", "discover"],
        test_scenario: ConnectorTestScenario,
        *,
        catalog: dict | None = None,
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
            "Expected exactly one CONNECTION_STATUS message. Got: \n" + "\n".join(result._messages)
        )

    @classmethod
    @property
    def acceptance_test_config_path(self) -> Path:
        """Get the path to the acceptance test config file.

        Check vwd and parent directories of cwd for the config file, and return the first one found.

        Give up if the config file is not found in any parent directory.
        """
        current_dir = Path.cwd()
        for parent_dir in current_dir.parents:
            config_path = parent_dir / ACCEPTANCE_TEST_CONFIG
            if config_path.exists():
                return config_path
        raise FileNotFoundError(
            f"Acceptance test config file not found in any parent directory from : {Path.cwd()}"
        )

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
        working_dir = cls.working_dir or Path()
        for test in tests_scenarios:
            if test.config_path:
                test.config_path = working_dir / test.config_path
            if test.configured_catalog_path:
                test.configured_catalog_path = working_dir / test.configured_catalog_path
        return tests_scenarios
