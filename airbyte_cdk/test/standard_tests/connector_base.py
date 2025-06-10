# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for connector test suites."""

from __future__ import annotations

import abc
import importlib
import inspect
import os
import sys
from pathlib import Path
from typing import Literal, cast

import pytest
import yaml
from boltons.typeutils import classproperty

from airbyte_cdk.models import (
    AirbyteMessage,
    Type,
)
from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.standard_tests._job_runner import IConnector, run_test_job
from airbyte_cdk.test.standard_tests.docker_connectors import DockerConnector
from airbyte_cdk.test.standard_tests.models import (
    ConnectorTestScenario,
)
from airbyte_cdk.utils.connector_paths import (
    ACCEPTANCE_TEST_CONFIG,
    find_connector_root,
)


@pytest.fixture
def use_docker_image(request: pytest.FixtureRequest) -> str | bool:
    """Fixture to determine if a Docker image should be used for the test."""
    return request.config.getoption("use_docker_image")


class ConnectorTestSuiteBase(abc.ABC):
    """Base class for connector test suites."""

    connector_class: type[IConnector] | None = None
    """The connector class or a factory function that returns an scenario of IConnector."""

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
        *,
        use_docker_image: str | bool,
    ) -> IConnector:
        """Instantiate the connector class."""
        """Get the connector class for the test suite.

        This assumes a python connector and should be overridden by subclasses to provide the
        specific connector class to be tested.
        """
        if use_docker_image:
            return cls.create_docker_connector(
                docker_image=use_docker_image,
            )

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
            return cast(
                type[IConnector],
                getattr(module, expected_class_name),
            )()
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
            return cast(
                type[IConnector],
                getattr(module, matching_class_name),
            )()

    @classmethod
    def create_docker_connector(
        cls,
        docker_image: str | Literal[True],
    ) -> IConnector:
        """Create a connector instance using Docker."""
        if not docker_image:
            raise ValueError("Docker image is required to create a Docker connector.")

        # Create the connector object by building the connector
        if docker_image is True:
            return DockerConnector.from_connector_directory(
                connector_directory=cls.get_connector_root_dir(),
            )

        if not isinstance(docker_image, str):
            raise ValueError(
                "Expected `docker_image` to be 'True' or of type `str`. "
                f"Type found: {type(docker_image).__name__}"
            )

        # Create the connector object using the provided Docker image
        return DockerConnector(
            connector_name=cls.get_connector_root_dir().name,
            docker_image=docker_image,
        )

    # Test Definitions (Generic for all connectors)

    def test_spec(
        self,
        *,
        use_docker_image: str | bool,
    ) -> None:
        """Standard test for `spec`.

        This test does not require a `scenario` input, since `spec`
        does not require any inputs.

        We assume `spec` should always succeed and it should always generate
        a valid `SPEC` message.

        Note: the parsing of messages by type also implicitly validates that
        the generated `SPEC` message is valid JSON.
        """
        scenario = ConnectorTestScenario()  # Empty scenario, empty config
        result = run_test_job(
            verb="spec",
            test_scenario=scenario,
            connector=self.create_connector(
                scenario=scenario,
                use_docker_image=use_docker_image,
            ),
        )
        # If an error occurs, it will be raised above.

        assert len(result.spec_messages) == 1, (
            "Expected exactly 1 spec message but got {len(result.spec_messages)}",
            result.errors,
        )

    def test_check(
        self,
        scenario: ConnectorTestScenario,
        *,
        use_docker_image: str | bool,
    ) -> None:
        """Run `connection` acceptance tests."""
        result: entrypoint_wrapper.EntrypointOutput = run_test_job(
            self.create_connector(
                scenario,
                use_docker_image=use_docker_image,
            ),
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
        categories = ["connection", "spec"]
        all_tests_config = yaml.safe_load(cls.acceptance_test_config_path.read_text())
        if "acceptance_tests" not in all_tests_config:
            raise ValueError(
                f"Acceptance tests config not found in {cls.acceptance_test_config_path}."
                f" Found only: {str(all_tests_config)}."
            )

        test_scenarios: list[ConnectorTestScenario] = []
        for category in categories:
            if (
                category not in all_tests_config["acceptance_tests"]
                or "tests" not in all_tests_config["acceptance_tests"][category]
            ):
                continue

            test_scenarios.extend(
                [
                    ConnectorTestScenario.model_validate(test)
                    for test in all_tests_config["acceptance_tests"][category]["tests"]
                    if "config_path" in test and "iam_role" not in test["config_path"]
                ]
            )

        connector_root = cls.get_connector_root_dir().absolute()
        for test in test_scenarios:
            if test.config_path:
                test.config_path = connector_root / test.config_path
            if test.configured_catalog_path:
                test.configured_catalog_path = connector_root / test.configured_catalog_path

        return test_scenarios
