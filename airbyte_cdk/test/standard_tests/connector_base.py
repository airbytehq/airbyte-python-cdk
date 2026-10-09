# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for connector test suites."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import TYPE_CHECKING, cast

from boltons.typeutils import classproperty

from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.models import (
    ConnectorTestScenario,
)
from airbyte_cdk.test.standard_tests._assertions import assert_config_matches_spec
from airbyte_cdk.test.standard_tests._job_runner import IConnector, run_test_job
from airbyte_cdk.test.standard_tests.docker_base import (
    DockerConnectorTestSuite,
    skip_unless_runs,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte_cdk.test import entrypoint_wrapper


class ConnectorTestSuiteBase(DockerConnectorTestSuite):
    """Base class for Python connector test suites."""

    connector: type[IConnector] | Callable[[], IConnector] | None  # type: ignore [reportRedeclaration]
    """The connector class or a factory function that returns an scenario of IConnector."""

    @classproperty  # type: ignore [no-redef]
    def connector(cls) -> type[IConnector] | Callable[[], IConnector] | None:
        """Get the connector class for the test suite.

        This assumes a python connector and should be overridden by subclasses to provide the
        specific connector class to be tested.
        """
        connector_root = cls.get_connector_root_dir()
        connector_name = cls.connector_name

        expected_module_name = connector_name.replace("-", "_").lower()
        expected_class_name = connector_name.replace("-", "_").title().replace("_", "")

        # dynamically import and get the connector class: <expected_module_name>.<expected_class_name>

        cwd_snapshot = Path().absolute()
        os.chdir(connector_root)

        # Dynamically import the module
        try:
            module = importlib.import_module(expected_module_name)
        except ModuleNotFoundError as e:
            raise ImportError(
                f"Could not import module '{expected_module_name}'. "
                "Please ensure you are running from within the connector's virtual environment, "
                "for instance by running `poetry run airbyte-cdk connector test` from the "
                "connector directory. If the issue persists, check that the connector "
                f"module matches the expected module name '{expected_module_name}' and that the "
                f"connector class matches the expected class name '{expected_class_name}'. "
                "Alternatively, you can run `airbyte-cdk image test` to run a subset of tests "
                "against the connector's image."
            ) from e
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
    def create_connector(
        cls,
        scenario: ConnectorTestScenario | None,
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
        """Run `connection` acceptance tests.

        Runs for configs listed under `connection` in `acceptance-test-config.yml`. Scenarios
        declared with `status: exception` expect `check` to raise instead of reporting a status;
        for those, only the presence of a trace error is asserted (in `run_test_job`).
        """
        skip_unless_runs(scenario, "check")
        result: entrypoint_wrapper.EntrypointOutput = run_test_job(
            self.create_connector(scenario),
            "check",
            test_scenario=scenario,
            connector_root=self.get_connector_root_dir(),
        )
        if scenario.expected_outcome.expect_uncaught_error():
            # An uncaught error is the expected outcome; no CONNECTION_STATUS is required.
            return

        assert len(result.connection_status_messages) == 1, (
            "Expected exactly one CONNECTION_STATUS message. "
            f"Got: {result.connection_status_messages!s}"
        )

    def test_config_matches_spec(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Validate the scenario's config against the connector's `spec`.

        Runs for configs listed under `spec` in `acceptance-test-config.yml` (the CAT
        `test_config_match_spec` check): the connector's own `connectionSpecification` must
        accept the config.
        """
        skip_unless_runs(scenario, "spec")
        connector_root = self.get_connector_root_dir()
        spec_result: entrypoint_wrapper.EntrypointOutput = run_test_job(
            self.create_connector(scenario),
            "spec",
            connector_root=connector_root,
            test_scenario=scenario.without_expected_outcome(),
        )
        assert_config_matches_spec(
            config=scenario.get_config_dict(connector_root=connector_root, empty_if_missing=False),
            spec_result=spec_result,
            connector_name=connector_root.absolute().name,
            scenario_id=scenario.id,
        )
