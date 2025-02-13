# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for connector test suites."""

from __future__ import annotations

import abc
from pathlib import Path
from typing import Any, Literal

import pytest
import yaml
from airbyte_connector_tester.job_runner import run_test_job
from pydantic import BaseModel

from airbyte_cdk import Connector
from airbyte_cdk.models import (
    AirbyteMessage,
    Type,
)
from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.declarative.models import (
    AcceptanceTestScenario,
)

ACCEPTANCE_TEST_CONFIG_PATH = Path("acceptance-test-config.yml")


class ConnectorTestSuiteBase(abc.ABC):
    """Base class for connector test suites."""

    acceptance_test_file_path = Path("./acceptance-test-config.json")
    """The path to the acceptance test config file.

    By default, this is set to the `acceptance-test-config.json` file in
    the root of the connector source directory.
    """

    connector_class: type[Connector]
    """The connector class to test."""

    # Public Methods - Subclasses may override these

    @abc.abstractmethod
    def new_connector(self, **kwargs: dict[str, Any]) -> Connector:
        """Create a new connector instance.

        By default, this returns a new instance of the connector class. Subclasses
        may override this method to generate a dynamic connector instance.
        """
        return self.connector_factory()

    # Internal Methods - We don't expect subclasses to override these

    @classmethod
    def _get_acceptance_tests(
        category: str,
        accept_test_config_path: Path = ACCEPTANCE_TEST_CONFIG_PATH,
    ) -> list[AcceptanceTestScenario]:
        all_tests_config = yaml.safe_load(accept_test_config_path.read_text())
        if "acceptance_tests" not in all_tests_config:
            raise ValueError(f"Acceptance tests config not found in {accept_test_config_path}")
        if category not in all_tests_config["acceptance_tests"]:
            return []
        if "tests" not in all_tests_config["acceptance_tests"][category]:
            raise ValueError(f"No tests found for category {category}")

        return [
            AcceptanceTestScenario.model_validate(test)
            for test in all_tests_config["acceptance_tests"][category]["tests"]
            if "iam_role" not in test["config_path"]
        ]

    # Test Definitions

    @pytest.mark.parametrize(
        "test_input,expected",
        [
            ("3+5", 8),
            ("2+4", 6),
            ("6*9", 54),
        ],
    )
    def test_use_plugin_parametrized_test(
        self,
        test_input,
        expected,
    ):
        assert eval(test_input) == expected

    @pytest.mark.parametrize(
        "instance",
        self._get_acceptance_tests("connection"),
        ids=lambda instance: instance.instance_name,
    )
    def test_check(
        self,
        instance: AcceptanceTestScenario,
    ) -> None:
        """Run `connection` acceptance tests."""
        result: entrypoint_wrapper.EntrypointOutput = run_test_job(
            self.new_connector(),
            "check",
            test_instance=instance,
        )
        conn_status_messages: list[AirbyteMessage] = [
            msg for msg in result._messages if msg.type == Type.CONNECTION_STATUS
        ]  # noqa: SLF001  # Non-public API
        assert len(conn_status_messages) == 1, (
            "Expected exactly one CONNECTION_STATUS message. Got: \n" + "\n".join(result._messages)
        )
