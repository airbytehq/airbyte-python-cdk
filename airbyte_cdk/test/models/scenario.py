# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Run acceptance tests in PyTest.

These tests leverage the same `acceptance-test-config.yml` configuration files as the
acceptance tests in CAT, but they run in PyTest instead of CAT. This allows us to run
the acceptance tests in the same local environment as we are developing in, speeding
up iteration cycles.
"""

from __future__ import annotations

from pathlib import Path  # noqa: TC003  # Pydantic needs this (don't move to 'if typing' block)
from typing import Any, Literal, cast

import yaml
from pydantic import BaseModel, ConfigDict

from airbyte_cdk.test.models.outcome import ExpectedOutcome


class ConnectorTestScenario(BaseModel):
    """Acceptance test scenario, as a Pydantic model.

    This class represents an acceptance test scenario, which is a single test case
    that can be run against a connector. It is used to deserialize and validate the
    acceptance test configuration file.
    """

    # Allows the class to be hashable, which PyTest will require
    # when we use to parameterize tests.
    model_config = ConfigDict(frozen=True)

    class AcceptanceTestExpectRecords(BaseModel):
        path: Path
        exact_order: bool = False

    class AcceptanceTestFileTypes(BaseModel):
        skip_test: bool
        bypass_reason: str

    config_path: Path | None = None
    config_dict: dict[str, Any] | None = None

    id: str | None = None

    configured_catalog_path: Path | None = None
    timeout_seconds: int | None = None
    expect_records: AcceptanceTestExpectRecords | None = None
    file_types: AcceptanceTestFileTypes | None = None
    status: Literal["succeed", "failed"] | None = None

    def get_config_dict(
        self,
        *,
        connector_root: Path,
        empty_if_missing: bool,
    ) -> dict[str, Any]:
        """Return the config dictionary.

        If a config dictionary has already been loaded, return it. Otherwise, load
        the config file and return the dictionary.

        If `self.config_dict` and `self.config_path` are both `None`:
        - return an empty dictionary if `empty_if_missing` is True
        - raise a ValueError if `empty_if_missing` is False
        """
        if self.config_dict is not None:
            return self.config_dict

        if self.config_path is not None:
            config_path = self.config_path
            if not config_path.is_absolute():
                # We usually receive a relative path here. Let's resolve it.
                config_path = (connector_root / self.config_path).resolve().absolute()

            return cast(
                dict[str, Any],
                yaml.safe_load(config_path.read_text()),
            )

        if empty_if_missing:
            return {}

        raise ValueError("No config dictionary or path provided.")

    @property
    def expected_outcome(self) -> ExpectedOutcome:
        """Whether the test scenario expects an exception to be raised.

        Returns True if the scenario expects an exception, False if it does not,
        and None if there is no set expectation.
        """
        return ExpectedOutcome.from_status_str(self.status)

    @property
    def instance_name(self) -> str:
        return self.config_path.stem if self.config_path else "Unnamed Scenario"

    def __str__(self) -> str:
        if self.id:
            return f"'{self.id}' Test Scenario"
        if self.config_path:
            return f"'{self.config_path.name}' Test Scenario"

        return f"'{hash(self)}' Test Scenario"

    def without_expected_outcome(self) -> ConnectorTestScenario:
        """Return a copy of the scenario that does not expect failure or success.

        This is useful when running multiple steps, to defer the expectations to a later step.
        """
        return ConnectorTestScenario(
            **self.model_dump(exclude={"status"}),
        )

    def with_expecting_failure(self) -> ConnectorTestScenario:
        """Return a copy of the scenario that expects failure.

        This is useful when deriving new scenarios from existing ones.
        """
        if self.status == "failed":
            return self

        return ConnectorTestScenario(
            **self.model_dump(exclude={"status"}),
            status="failed",
        )

    def with_expecting_success(self) -> ConnectorTestScenario:
        """Return a copy of the scenario that expects success.

        This is useful when deriving new scenarios from existing ones.
        """
        if self.status == "succeed":
            return self

        return ConnectorTestScenario(
            **self.model_dump(exclude={"status"}),
            status="succeed",
        )
