# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Run acceptance tests in PyTest.

These tests leverage the same `acceptance-test-config.yml` configuration files as the
acceptance tests in CAT, but they run in PyTest instead of CAT. This allows us to run
the acceptance tests in the same local environment as we are developing in, speeding
up iteration cycles.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, cast

import yaml
from pydantic import BaseModel


class ConnectorTestScenario(BaseModel):
    """Acceptance test scenario, as a Pydantic model.

    This class represents an acceptance test scenario, which is a single test case
    that can be run against a connector. It is used to deserialize and validate the
    acceptance test configuration file.
    """

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

    def get_config_dict(self) -> dict[str, Any]:
        """Return the config dictionary.

        If a config dictionary has already been loaded, return it. Otherwise, load
        the config file and return the dictionary.
        """
        if self.config_dict:
            return self.config_dict

        if self.config_path:
            return cast(dict[str, Any], yaml.safe_load(self.config_path.read_text()))

        raise ValueError("No config dictionary or path provided.")

    @property
    def expect_exception(self) -> bool:
        return self.status and self.status == "failed" or False

    @property
    def instance_name(self) -> str:
        return self.config_path.stem if self.config_path else "Unnamed Scenario"

    def __str__(self) -> str:
        if self.id:
            return f"'{self.id}' Test Scenario"
        if self.config_path:
            return f"'{self.config_path.name}' Test Scenario"

        return f"'{hash(self)}' Test Scenario"
