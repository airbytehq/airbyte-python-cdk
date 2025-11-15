# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Models to define test scenarios (smoke tests) which leverage the Standard Tests framework.

NOTE: The `acceptance-test-config.yml` file has been deprecated in favor of the new format.

Connector smoke tests should be defined in the connector's `metadata.yaml` file, under the
`connectorTestSuitesOptions` section, as shown in the example below:

## Basic Configuration of Scenarios

```yaml
data:
  # ...
  connectorTestSuitesOptions:
    # ...
    - suite: smokeTests
      scenarios:
        - name: default
          config_file: secrets/config_oauth.json
        - name: invalid_config
          config_file: integration_tests/invalid_config.json
          expect_failure: true
```

You can also specify config settings inline, instead of using a config file:

```yaml
data:
  # ...
  connectorTestSuitesOptions:
    # ...
    - suite: smokeTests
      scenarios:
        - name: default
          config_file: secrets/config_oauth.json
          config_settings:
            # This will override any matching settings in `config_file`:
            start_date: "2025-01-01T00:00:00Z"
        - name: invalid_config
          # No config file needed if using fully hard-coded settings:
          config_settings:
            client_id: invalid
            client_secret: invalid
```

## Streams Filtering

There are several ways to filter which streams are read during a test scenario:

- `only_streams`: A list of stream names to include in the scenario.
- `exclude_streams`: A list of stream names to exclude from the scenario.
- `suggested_streams_only`: A boolean indicating whether to limit to the connector's suggested
  streams list, if present. (Looks for `data.suggestedStreams` field in `metadata.yaml`.)

### Stream Filtering Examples

Filter for just one stream:

```yaml
data:
  # ...
  connectorTestSuitesOptions:
    # ...
    - suite: smokeTests
      scenarios:
        - name: default
          config_file: secrets/config_oauth.json
          only_streams:
            - users
```

Exclude a set of premium or restricted streams:

```yaml
data:
  # ...
  connectorTestSuitesOptions:
    # ...
    - suite: smokeTests
      scenarios:
        - name: default # exclude premium streams
          exclude_streams:
            - premium_users
            - restricted_users
```

Filter to just the suggested streams, minus a specific excluded streams:

```yaml
data:
  # ...
  connectorTestSuitesOptions:
    # ...
    - suite: smokeTests
      scenarios:
        - name: default # suggested streams, except restricted_users
          suggested_streams_only: true
          exclude_streams:
            - restricted_users

## Legacy Configuration

For legacy purposes, these tests can leverage the same `acceptance-test-config.yml` configuration
files as the acceptance tests in CAT, but they run in PyTest instead of CAT. This allows us to run
the acceptance tests in the same local environment as we are developing in, speeding
up iteration cycles.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from pathlib import Path  # noqa: TC003  # Pydantic needs this (don't move to 'if typing' block)
from typing import TYPE_CHECKING, Any, Literal, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field

from airbyte_cdk.test.models.outcome import ExpectedOutcome

if TYPE_CHECKING:
    from collections.abc import Generator


_LEGACY_FAILURE_STATUSES: set[str] = {"failed", "exception"}


class ConnectorTestScenario(BaseModel):
    """Smoke test scenario, as a Pydantic model."""

    name: str = Field(kw_only=True)
    """What to call this scenario in test reports.

    Common names include:
    - "default": the default scenario for a connector, with a valid config.
    - "invalid_config": a scenario with an invalid config, to test error handling.
    - "oauth_config": a scenario that uses OAuth for authentication.
    """

    config_file: Path | None = Field(kw_only=True, default=None)
    """Relative path to the config file to use for this scenario."""

    config_settings: dict[str, Any] | None = Field(default=None, kw_only=True)
    """Optional dictionary of config settings to use for this scenario.

    If both `config_settings` and `config_file` are provided, keys in `config_settings` take precedence over
    corresponding settings within `config_file`. This allows a single secrets file to be used for multiple scenarios,
    with scenario-specific overrides applied as needed.
    """

    expect_failure: bool = Field(default=False, kw_only=True)
    """Whether the scenario is expected to fail."""

    only_streams: list[str] | None = Field(default=None, kw_only=True)
    """List of stream names to include in the scenario."""

    exclude_streams: list[str] | None = Field(default=None, kw_only=True)
    """List of stream names to exclude from the scenario."""

    suggested_streams_only: bool = Field(default=False, kw_only=True)
    """Whether to limit to the connector's suggested streams list, if present."""

    suggested_streams: list[str] | None = Field(default=None, kw_only=True)
    """List of suggested stream names for the connector (if provided)."""

    configured_catalog_path: Path | None = Field(default=None, kw_only=True)
    """Path to the configured catalog file for the scenario."""

    def get_streams_filter(
        self,
    ) -> Callable[[str], bool]:
        """Return a function that filters streams based on the scenario's only_streams and exclude_streams.

        If neither only_streams nor exclude_streams are set, return None.
        """

        def filter_fn(stream_name: str) -> bool:
            if self.only_streams is not None and stream_name not in self.only_streams:
                # Stream is not in the `only_streams` list, exclude it.
                return False

            if self.exclude_streams is not None and stream_name in self.exclude_streams:
                # Stream is in the `exclude_streams` list, exclude it.
                return False

            # No exclusion reason found, include the stream.
            return True

        return filter_fn

    @classmethod
    def from_metadata_yaml(cls, metadata_yaml: Path) -> list[ConnectorTestScenario] | None:
        """Return a list of scenarios defined within a metadata YAML file.

        Example `metadata_yaml` content:
        ```yaml

        scenarios:
        data:
            # ...
            connectorTestSuitesOptions:
                # ...
                - suite: smokeTests
                scenarios:
                    - name: default
                    config_file: secrets/config_oauth.json
                    - name: invalid_config
                    config_file: integration_tests/invalid_config.json
                    expect_failure: true
        ```

        This simpler config replaces the legacy `acceptance-test-config.yml` file for
        defining smoke test (previously called "Acceptance Test") scenarios for a connector.

        Returns:
          - None if the `smokeTests` suite is not defined in the metadata.
          - An empty list if the `smokeTests` suite is defined but has no scenarios.
          - A list of `ConnectorTestScenario` instances if the `smokeTests` suite is defined
            and has scenarios.
        """
        metadata_data = yaml.safe_load(metadata_yaml.read_text()).get("data", {})
        if not metadata_data:
            raise ValueError(f"Metadata YAML file {metadata_yaml} is missing 'data' section.")

        connector_test_suite_options = metadata_data.get("connectorTestSuitesOptions", [])
        smoke_test_config: dict[str, Any] | None = next(
            (
                option
                for option in connector_test_suite_options
                if option.get("suite") == "smokeTests"
            ),
            None,
        )
        if smoke_test_config is None:
            # Return `None` because the `smokeTests` suite is fully undefined.
            return None

        suggested_streams = metadata_data.get("suggestedStreams")

        result: list[ConnectorTestScenario] = []

        return [
            cls.from_metadata_smoke_test_definition(
                definition=scenario,
                connector_root=metadata_yaml.parent,
                suggested_streams=suggested_streams,
            )
            for scenario in smoke_test_config.get("scenarios", [])
        ]

    @classmethod
    def from_metadata_smoke_test_definition(
        cls,
        definition: dict[str, Any],
        connector_root: Path,
        suggested_streams: list[str] | None,
    ) -> ConnectorTestScenario:
        """Return a scenario defined within a smoke test definition.

        Example `definition` content:
        ```yaml
        name: default
        config_file: secrets/config_oauth.json
        expect_failure: true
        ```

        This simpler config replaces the legacy `acceptance-test-config.yml` file for
        defining smoke test (previously called "Acceptance Test") scenarios for a connector.
        """
        if "config_file" not in definition:
            raise ValueError("Smoke test scenario definition must include a 'config_file' field.")
        if "name" not in definition:
            raise ValueError("Smoke test scenario definition must include a 'name' field.")

        return ConnectorTestScenario.model_validate({
            **definition,
            "suggested_streams": suggested_streams,
        })

    def with_expecting_failure(self) -> ConnectorTestScenario:
        """Return a copy of the scenario that expects failure.

        This is useful when deriving new scenarios from existing ones.
        """
        if self.expect_failure is True:
            return self

        return ConnectorTestScenario(
            **self.model_dump(exclude={"expect_failure"}),
            expect_failure=True,
        )

    def with_expecting_success(self) -> ConnectorTestScenario:
        """Return a copy of the scenario that expects success.

        This is useful when deriving new scenarios from existing ones.
        """
        if self.expect_failure is False:
            return self

        return ConnectorTestScenario(
            **self.model_dump(exclude={"expect_failure"}),
            expect_failure=False,
        )

    @property
    def requires_creds(self) -> bool:
        """Return True if the scenario requires credentials to run."""
        return bool(self.config_file and "secrets" in self.config_file.parts)

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
        if not empty_if_missing and self.config_file is None and self.config_settings is None:
            raise ValueError("No config dictionary or path provided.")

        result: dict[str, Any] = {}
        if self.config_file is not None:
            config_path = self.config_file
            if not config_path.is_absolute():
                # We usually receive a relative path here. Let's resolve it.
                config_path = (connector_root / self.config_file).resolve().absolute()

            result.update(
                cast(
                    dict[str, Any],
                    yaml.safe_load(config_path.read_text()),
                )
            )

        if self.config_settings is not None:
            result.update(self.config_settings)

        return result

    @contextmanager
    def with_temp_config_file(
        self,
        connector_root: Path,
    ) -> Generator[Path, None, None]:
        """Yield a temporary JSON file path containing the config dict and delete it on exit."""
        config = self.get_config_dict(
            empty_if_missing=True,
            connector_root=connector_root,
        )
        with tempfile.NamedTemporaryFile(
            prefix="config-",
            suffix=".json",
            mode="w",
            delete=False,  # Don't fail if cannot delete the file on exit
            encoding="utf-8",
        ) as temp_file:
            temp_file.write(json.dumps(config))
            temp_file.flush()
            # Allow the file to be read by other processes
            temp_path = Path(temp_file.name)
            temp_path.chmod(temp_path.stat().st_mode | 0o444)
            yield temp_path

        # attempt cleanup, ignore errors
        with suppress(OSError):
            temp_path.unlink()


class LegacyAcceptanceTestScenario(BaseModel):
    """Legacy acceptance test scenario, as a Pydantic model.

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

    class AcceptanceTestEmptyStream(BaseModel):
        name: str
        bypass_reason: str | None = None

        # bypass reason does not affect equality
        def __hash__(self) -> int:
            return hash(self.name)

    config_path: Path | None = None
    config_dict: dict[str, Any] | None = None

    _id: str | None = None  # Used to override the default ID generation

    configured_catalog_path: Path | None = None
    empty_streams: list[AcceptanceTestEmptyStream] | None = None
    timeout_seconds: int | None = None
    expect_records: AcceptanceTestExpectRecords | None = None
    file_types: AcceptanceTestFileTypes | None = None
    status: Literal["succeed", "failed", "exception"] | None = None

    @property
    def expected_outcome(self) -> ExpectedOutcome:
        """Whether the test scenario expects an exception to be raised.

        Returns True if the scenario expects an exception, False if it does not,
        and None if there is no set expectation.
        """
        return ExpectedOutcome.from_status_str(self.status)

    @property
    def id(self) -> str:
        """Return a unique identifier for the test scenario.

        This is used by PyTest to identify the test scenario.
        """
        if self._id:
            return self._id

        if self.config_path:
            return self.config_path.stem

        return str(hash(self))

    def __str__(self) -> str:
        return f"'{self.id}' Test Scenario"

    def as_test_scenario(self) -> ConnectorTestScenario:
        """Return a ConnectorTestScenario representation of this scenario.

        This is useful when we want to run the same scenario as both a legacy acceptance test
        and a smoke test.
        """
        if not self.config_path:
            raise ValueError("Cannot convert to ConnectorTestScenario without a config_path.")

        return ConnectorTestScenario(
            name=self.id,
            config_file=self.config_path,
            config_settings=self.config_dict,
            exclude_streams=[s.name for s in self.empty_streams or []],
            expect_failure=bool(self.status and self.status in _LEGACY_FAILURE_STATUSES),
            configured_catalog_path=self.configured_catalog_path,
        )
