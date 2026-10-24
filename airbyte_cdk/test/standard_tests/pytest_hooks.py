# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Pytest hooks for Airbyte CDK tests.

These hooks are used to customize the behavior of pytest during test discovery and execution.

To use these hooks within a connector, add the following lines to the connector's `conftest.py`
file, or to another file that is imported during test discovery:

```python
pytest_plugins = [
    "airbyte_cdk.test.standard_tests.pytest_hooks",
]
```
"""

from typing import Literal, cast

import pytest

from airbyte_cdk.test.models.scenario import ConnectorTestScenario


@pytest.fixture
def connector_image_override(request: pytest.FixtureRequest) -> str | None:
    """Return the value of --connector-image, or None if not set."""
    return cast(str | None, request.config.getoption("--connector-image"))


@pytest.fixture
def connector_base_image_override(request: pytest.FixtureRequest) -> str | None:
    """Return the value of --connector-base-image, or None if not set.

    When set, the connector image is built `FROM` this base image instead of the
    `baseImage` declared in metadata.yaml. This lets the image tests exercise a
    locally-built `source-declarative-manifest` image (e.g. one built from the current
    CDK branch) rather than the published base image's CDK.
    """
    return cast(str | None, request.config.getoption("--connector-base-image"))


READ_FROM_STREAMS_DEFAULT = "default"
"""The `--read-from-streams` value used when the option is omitted: the Docker read test runs."""

READ_SCENARIOS_DEFAULT = "default"
"""The `--read-scenarios` value used when the option is omitted."""


@pytest.fixture
def read_from_streams(
    request: pytest.FixtureRequest,
) -> Literal["all", "none", "default"] | list[str]:
    """Return the value of `--read-from-streams`: which streams the Docker read test reads.

    The Docker read test (`test_docker_image_build_and_read`) runs a full-refresh `read` inside the
    connector image for each config listed under `basic_read` or `full_refresh` in
    `acceptance-test-config.yml`, of the streams of its `configured_catalog_path` (or every
    discovered stream) minus its `empty_streams`. This option narrows those streams further:

    - [Omitted], `--read-from-streams=default`, `=true` or `=suggested`: Read the connector's
      `suggestedStreams` from `metadata.yaml`. A connector that declares none, or whose suggested
      streams are all excluded for the config, reads all of the config's streams.
    - `--read-from-streams=all`: Read all of the config's streams.
    - `--read-from-streams=stream1,stream2`: Read only these of the config's streams. A config
      that reads none of them is skipped.
    - `--read-from-streams=false` or `=none`: Skip the Docker read test (the explicit opt-out).
      Prefer `false` in CI files: CDK 7.36.0 and earlier read `none` as a stream name.

    Which configs are read is set by `--read-scenarios`. The two options combine: the test reads
    the streams selected here for each config selected there, so the default of both reads at
    least one config of every connector that lists a config under a read section.
    """
    input_val = request.config.getoption(
        "--read-from-streams",
        default=READ_FROM_STREAMS_DEFAULT,
    )
    if input_val is None:
        input_val = READ_FROM_STREAMS_DEFAULT
    if not isinstance(input_val, str):
        # Only a bool can get here, from a caller that sets the option programmatically.
        return "default" if input_val else "none"

    normalized = input_val.strip().lower()
    if normalized in ("false", "none"):
        return "none"
    if normalized in ("true", "suggested", "default"):
        return "default"
    if normalized == "all":
        # This will sometimes fail if the account doesn't have permissions
        # to premium or restricted stream data.
        return "all"

    # A comma-separated list of stream names; a single name gives a one-element list.
    return [name.strip() for name in input_val.split(",") if name.strip()]


@pytest.fixture
def read_scenarios(
    request: pytest.FixtureRequest,
) -> list[str] | Literal["all", "none", "default"]:
    """Return the value of `--read-scenarios`: which configs the Docker read test reads.

    Only configs listed under `basic_read` or `full_refresh` in `acceptance-test-config.yml` are
    ever read. Of those:

    - [Omitted] or `--read-scenarios=default`: Read the configs whose file name (without `.json`)
      is `config`, `valid_config` or `default`. A connector with none of those reads its first
      read config instead, so the default never skips every Docker read of a connector.
    - `--read-scenarios=all`: Read every read config.
    - `--read-scenarios=scenario1,scenario2`: Read only these configs (by file name, with or
      without `.json`); the others are skipped.
    - `--read-scenarios=none`: Skip the Docker read test, like `--read-from-streams=false`.

    `--read-from-streams` then picks the streams read for each selected config. The in-process
    read test (`test_basic_read`) ignores both options and reads every read config.
    """
    input_val = request.config.getoption(
        "--read-scenarios",
        default=READ_SCENARIOS_DEFAULT,
    )
    if not input_val:
        input_val = READ_SCENARIOS_DEFAULT
    input_val = cast(str, input_val)

    normalized = input_val.strip().lower()
    if normalized in ("default", "all", "none"):
        return cast(Literal["all", "none", "default"], normalized)

    return [
        scenario_name.strip().lower().removesuffix(".json")
        for scenario_name in input_val.split(",")
        if scenario_name.strip()
    ]


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add the standard tests' options to pytest's CLI."""
    parser.addoption(
        "--connector-image",
        action="store",
        default=None,
        help="Use this pre-built connector Docker image instead of building one.",
    )
    parser.addoption(
        "--connector-base-image",
        action="store",
        default=None,
        help=(
            "Build the connector image `FROM` this base image instead of the `baseImage` "
            "declared in metadata.yaml. Useful for testing against a locally-built "
            "`source-declarative-manifest` image built from the current CDK branch. "
            "The image must be visible to your default buildx builder; locally-built images "
            "are only resolved by the default `docker` driver, not `docker-container` builders."
        ),
    )
    parser.addoption(
        "--read-from-streams",
        action="store",
        default=READ_FROM_STREAMS_DEFAULT,
        help=read_from_streams.__doc__,
    )
    parser.addoption(
        "--read-scenarios",
        action="store",
        default=READ_SCENARIOS_DEFAULT,
        help=read_scenarios.__doc__,
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """A helper for pytest_generate_tests hook.

    If a test method (in a class subclassed from our base class)
    declares an argument 'scenario', this function retrieves the
    'scenarios' attribute from the test class and parametrizes that
    test with the values from 'scenarios'.

    ## Usage

    ```python
    from airbyte_cdk.test.standard_tests.connector_base import (
        generate_tests,
        ConnectorTestSuiteBase,
    )

    def pytest_generate_tests(metafunc):
        generate_tests(metafunc)

    class TestMyConnector(ConnectorTestSuiteBase):
        ...

    ```
    """
    # Check if the test function requires an 'scenario' argument
    if "scenario" in metafunc.fixturenames:
        # Retrieve the test class
        test_class = metafunc.cls
        if test_class is None:
            return

        # Check that the class is compatible with our test suite
        scenarios_attr = getattr(test_class, "get_scenarios", None)
        if scenarios_attr is None:
            raise ValueError(
                f"Test class {test_class} does not have a 'scenarios' attribute. "
                "Please define the 'scenarios' attribute in the test class."
            )

        # Get the scenarios defined or discovered in the test class
        scenarios = test_class.get_scenarios()

        # Create pytest.param objects with special marks as needed
        parametrized_scenarios = [
            pytest.param(
                scenario,
                marks=[pytest.mark.requires_creds] if scenario.requires_creds else [],
            )
            for scenario in scenarios
        ]

        # Parametrize the 'scenario' argument with the scenarios
        metafunc.parametrize(
            "scenario",
            parametrized_scenarios,
            ids=_scenario_test_ids(scenarios),
        )


def _scenario_test_ids(scenarios: list[ConnectorTestScenario]) -> list[str]:
    """Return unique, human-readable pytest IDs for the given scenarios.

    Scenario IDs are derived from the config file stem, so configs in different directories
    (e.g. `integration_tests/config.json` and `secrets/config.json`) collide. Where they do,
    we qualify the ID with the config's parent directory so failures name the config that
    failed. Note this only affects the displayed test ID, not `ConnectorTestScenario.id`,
    which scenario selection (`--read-scenarios`) matches against.
    """
    labels = [str(scenario) for scenario in scenarios]
    duplicated = {label for label in labels if labels.count(label) > 1}
    return [
        f"{scenario.config_path.parent.name}/{label}"
        if label in duplicated and scenario.config_path is not None
        else label
        for scenario, label in zip(scenarios, labels)
    ]
