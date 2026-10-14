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

from dataclasses import dataclass
from typing import Literal, cast

import pytest

from airbyte_cdk.test.models.scenario import ConnectorTestScenario
from airbyte_cdk.test.standard_tests._acceptance_test_config import (
    AcceptanceTestConfigError,
    ScenarioBypass,
)
from airbyte_cdk.utils.connector_paths import ACCEPTANCE_TEST_CONFIG


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


@pytest.fixture
def read_from_streams(
    request: pytest.FixtureRequest,
) -> Literal["all", "none", "default"] | list[str]:
    """Specify if the test should read from streams.

    The input can be one of the following:
    - [Omitted] - Default to False, meaning no streams will be read.
    - `--read-from-streams`: Read from all suggested streams.
    - `--read-from-streams=true`: Read from all suggested streams.
    - `--read-from-streams=suggested`: Read from all suggested streams.
    - `--read-from-streams=default`: Read from all suggested streams.
    - `--read-from-streams=all`: Read from all streams.
    - `--read-from-streams=stream1,stream2`: Read from the specified streams only.
    - `--read-from-streams=false`: Do not read from any streams.
    - `--read-from-streams=none`: Do not read from any streams.
    """
    input_val: str = request.config.getoption(
        "--read-from-streams",
        default="default",  # type: ignore
    )  # type: ignore

    if isinstance(input_val, str):
        if input_val.lower() == "false":
            return "none"
        if input_val.lower() in ["true", "suggested", "default"]:
            # Default to 'default' (suggested) streams if the input is 'true', 'suggested', or
            # 'default'.
            # This is the default behavior if the option is not set.
            return "default"
        if input_val.lower() == "all":
            # This will sometimes fail if the account doesn't have permissions
            # to premium or restricted stream data.
            return "all"

        # If the input is a comma-separated list, split it into a list.
        # This will return a one-element list if the input is a single stream name.
        return input_val.split(",")

    # Else, probably a bool; return it as is.
    return input_val or "none"


@pytest.fixture
def read_scenarios(
    request: pytest.FixtureRequest,
) -> list[str] | Literal["all", "default"]:
    """Return the value of `--read-scenarios`.

    This argument is ignored if `--read-from-streams` is False or not set.

    The input can be one of the following:
    - [Omitted] - Default to 'config.json', meaning the default scenario will be read.
    - `--read-scenarios=all`: Read all scenarios.
    - `--read-scenarios=none`: Read no scenarios. (Overrides `--read-from-streams`, if set.)
    - `--read-scenarios=scenario1,scenario2`: Read the specified scenarios only.

    """
    input_val = cast(
        str,
        request.config.getoption(
            "--read-scenarios",
            default="default",  # type: ignore
        ),
    )

    if input_val.lower() == "default":
        # Default config scenario is always 'config.json'.
        return "default"

    if input_val.lower() == "none":
        # Default config scenario is always 'config.json'.
        return []

    return (
        [
            scenario_name.strip().lower().removesuffix(".json")
            for scenario_name in input_val.split(",")
        ]
        if input_val
        else []
    )


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add --connector-image to pytest's CLI."""
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
        default=None,
        help=read_from_streams.__doc__,
    )
    parser.addoption(
        "--read-scenarios",
        action="store",
        default="default",
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

        # Get the scenarios defined or discovered in the test class, and the declared bypasses.
        config_error: str | None = None
        try:
            scenarios = test_class.get_scenarios()
            get_bypasses = getattr(test_class, "get_scenario_bypasses", None)
            bypasses: list[ScenarioBypass] = get_bypasses() if get_bypasses else []
        except AcceptanceTestConfigError as error:
            config_error = str(error)
        if config_error is not None:
            # Failed outside the `except` block, so pytest does not print the error twice.
            pytest.fail(config_error, pytrace=False)

        if not scenarios and not bypasses:
            # Parametrizing with an empty list would skip every scenario test and exit 0.
            describe = getattr(test_class, "describe_empty_scenarios", None)
            pytest.fail(
                describe()
                if describe
                else f"Test class {test_class.__name__} has no test scenarios.",
                pytrace=False,
            )

        bypass_groups = _group_bypasses(bypasses)
        if bypass_groups:
            connector_name = getattr(test_class, "connector_name", test_class.__name__)
            metafunc.config.stash.setdefault(_BYPASS_REPORT_KEY, {})[str(connector_name)] = (
                f"{connector_name}: {ACCEPTANCE_TEST_CONFIG} declares {len(bypasses)} bypass(es), "
                "reported as skipped tests: "
                + "; ".join(f"{group.scope}: {group.reason}" for group in bypass_groups)
            )

        # Create pytest.param objects with special marks as needed
        parametrized_scenarios = [
            pytest.param(
                scenario,
                marks=[pytest.mark.requires_creds] if scenario.requires_creds else [],
            )
            for scenario in scenarios
        ]
        # One skipped param per distinct bypass reason, so the CI summary shows the reason.
        bypass_params = [
            pytest.param(
                ConnectorTestScenario(bypass_reason=group.reason, sections=group.sections),
                marks=pytest.mark.skip(
                    reason=f"{ACCEPTANCE_TEST_CONFIG} bypasses {group.scope}: {group.reason}"
                ),
            )
            for group in bypass_groups
        ]

        # Parametrize the 'scenario' argument with the scenarios, then the bypasses
        metafunc.parametrize(
            "scenario",
            parametrized_scenarios + bypass_params,
            ids=_scenario_test_ids(scenarios) + [group.test_id for group in bypass_groups],
        )


_BYPASS_REPORT_KEY = pytest.StashKey[dict[str, str]]()
"""The collection report line of each connector whose config declares bypasses."""


def pytest_report_collectionfinish(config: pytest.Config) -> list[str]:
    """Print one line per connector that declares bypasses, after the collection summary."""
    return [line for _, line in sorted(config.stash.get(_BYPASS_REPORT_KEY, {}).items())]


@dataclass(frozen=True)
class _BypassGroup:
    """The bypasses that share one reason, reported as one skipped test per scenario test."""

    reason: str
    scope: str
    """What is bypassed: `the connector`, or the sections and entries, e.g. `connection.tests[1]`."""
    sections: tuple[str, ...]
    """The bypassed sections; empty when the whole connector is bypassed."""
    test_id: str


def _group_bypasses(bypasses: list[ScenarioBypass]) -> list[_BypassGroup]:
    """Group bypasses by reason: the usual config repeats one reason on every section."""
    by_reason: dict[str, list[ScenarioBypass]] = {}
    for bypass in bypasses:
        by_reason.setdefault(bypass.reason.strip(), []).append(bypass)

    groups: list[_BypassGroup] = []
    for reason, group in by_reason.items():
        connector_wide = any(not bypass.sections for bypass in group)
        sections = (
            ()
            if connector_wide
            else tuple(dict.fromkeys(section for bypass in group for section in bypass.sections))
        )
        scope = (
            "the connector"
            if connector_wide
            else ", ".join(
                bypass.location.removeprefix("acceptance_tests.").removesuffix(".bypass_reason")
                for bypass in group
            )
        )
        test_id = "bypassed-" + ("connector" if connector_wide else "-".join(sections))
        if any(existing.test_id == test_id for existing in groups):
            test_id = f"{test_id}-{len(groups) + 1}"
        groups.append(_BypassGroup(reason=reason, scope=scope, sections=sections, test_id=test_id))
    return groups


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
