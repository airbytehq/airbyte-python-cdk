# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for connector test suites."""

from __future__ import annotations

import importlib
import inspect
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
from boltons.typeutils import classproperty

from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.models import (
    ConnectorTestScenario,
)
from airbyte_cdk.test.standard_tests._assertions import assert_config_matches_spec
from airbyte_cdk.test.standard_tests._job_runner import IConnector, run_test_job
from airbyte_cdk.test.standard_tests._spec_lint import (
    assert_no_secrets_in_output,
    get_single_spec,
)
from airbyte_cdk.test.standard_tests.docker_base import (
    _SPEC_FAILED_SKIP_REASON,
    DockerConnectorTestSuite,
    skip_unless_runs,
)

if TYPE_CHECKING:
    import logging
    from collections.abc import Callable, Iterable, Mapping

    from airbyte_cdk.models import (
        AirbyteMessage,
        AirbyteStateMessage,
        ConfiguredAirbyteCatalog,
    )
    from airbyte_cdk.sources.source import Source
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
        """Instantiate the connector class.

        A connector class or factory that requires `catalog`, `config` or `state` (the
        constructor of a Python source built on `YamlDeclarativeSource`, `FileBasedSource` or
        `ConcurrentSourceAdapter` usually does) receives the scenario's config, no catalog and no
        state, as the connector's `run.py` passes them when it launches without a catalog or
        state. An optional `config` also receives the scenario's config, when the scenario has one.
        When the class or factory accepts any of these arguments, required or optional, its
        `read` is done by a source built with the catalog, config and state of the `read` command,
        as `run.py` builds it: a file-based source takes its stream schemas from the catalog it is
        built with, and a declarative source evaluates the config in its components when it is
        built.
        """
        connector = cls.connector  # type: ignore
        if connector:
            if callable(connector) or isinstance(connector, type):
                # If the connector is a class or factory function, instantiate it:
                init_kwargs = _source_init_kwargs(
                    connector,
                    scenario=scenario,
                    connector_root=cls.get_connector_root_dir(),
                )
                instance = connector(**init_kwargs)
                read_args = _source_read_args(connector)
                if read_args:
                    _read_with_source_built_for_the_command(instance, connector, read_args)
                return cast(IConnector, instance)  # type: ignore [redundant-cast]

        # Otherwise, we can't instantiate the connector. Fail with a clear error message.
        raise NotImplementedError(
            "No connector class or connector factory function provided. "
            "Please provide a class or factory function in `cls.connector`, or "
            "override `cls.create_connector()` to define a custom initialization process."
        )

    def _assert_no_secrets_in_output(
        self,
        scenario: ConnectorTestScenario,
        output: entrypoint_wrapper.EntrypointOutput,
        *,
        verb: str,
    ) -> None:
        """Assert that no `airbyte_secret` value from the scenario's config is in `output`.

        Pass this to `run_test_job` as `inspect_output`, so it runs before the job's own
        assertions print raw messages. It runs `spec` in-process once per call, to learn which
        config paths are secret. If `spec` fails, the test is skipped: the spec tests report
        that failure, and the outcome assertions must not print raw messages unchecked.
        """
        try:
            spec_result = run_test_job(
                self.create_connector(scenario),
                "spec",
                connector_root=self.get_connector_root_dir(),
            )
            spec = get_single_spec(spec_result, connector_name=self.connector_name)
        except Exception:
            pytest.skip(_SPEC_FAILED_SKIP_REASON)
        assert_no_secrets_in_output(
            output,
            spec=spec,
            config=scenario.get_config_dict(
                connector_root=self.get_connector_root_dir(),
                empty_if_missing=True,
            ),
            verb=verb,
            connector_name=self.connector_name,
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
            inspect_output=lambda output: self._assert_no_secrets_in_output(
                scenario, output, verb="check"
            ),
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


_SOURCE_INIT_ARGS = ("catalog", "config", "state")
"""Constructor arguments the test suite can supply to a connector class or factory."""


def _source_init_kwargs(
    factory: object,
    *,
    scenario: ConnectorTestScenario | None,
    connector_root: Path,
) -> dict[str, object]:
    """Return the keyword arguments to build the connector class or factory with.

    Only `catalog`, `config` and `state` can be supplied: `config` is the scenario's config (an
    empty dict when the scenario has none, or for `spec`), `catalog` and `state` are `None`. Any
    other required argument raises a `TypeError` naming it, since the suite cannot guess it.

    An optional `config` (accepted by keyword) also receives the scenario's config when the
    scenario has one, as `run.py` passes the `--config` it launches with: a declarative Python
    source evaluates its config in its components when it is built.
    """
    try:
        signature = inspect.signature(factory)  # type: ignore [arg-type]
    except (TypeError, ValueError):
        # Builtins and some C extensions have no signature; call them without arguments.
        return {}

    required = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.default is inspect.Parameter.empty
        and parameter.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]
    unsupported = [
        parameter.name
        for parameter in required
        if parameter.name not in _SOURCE_INIT_ARGS
        or parameter.kind is inspect.Parameter.POSITIONAL_ONLY
    ]
    if unsupported:
        factory_name = getattr(factory, "__name__", repr(factory))
        raise TypeError(
            f"The standard tests cannot instantiate `{factory_name}`: it requires the "
            f"argument(s) {', '.join(f'`{name}`' for name in unsupported)}, and the suite can "
            f"only supply {', '.join(f'`{name}`' for name in _SOURCE_INIT_ARGS)}. Give those "
            "arguments defaults, export a factory function under the connector class name, or "
            "override `create_connector()` in a test suite subclass."
        )

    kwargs: dict[str, object] = {}
    for parameter in required:
        if parameter.name == "config":
            kwargs["config"] = _scenario_config_or_empty(scenario, connector_root=connector_root)
        else:
            kwargs[parameter.name] = None

    optional_config = signature.parameters.get("config")
    if (
        "config" not in kwargs
        and optional_config is not None
        and optional_config.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    ):
        config = _scenario_config_or_empty(scenario, connector_root=connector_root)
        if config:
            kwargs["config"] = config
    return kwargs


def _scenario_config_or_empty(
    scenario: ConnectorTestScenario | None,
    *,
    connector_root: Path,
) -> dict[str, object]:
    """Return the scenario's config, or an empty dict when there is none or its file is missing.

    A missing config file is reported by the test that runs the command, not here.
    """
    if scenario is None:
        return {}
    try:
        return scenario.get_config_dict(connector_root=connector_root, empty_if_missing=True)
    except FileNotFoundError:
        return {}


def _source_read_args(factory: object) -> tuple[str, ...]:
    """Return the names among `catalog`, `config` and `state` that the factory accepts by keyword."""
    try:
        signature = inspect.signature(factory)  # type: ignore [arg-type]
    except (TypeError, ValueError):
        return ()
    return tuple(
        parameter.name
        for parameter in signature.parameters.values()
        if parameter.name in _SOURCE_INIT_ARGS
        and parameter.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    )


def _read_with_source_built_for_the_command(
    source: object,
    factory: Callable[..., object],
    read_args: tuple[str, ...],
) -> None:
    """Make `source.read` read with a source built from the `read` command's arguments.

    The suite builds the source before it knows the catalog and state of a `read`, while the
    connector's `run.py` builds it with them.
    """
    if not callable(getattr(source, "read", None)):
        return

    def read(
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterable[AirbyteMessage]:
        command_args: dict[str, object] = {"catalog": catalog, "config": config, "state": state}
        launched = cast("Source", factory(**{name: command_args[name] for name in read_args}))
        return launched.read(logger, config, catalog, state)

    setattr(source, "read", read)
