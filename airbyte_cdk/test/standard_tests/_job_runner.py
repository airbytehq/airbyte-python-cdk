# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Job runner for Airbyte Standard Tests."""

import logging
import tempfile
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Literal

import orjson
from typing_extensions import Protocol, runtime_checkable

from airbyte_cdk.models import (
    ConfiguredAirbyteCatalog,
    Status,
)
from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.standard_tests.models import (
    ConnectorTestScenario,
)


def _errors_to_str(
    entrypoint_output: entrypoint_wrapper.EntrypointOutput,
) -> str:
    """Convert errors from entrypoint output to a string."""
    if not entrypoint_output.errors:
        # If there are no errors, return an empty string.
        return ""

    return "\n" + "\n".join(
        [
            str(error.trace.error).replace(
                "\\n",
                "\n",
            )
            for error in entrypoint_output.errors
            if error.trace
        ],
    )


@runtime_checkable
class IConnector(Protocol):
    """A connector that can be run in a test scenario.

    Note: We currently use 'spec' to determine if we have a connector object.
    In the future, it would be preferred to leverage a 'launch' method instead,
    directly on the connector (which doesn't yet exist).
    """

    def spec(self, logger: logging.Logger) -> Any:
        """Connectors should have a `spec` method."""


def run_test_job(
    connector: IConnector | type[IConnector] | Callable[[], IConnector],
    verb: Literal["read", "check", "discover"],
    test_scenario: ConnectorTestScenario,
    *,
    catalog: ConfiguredAirbyteCatalog | dict[str, Any] | None = None,
) -> entrypoint_wrapper.EntrypointOutput:
    """Run a test scenario from provided CLI args and return the result."""
    if not connector:
        raise ValueError("Connector is required")

    if catalog and isinstance(catalog, ConfiguredAirbyteCatalog):
        # Convert the catalog to a dict if it's already a ConfiguredAirbyteCatalog.
        catalog = asdict(catalog)

    connector_obj: IConnector
    if isinstance(connector, type) or callable(connector):
        # If the connector is a class or a factory lambda, instantiate it.
        connector_obj = connector()
    elif isinstance(connector, IConnector):
        connector_obj = connector
    else:
        raise ValueError(
            f"Invalid connector input: {type(connector)}",
        )

    args: list[str] = [verb]
    if test_scenario.config_path:
        args += ["--config", str(test_scenario.config_path)]
    elif test_scenario.config_dict:
        config_path = (
            Path(tempfile.gettempdir()) / "airbyte-test" / f"temp_config_{uuid.uuid4().hex}.json"
        )
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(orjson.dumps(test_scenario.config_dict).decode())
        args += ["--config", str(config_path)]

    catalog_path: Path | None = None
    if verb not in ["discover", "check"]:
        # We need a catalog for read.
        if catalog:
            # Write the catalog to a temp json file and pass the path to the file as an argument.
            catalog_path = (
                Path(tempfile.gettempdir())
                / "airbyte-test"
                / f"temp_catalog_{uuid.uuid4().hex}.json"
            )
            catalog_path.parent.mkdir(parents=True, exist_ok=True)
            catalog_path.write_text(orjson.dumps(catalog).decode())
        elif test_scenario.configured_catalog_path:
            catalog_path = Path(test_scenario.configured_catalog_path)

        if catalog_path:
            args += ["--catalog", str(catalog_path)]

    # This is a bit of a hack because the source needs the catalog early.
    # Because it *also* can fail, we have to redundantly wrap it in a try/except block.

    result: entrypoint_wrapper.EntrypointOutput = entrypoint_wrapper._run_command(  # noqa: SLF001  # Non-public API
        source=connector_obj,  # type: ignore [arg-type]
        args=args,
        expecting_exception=test_scenario.expect_exception,
    )
    if result.errors and not test_scenario.expect_exception:
        raise AssertionError(
            f"Expected no errors but got {len(result.errors)}: \n" + _errors_to_str(result)
        )

    if verb == "check":
        # Check is expected to fail gracefully without an exception.
        # Instead, we assert that we have a CONNECTION_STATUS message with
        # a failure status.
        assert len(result.connection_status_messages) == 1, (
            "Expected exactly one CONNECTION_STATUS message. Got "
            f"{len(result.connection_status_messages)}:\n"
            + "\n".join([str(msg) for msg in result.connection_status_messages])
            + _errors_to_str(result)
        )
        if test_scenario.expect_exception:
            conn_status = result.connection_status_messages[0].connectionStatus
            assert conn_status, (
                "Expected CONNECTION_STATUS message to be present. Got: \n"
                + "\n".join([str(msg) for msg in result.connection_status_messages])
            )
            assert conn_status.status == Status.FAILED, (
                "Expected CONNECTION_STATUS message to be FAILED. Got: \n"
                + "\n".join([str(msg) for msg in result.connection_status_messages])
            )

        return result

    # For all other verbs, we assert check that an exception is raised (or not).
    if test_scenario.expect_exception:
        if not result.errors:
            raise AssertionError("Expected exception but got none.")

        return result

    assert not result.errors, (
        f"Expected no errors but got {len(result.errors)}: \n" + _errors_to_str(result)
    )

    return result
