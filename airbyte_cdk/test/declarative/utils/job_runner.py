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
from airbyte_cdk.test.declarative.models import (
    ConnectorTestScenario,
)


@runtime_checkable
class IConnector(Protocol):
    """A connector that can be run in a test scenario."""

    def launch(self, args: list[str] | None) -> None:
        """Launch the connector with the given arguments."""
        ...


def run_test_job(
    connector: IConnector | type[IConnector] | Callable[[], IConnector],
    verb: Literal["read", "check", "discover"],
    test_instance: ConnectorTestScenario,
    *,
    catalog: ConfiguredAirbyteCatalog | dict[str, Any] | None = None,
) -> entrypoint_wrapper.EntrypointOutput:
    """Run a test job from provided CLI args and return the result."""
    if not connector:
        raise ValueError("Connector is required")

    if catalog and isinstance(catalog, ConfiguredAirbyteCatalog):
        # Convert the catalog to a dict if it's already a ConfiguredAirbyteCatalog.
        catalog = asdict(catalog)

    connector_obj: IConnector
    if isinstance(connector, type) or callable(connector):
        # If the connector is a class or a factory lambda, instantiate it.
        connector_obj = connector()
    elif (
        isinstance(
            connector,
            IConnector,
        )
        or True
    ):  # TODO: Get a valid protocol check here
        connector_obj = connector
    else:
        raise ValueError(
            f"Invalid connector input: {type(connector)}",
        )

    args: list[str] = [verb]
    if test_instance.config_path:
        args += ["--config", str(test_instance.config_path)]
    elif test_instance.config_dict:
        config_path = (
            Path(tempfile.gettempdir()) / "airbyte-test" / f"temp_config_{uuid.uuid4().hex}.json"
        )
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(orjson.dumps(test_instance.config_dict).decode())
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
        elif test_instance.configured_catalog_path:
            catalog_path = Path(test_instance.configured_catalog_path)

        if catalog_path:
            args += ["--catalog", str(catalog_path)]

    # This is a bit of a hack because the source needs the catalog early.
    # Because it *also* can fail, we have ot redundantly wrap it in a try/except block.

    result: entrypoint_wrapper.EntrypointOutput = entrypoint_wrapper._run_command(  # noqa: SLF001  # Non-public API
        source=connector_obj,  # type: ignore [arg-type]
        args=args,
        expecting_exception=test_instance.expect_exception,
    )
    if result.errors and not test_instance.expect_exception:
        raise AssertionError(
            "\n\n".join(
                [str(err.trace.error).replace("\\n", "\n") for err in result.errors if err.trace],
            )
        )

    if verb == "check":
        # Check is expected to fail gracefully without an exception.
        # Instead, we assert that we have a CONNECTION_STATUS message with
        # a failure status.
        assert not result.errors, "Expected no errors from check. Got:\n" + "\n".join(
            [str(error) for error in result.errors]
        )
        assert len(result.connection_status_messages) == 1, (
            "Expected exactly one CONNECTION_STATUS message. Got "
            f"{len(result.connection_status_messages)}:\n"
            + "\n".join([str(msg) for msg in result.connection_status_messages])
        )
        if test_instance.expect_exception:
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
    if test_instance.expect_exception:
        if not result.errors:
            raise AssertionError("Expected exception but got none.")

        return result
    if result.errors:
        raise AssertionError(
            "\n\n".join(
                [
                    str(err.trace.error).replace(
                        "\\n",
                        "\n",
                    )
                    for err in result.errors
                    if err.trace
                ],
            )
        )

    return result
