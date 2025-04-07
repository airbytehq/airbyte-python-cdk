import tempfile
import uuid
from pathlib import Path
from typing import Callable, Literal

import orjson

from airbyte_cdk import Connector
from airbyte_cdk.models import (
    Status,
)
from airbyte_cdk.sources.abstract_source import AbstractSource
from airbyte_cdk.sources.declarative.declarative_source import DeclarativeSource
from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.declarative.models import (
    ConnectorTestScenario,
)


def run_test_job(
    connector: Connector | type[Connector] | Callable[[], Connector],
    verb: Literal["read", "check", "discover"],
    test_instance: ConnectorTestScenario,
    *,
    catalog: dict | None = None,
) -> entrypoint_wrapper.EntrypointOutput:
    """Run a test job from provided CLI args and return the result."""
    if not connector:
        raise ValueError("Connector is required")

    connector_obj: Connector
    if isinstance(connector, type):
        connector_obj = connector()
    elif isinstance(connector, Connector):
        connector_obj = connector
    elif isinstance(connector, DeclarativeSource | AbstractSource):
        connector_obj = connector
    elif isinstance(connector, Callable):
        try:
            connector_obj = connector()
        except Exception as ex:
            if not test_instance.expect_exception:
                raise

            return entrypoint_wrapper.EntrypointOutput(
                messages=[],
                uncaught_exception=ex,
            )
    else:
        raise ValueError(f"Invalid source type: {type(connector)}")

    args = [verb]
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
        source=connector_obj,
        args=args,
        expecting_exception=test_instance.expect_exception,
    )
    if result.errors and not test_instance.expect_exception:
        raise AssertionError(
            "\n\n".join(
                [str(err.trace.error).replace("\\n", "\n") for err in result.errors],
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
            + "\n".join(result.connection_status_messages)
        )
        if test_instance.expect_exception:
            assert result.connection_status_messages[0].connectionStatus.status == Status.FAILED, (
                "Expected CONNECTION_STATUS message to be FAILED. Got: \n"
                + "\n".join([str(result.connection_status_messages)])
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
                [str(err.trace.error).replace("\\n", "\n") for err in result.errors],
            )
        )

    return result
