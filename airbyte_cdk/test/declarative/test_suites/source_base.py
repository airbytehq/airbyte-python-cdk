# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for source test suites."""

from dataclasses import asdict

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)
from airbyte_cdk.test import entrypoint_wrapper
from airbyte_cdk.test.declarative.models import (
    ConnectorTestScenario,
)
from airbyte_cdk.test.declarative.test_suites.connector_base import (
    ConnectorTestSuiteBase,
)
from airbyte_cdk.test.declarative.utils.job_runner import run_test_job


class SourceTestSuiteBase(ConnectorTestSuiteBase):
    """Base class for source test suites.

    This class provides a base set of functionality for testing source connectors, and it
    inherits all generic connector tests from the `ConnectorTestSuiteBase` class.
    """

    def test_check(
        self,
        instance: ConnectorTestScenario,
    ) -> None:
        """Run `connection` acceptance tests."""
        result: entrypoint_wrapper.EntrypointOutput = run_test_job(
            self.create_connector(instance),
            "check",
            test_instance=instance,
        )
        conn_status_messages: list[AirbyteMessage] = [
            msg for msg in result._messages if msg.type == Type.CONNECTION_STATUS
        ]  # noqa: SLF001  # Non-public API
        num_status_messages = len(conn_status_messages)
        assert num_status_messages == 1, (
            f"Expected exactly one CONNECTION_STATUS message. Got {num_status_messages}: \n"
            + "\n".join([str(m) for m in result._messages])
        )

    def test_basic_read(
        self,
        instance: ConnectorTestScenario,
    ) -> None:
        """Run acceptance tests."""
        discover_result = run_test_job(
            self.create_connector(instance),
            "discover",
            test_instance=instance,
        )
        if instance.expect_exception:
            assert discover_result.errors, "Expected exception but got none."
            return

        configured_catalog = ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=stream,
                    sync_mode=SyncMode.full_refresh,
                    destination_sync_mode=DestinationSyncMode.append_dedup,
                )
                for stream in discover_result.catalog.catalog.streams  # type: ignore [reportOptionalMemberAccess, union-attr]
            ]
        )
        result = run_test_job(
            self.create_connector(instance),
            "read",
            test_instance=instance,
            catalog=configured_catalog,
        )

        if not result.records:
            raise AssertionError("Expected records but got none.")  # noqa: TRY003

    def test_fail_with_bad_catalog(
        self,
        instance: ConnectorTestScenario,
    ) -> None:
        """Test that a bad catalog fails."""
        invalid_configured_catalog = ConfiguredAirbyteCatalog(
            streams=[
                # Create ConfiguredAirbyteStream which is deliberately invalid
                # with regard to the Airbyte Protocol.
                # This should cause the connector to fail.
                ConfiguredAirbyteStream(
                    stream=AirbyteStream(
                        name="__AIRBYTE__stream_that_does_not_exist",
                        json_schema={
                            "type": "object",
                            "properties": {"f1": {"type": "string"}},
                        },
                        supported_sync_modes=[SyncMode.full_refresh],
                    ),
                    sync_mode="INVALID",  # type: ignore [reportArgumentType]
                    destination_sync_mode="INVALID",  # type: ignore [reportArgumentType]
                )
            ]
        )
        # Set expected status to "failed" to ensure the test fails if the connector.
        instance.status = "failed"
        result = self.run_test_scenario(
            "read",
            test_scenario=instance,
            catalog=asdict(invalid_configured_catalog),
        )
        assert result.errors, "Expected errors but got none."
        assert result.trace_messages, "Expected trace messages but got none."

    def test_discover(
        self,
        instance: ConnectorTestScenario,
    ) -> None:
        """Run acceptance tests."""
        run_test_job(
            self.create_connector(instance),
            "check",
            test_instance=instance,
        )
