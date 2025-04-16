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
from airbyte_cdk.test.standard_tests._job_runner import run_test_job
from airbyte_cdk.test.standard_tests.connector_base import (
    ConnectorTestSuiteBase,
)
from airbyte_cdk.test.standard_tests.models import (
    ConnectorTestScenario,
)


class SourceTestSuiteBase(ConnectorTestSuiteBase):
    """Base class for source test suites.

    This class provides a base set of functionality for testing source connectors, and it
    inherits all generic connector tests from the `ConnectorTestSuiteBase` class.
    """

    def test_check(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Run standard `check` tests on the connector.

        Assert that the connector returns a single CONNECTION_STATUS message.
        This test is designed to validate the connector's ability to establish a connection
        and return its status with the expected message type.
        """
        result: entrypoint_wrapper.EntrypointOutput = run_test_job(
            self.create_connector(scenario),
            "check",
            test_scenario=scenario,
        )
        conn_status_messages: list[AirbyteMessage] = [
            msg for msg in result._messages if msg.type == Type.CONNECTION_STATUS
        ]  # noqa: SLF001  # Non-public API
        num_status_messages = len(conn_status_messages)
        assert num_status_messages == 1, (
            f"Expected exactly one CONNECTION_STATUS message. Got {num_status_messages}: \n"
            + "\n".join([str(m) for m in result._messages])
        )

    def test_discover(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Standard test for `discover`."""
        run_test_job(
            self.create_connector(scenario),
            "discover",
            test_scenario=scenario,
        )

    def test_basic_read(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Run standard `read` test on the connector.

        This test is designed to validate the connector's ability to read data
        from the source and return records. It first runs a `discover` job to
        obtain the catalog of streams, and then it runs a `read` job to fetch
        records from those streams.
        """
        discover_result = run_test_job(
            self.create_connector(scenario),
            "discover",
            test_scenario=scenario,
        )
        if scenario.expect_exception:
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
            self.create_connector(scenario),
            "read",
            test_scenario=scenario,
            catalog=configured_catalog,
        )

        if not result.records:
            raise AssertionError("Expected records but got none.")  # noqa: TRY003

    def test_fail_read_with_bad_catalog(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Standard test for `read` when passed a bad catalog file."""
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
        scenario.status = "failed"
        result: entrypoint_wrapper.EntrypointOutput = run_test_job(
            self.create_connector(scenario),
            "read",
            test_scenario=scenario,
            catalog=asdict(invalid_configured_catalog),
        )
        assert result.errors, "Expected errors but got none."
        assert result.trace_messages, "Expected trace messages but got none."
