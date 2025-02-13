# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for source test suites."""

from dataclasses import asdict
from pathlib import Path

import pytest
from airbyte_connector_tester.connector_tests import ConnectorTestSuiteBase
from airbyte_connector_tester.instances import (
    AcceptanceTestScenario,
    get_acceptance_tests,
)
from airbyte_connector_tester.job_runner import run_test_job

from airbyte_cdk.models import (
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)


class SourceTestSuiteBase(ConnectorTestSuiteBase):
    """Base class for source test suites.

    This class provides a base set of functionality for testing source connectors, and it
    inherits all generic connector tests from the `ConnectorTestSuiteBase` class.
    """

    @pytest.mark.parametrize(
        "instance",
        get_acceptance_tests("full_refresh"),
        ids=lambda instance: instance.instance_name,
    )
    def test_full_refresh(
        self,
        instance: AcceptanceTestScenario,
    ) -> None:
        """Run acceptance tests."""
        result = run_test_job(
            self.new_connector(),
            "read",
            test_instance=instance,
        )
        if not result.records:
            raise AssertionError("Expected records but got none.")  # noqa: TRY003

    @pytest.mark.parametrize(
        "instance",
        get_acceptance_tests("basic_read"),
        ids=lambda instance: instance.instance_name,
    )
    def test_basic_read(
        self,
        instance: AcceptanceTestScenario,
    ) -> None:
        """Run acceptance tests."""
        discover_result = run_test_job(
            self.new_connector(),
            "discover",
            test_instance=instance,
        )
        assert discover_result.catalog, "Expected a non-empty catalog."
        configured_catalog = ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=stream,
                    sync_mode=SyncMode.full_refresh,
                    destination_sync_mode=DestinationSyncMode.append_dedup,
                )
                for stream in discover_result.catalog.catalog.streams
            ]
        )
        result = run_test_job(
            self.new_connector(),
            "read",
            test_instance=instance,
            catalog=configured_catalog,
        )

        if not result.records:
            raise AssertionError("Expected records but got none.")  # noqa: TRY003

    @pytest.mark.parametrize(
        "instance",
        get_acceptance_tests("basic_read"),
        ids=lambda instance: instance.instance_name,
    )
    def test_fail_with_bad_catalog(
        self,
        instance: AcceptanceTestScenario,
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
                    sync_mode="INVALID",
                    destination_sync_mode="INVALID",
                )
            ]
        )
        # Set expected status to "failed" to ensure the test fails if the connector.
        instance.status = "failed"
        result = run_test_job(
            self.new_connector(),
            "read",
            test_instance=instance,
            catalog=asdict(invalid_configured_catalog),
        )
        assert result.errors, "Expected errors but got none."
        assert result.trace_messages, "Expected trace messages but got none."

    @pytest.mark.parametrize(
        "instance",
        get_acceptance_tests("full_refresh"),
        ids=lambda instance: instance.instance_name,
    )
    def test_discover(
        self,
        instance: AcceptanceTestScenario,
    ) -> None:
        """Run acceptance tests."""
        run_test_job(
            self.new_connector(),
            "check",
            test_instance=instance,
        )
