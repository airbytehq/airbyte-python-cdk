# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for source test suites."""

from dataclasses import asdict
from typing import TYPE_CHECKING

import pytest

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)
from airbyte_cdk.test.models import (
    ConnectorTestScenario,
)
from airbyte_cdk.test.standard_tests._job_runner import run_test_job
from airbyte_cdk.test.standard_tests._read_assertions import (
    assert_read_records,
    assert_schema_validation_opt_out_allowed,
)
from airbyte_cdk.test.standard_tests.connector_base import (
    ConnectorTestSuiteBase,
)
from airbyte_cdk.test.standard_tests.docker_base import (
    get_discovered_streams,
    skip_unless_runs,
)

if TYPE_CHECKING:
    from airbyte_cdk.test import entrypoint_wrapper


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

        Runs for configs listed under `connection` in `acceptance-test-config.yml`. Assert that
        the connector returns a single CONNECTION_STATUS message whose status matches the
        scenario's expectation. This test is designed to validate the connector's ability to
        establish a connection and return its status with the expected message type.

        Scenarios declared with `status: exception` expect `check` to raise instead of reporting
        a status; for those, only the presence of a trace error is asserted (in `run_test_job`).
        """
        skip_unless_runs(scenario, "check")
        result: entrypoint_wrapper.EntrypointOutput = run_test_job(
            self.create_connector(scenario),
            "check",
            test_scenario=scenario,
            connector_root=self.get_connector_root_dir(),
        )
        if scenario.expected_outcome.expect_uncaught_error():
            # An uncaught error is the expected outcome; no CONNECTION_STATUS is required.
            return

        num_status_messages = len(result.connection_status_messages)
        assert num_status_messages == 1, (
            f"Expected exactly one CONNECTION_STATUS message. Got {num_status_messages}: \n"
            + "\n".join([str(m) for m in result.get_message_iterator()])
        )

    def test_discover(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Standard test for `discover`.

        Runs for configs listed under `discovery` in `acceptance-test-config.yml`: `discover`
        must succeed and return at least one stream.
        """
        skip_unless_runs(scenario, "discover")
        if scenario.expected_outcome.expect_exception():
            # If the scenario expects an exception, we can't ensure it specifically would fail
            # in discover, because some discover implementations do not need to make a connection.
            # We skip this test in that case.
            pytest.skip("Skipping discover test for scenario that expects an exception.")
            return

        connector_root = self.get_connector_root_dir()
        discover_result = run_test_job(
            self.create_connector(scenario),
            "discover",
            connector_root=connector_root,
            test_scenario=scenario.with_default_success(),
        )
        get_discovered_streams(discover_result, connector_root.absolute().name)

    def test_spec(self) -> None:
        """Standard test for `spec`.

        This test does not require a `scenario` input, since `spec`
        does not require any inputs.

        We assume `spec` should always succeed and it should always generate
        a valid `SPEC` message.

        Note: the parsing of messages by type also implicitly validates that
        the generated `SPEC` message is valid JSON.
        """
        result = run_test_job(
            verb="spec",
            test_scenario=None,
            connector=self.create_connector(scenario=None),
            connector_root=self.get_connector_root_dir(),
        )
        # If an error occurs, it will be raised above.

        assert len(result.spec_messages) == 1, (
            f"Expected exactly 1 spec message but got {len(result.spec_messages)}. "
            f"Errors: {result.errors!s}"
        )

    def test_basic_read(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Run standard `read` test on the connector.

        Runs for configs listed under `basic_read` or `full_refresh` in
        `acceptance-test-config.yml`. This test is designed to validate the connector's ability
        to read data from the source and return records. It first runs a `discover` job to
        obtain the catalog of streams, and then it runs a full-refresh `read` job to fetch
        records from those streams, except the ones declared in `empty_streams`.

        A read expected to succeed must return records. For a config listed under `basic_read`,
        the records are also checked as in CAT's basic read test: every stream must return at
        least one record, and every record must match its stream's JSON schema (opt out with
        `validate_schema: false`, which a connector at `test_strictness_level: high` cannot).
        """
        skip_unless_runs(scenario, "read")
        if scenario.is_basic_read_config:
            assert_schema_validation_opt_out_allowed(
                validate_schema=scenario.validate_schema,
                test_strictness_level=scenario.test_strictness_level,
            )
        connector_root = self.get_connector_root_dir()
        discover_result = run_test_job(
            self.create_connector(scenario),
            "discover",
            connector_root=connector_root,
            test_scenario=scenario.without_expected_outcome(),
        )
        if scenario.expected_outcome.expect_exception() and discover_result.errors:
            # Failed as expected; we're done.
            return
        if discover_result.errors:
            raise discover_result.as_exception()
        streams = get_discovered_streams(discover_result, connector_root.absolute().name)

        if scenario.empty_streams:
            # Filter out streams marked as empty in the scenario.
            empty_stream_names = [stream.name for stream in scenario.empty_streams]
            streams = [s for s in streams if s.name not in empty_stream_names]

        configured_catalog = ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=stream,
                    sync_mode=SyncMode.full_refresh,
                    destination_sync_mode=DestinationSyncMode.append_dedup,
                )
                for stream in streams
            ]
        )
        read_scenario = scenario.with_default_success()
        result = run_test_job(
            self.create_connector(scenario),
            "read",
            test_scenario=read_scenario,
            connector_root=connector_root,
            catalog=configured_catalog,
        )

        if read_scenario.expected_outcome.expect_exception():
            # The read failed as expected (asserted by `run_test_job`); there are no records to check.
            return

        if scenario.is_basic_read_config:
            # Also asserts that the read returned records, in the same pass over them.
            assert_read_records(
                records=result.records_iterator,
                configured_catalog=configured_catalog,
                require_records_per_stream=True,
                validate_schema=scenario.validate_schema,
            )
        elif next(result.records_iterator, None) is None:
            raise AssertionError("Expected records but got none.")

    def test_incremental_read(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Run an incremental `read` on the connector.

        Runs for configs listed under `incremental` in `acceptance-test-config.yml`. Every
        discovered stream that supports incremental sync (minus `empty_streams`, and minus
        streams that need a user-defined cursor the catalog cannot know) is read in incremental
        mode; the read must finish without errors and emit at least one STATE message.
        """
        skip_unless_runs(scenario, "incremental_read")
        if scenario.expected_outcome.expect_exception():
            pytest.skip("Skipping incremental read test for scenario that expects an exception.")
            return

        connector_root = self.get_connector_root_dir()
        discover_result = run_test_job(
            self.create_connector(scenario),
            "discover",
            connector_root=connector_root,
            test_scenario=scenario.with_default_success(),
        )
        streams = get_discovered_streams(discover_result, connector_root.absolute().name)
        incremental_streams = [
            stream
            for stream in streams
            if SyncMode.incremental in (stream.supported_sync_modes or [])
            and (stream.source_defined_cursor or stream.default_cursor_field)
        ]
        assert incremental_streams, (
            f"Config '{scenario.id}' is listed under `incremental` in "
            "`acceptance-test-config.yml`, but no discovered stream supports incremental sync."
        )

        if scenario.empty_streams:
            empty_stream_names = [stream.name for stream in scenario.empty_streams]
            incremental_streams = [
                stream for stream in incremental_streams if stream.name not in empty_stream_names
            ]
            if not incremental_streams:
                pytest.skip("Every incremental stream is listed in `empty_streams`.")
                return

        configured_catalog = ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=stream,
                    sync_mode=SyncMode.incremental,
                    cursor_field=stream.default_cursor_field,
                    destination_sync_mode=DestinationSyncMode.append_dedup,
                )
                for stream in incremental_streams
            ]
        )
        result = run_test_job(
            self.create_connector(scenario),
            "read",
            test_scenario=scenario.with_default_success(),
            connector_root=connector_root,
            catalog=configured_catalog,
        )
        assert result.state_messages, (
            f"Incremental read for config '{scenario.id}' emitted no STATE message. "
            f"Streams read: {[stream.name for stream in incremental_streams]}"
        )

    def test_fail_read_with_bad_catalog(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Standard test for `read` when passed a bad catalog file.

        Runs for configs listed under `basic_read`, `full_refresh` or `incremental` in
        `acceptance-test-config.yml`.
        """
        skip_unless_runs(scenario, "read", "incremental_read")
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
                ),
            ],
        )
        result: entrypoint_wrapper.EntrypointOutput = run_test_job(
            self.create_connector(scenario),
            "read",
            connector_root=self.get_connector_root_dir(),
            test_scenario=scenario.with_expecting_failure(),  # Expect failure due to bad catalog
            catalog=asdict(invalid_configured_catalog),
        )
        assert result.errors, "Expected errors but got none."
        assert result.trace_messages, "Expected trace messages but got none."
