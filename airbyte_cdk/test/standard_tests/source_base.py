# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for source test suites."""

import warnings
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
from airbyte_cdk.test.standard_tests._catalog_checks import (
    DiscoveredCatalogWarning,
    find_catalog_problems,
    format_catalog_problems,
)
from airbyte_cdk.test.standard_tests._job_runner import run_test_job
from airbyte_cdk.test.standard_tests._read_assertions import (
    assert_read_records,
    assert_schema_validation_opt_out_allowed,
)
from airbyte_cdk.test.standard_tests._spec_lint import assert_spec_is_valid, get_single_spec
from airbyte_cdk.test.standard_tests.connector_base import (
    ConnectorTestSuiteBase,
)
from airbyte_cdk.test.standard_tests.docker_base import (
    get_discovered_streams,
    get_read_streams,
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
            inspect_output=lambda output: self._assert_no_secrets_in_output(
                scenario, output, verb="check"
            ),
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

        Runs for configs listed under `discovery` in `acceptance-test-config.yml`. Assert
        that the connector emits exactly one catalog and that the catalog passes the checks in
        `CATALOG_CHECKS`. Problems found by checks that are not enforced yet are reported as a
        `DiscoveredCatalogWarning` instead of failing the test.
        """
        skip_unless_runs(scenario, "discover")
        if scenario.expected_outcome.expect_exception():
            # If the scenario expects an exception, we can't ensure it specifically would fail
            # in discover, because some discover implementations do not need to make a connection.
            # We skip this test in that case.
            pytest.skip("Skipping discover test for scenario that expects an exception.")
            return

        connector_root = self.get_connector_root_dir()
        result = run_test_job(
            self.create_connector(scenario),
            "discover",
            connector_root=connector_root,
            test_scenario=scenario.with_default_success(),
        )
        if result.errors and not scenario.expected_outcome.expect_success():
            # The scenario allows `discover` to fail, so there is no catalog to check.
            return

        catalog_messages = result.get_message_by_types([Type.CATALOG])
        assert len(catalog_messages) == 1, (
            f"Expected exactly one CATALOG message. Got {len(catalog_messages)}."
        )
        catalog = catalog_messages[0].catalog
        assert catalog is not None, "The CATALOG message does not contain a catalog."

        problems = find_catalog_problems(catalog)
        advisory = {check: found for check, found in problems.items() if not check.enforced}
        if advisory:
            warnings.warn(
                "The discovered catalog has problems that will fail this test in a future "
                f"CDK release:\n{format_catalog_problems(advisory)}",
                category=DiscoveredCatalogWarning,
                stacklevel=1,
            )
        enforced = {check: found for check, found in problems.items() if check.enforced}
        assert not enforced, (
            f"The discovered catalog has problems:\n{format_catalog_problems(enforced)}"
        )

    def test_spec(self) -> None:
        """Standard test for `spec`.

        This test does not require a `scenario` input, since `spec`
        does not require any inputs.

        We assume `spec` should always succeed and it should always generate
        a single valid `SPEC` message, which must pass the credential-free spec lint.

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

        spec = get_single_spec(result, connector_name=self.connector_name)
        assert_spec_is_valid(spec, connector_name=self.connector_name)

    def test_basic_read(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Run standard `read` test on the connector.

        Runs for configs listed under `basic_read` or `full_refresh` in
        `acceptance-test-config.yml`. This test is designed to validate the connector's ability
        to read data from the source and return records. It first runs a `discover` job to
        obtain the catalog of streams, and then it runs a full-refresh `read` job to fetch
        records from the streams of the config's `configured_catalog_path`, or from every
        discovered stream when it declares none, minus its `empty_streams` (see
        `get_read_streams`).

        A read expected to succeed must return records. For a config listed under `basic_read`,
        the records are also checked as in CAT's basic read test: every stream it reads must
        return at least one record, and every record must match its stream's JSON schema (opt out
        with `validate_schema: false`, which a connector at `test_strictness_level: high` cannot).
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
        streams = get_read_streams(
            scenario,
            get_discovered_streams(discover_result, connector_root.absolute().name),
            connector_root,
        )

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

    def test_fail_read_with_bad_catalog(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Standard test for `read` when passed a bad catalog file.

        Runs for configs listed under `basic_read` or `full_refresh` in
        `acceptance-test-config.yml`.
        """
        skip_unless_runs(scenario, "read")
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
