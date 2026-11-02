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
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
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
    StreamStatusWarning,
    assert_error_trace_messages,
    assert_read_records,
    assert_schema_validation_opt_out_allowed,
    find_stream_status_problems,
    make_invalid_config,
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
        the read is also checked as in CAT's basic read test:

        - every stream it reads must return at least one record;
        - every record must match its stream's JSON schema (opt out with
          `validate_schema: false`, which a connector at `test_strictness_level: high` cannot);
        - every record of a stream with a `source_defined_primary_key` must have a usable
          primary key: no key field holds an object or an array, and not every key field is
          missing or null (opt out with `validate_primary_keys_data_type: false`);
        - every stream it reads must emit a `STREAM` (or `GLOBAL`) state message, and every
          state message must carry `sourceStats` (opt out with `validate_state_messages: false`).

        Every stream it reads should also report `STARTED`, then only `RUNNING`, then
        `COMPLETE` stream statuses. That check is not enforced yet: its problems are reported as
        a `StreamStatusWarning`.
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
            status_problems = find_stream_status_problems(result.trace_messages, configured_catalog)
            if status_problems:
                warnings.warn(
                    "The read's stream statuses are incomplete. This check is not enforced "
                    "yet:\n" + "\n".join(status_problems),
                    category=StreamStatusWarning,
                    stacklevel=1,
                )
            # Also asserts that the read returned records, in the same pass over them.
            assert_read_records(
                records=result.records_iterator,
                configured_catalog=configured_catalog,
                require_records_per_stream=True,
                validate_schema=scenario.validate_schema,
                validate_primary_keys=scenario.validate_primary_keys_data_type,
                state_messages=(
                    result.state_messages if scenario.validate_state_messages else None
                ),
            )
        elif next(result.records_iterator, None) is None:
            raise AssertionError("Expected records but got none.")

    def test_fail_read_with_bad_catalog(
        self,
        scenario: ConnectorTestScenario,
    ) -> None:
        """Standard test for `read` when passed a bad catalog file.

        Runs for configs listed under `basic_read` or `full_refresh` in
        `acceptance-test-config.yml`. The read must fail and emit a well-formed `ERROR` trace
        message (see `assert_error_trace_messages`). This is CAT's
        `TestBasicRead.test_airbyte_trace_message_on_failure`, and as in CAT it is skipped for a
        config whose `basic_read` entry sets `expect_trace_message_on_failure: false`.
        """
        skip_unless_runs(scenario, "read")
        if not scenario.expect_trace_message_on_failure:
            pytest.skip(
                f"`expect_trace_message_on_failure: false` is set for {scenario.id!r} in "
                "`acceptance-test-config.yml`."
            )
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
        assert_error_trace_messages(result.get_message_iterator())

    def test_fail_read_with_invalid_config(self) -> None:
        """Standard test for `read` with a config that the connector's spec rejects.

        A read that fails must report why in a well-formed `ERROR` trace message (see
        `assert_error_trace_messages`). CAT had no such test: its
        `test_airbyte_trace_message_on_failure` is `test_fail_read_with_bad_catalog`, and this
        test honours the same opt-out. Where `test_fail_read_with_bad_catalog` breaks the
        catalog, which the entrypoint rejects before any connector code runs, this test gives
        the connector a config its own spec rejects: one that holds none of the spec's required
        top-level properties and no value from any real config (`make_invalid_config`). No
        credentials are needed, so it runs once per connector, and no request can be sent with
        real credentials.

        Skipped when the spec requires no top-level property (no config is invalid for sure),
        or when any `basic_read` entry of `acceptance-test-config.yml` sets
        `expect_trace_message_on_failure: false`: the connector does not promise an error trace
        for a failed read.

        A source that cannot even be created from the invalid config fails the same way
        `source-declarative-manifest` reports it: as an uncaught error, turned into a trace
        message.
        """
        opted_out = sorted(
            scenario.id
            for scenario in self.get_scenarios()
            if not scenario.expect_trace_message_on_failure
        )
        if opted_out:
            pytest.skip(
                "`expect_trace_message_on_failure: false` is set for "
                f"{', '.join(repr(scenario_id) for scenario_id in opted_out)} in "
                "`acceptance-test-config.yml`."
            )

        connector_root = self.get_connector_root_dir()
        spec_result = run_test_job(
            self.create_connector(scenario=None),
            "spec",
            test_scenario=None,
            connector_root=connector_root,
        )
        spec = get_single_spec(spec_result, connector_name=self.connector_name)
        invalid_config = make_invalid_config(spec.connectionSpecification)
        if invalid_config is None:
            pytest.skip(
                "The connector's spec requires no top-level property, so no config is "
                "certain to be invalid."
            )

        # No expected status: a read that succeeds is reported below, with its likely cause.
        scenario = ConnectorTestScenario(config_dict=invalid_config)
        catalog = ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=AirbyteStream(
                        name="__AIRBYTE__stream_that_does_not_exist",
                        json_schema={
                            "type": "object",
                            "properties": {"f1": {"type": "string"}},
                        },
                        supported_sync_modes=[SyncMode.full_refresh],
                    ),
                    sync_mode=SyncMode.full_refresh,
                    destination_sync_mode=DestinationSyncMode.append,
                )
            ]
        )
        try:
            connector = self.create_connector(scenario)
        except Exception as error:  # noqa: BLE001  # Any error here is the read failing.
            result = EntrypointOutput(messages=[], uncaught_exception=error)
        else:
            result = run_test_job(
                connector,
                "read",
                connector_root=connector_root,
                test_scenario=scenario,
                catalog=catalog,
            )
        assert result.errors, (
            "The read succeeded with a config that the connector's spec rejects (it holds none "
            "of the spec's required top-level properties). The connector did not validate the "
            "config against its spec: the CDK entrypoint does so unless the source sets "
            "`check_config_against_spec = False`. To skip this test, set "
            "`expect_trace_message_on_failure: false` in a `basic_read` entry of "
            "`acceptance-test-config.yml`, with a YAML comment giving the reason."
        )
        assert_error_trace_messages(result.get_message_iterator())
