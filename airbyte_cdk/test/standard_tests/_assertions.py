# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Shared assertions for Airbyte Standard Tests.

These assertions are shared between the in-process test runner (`_job_runner.run_test_job`)
and the Docker-based test suite (`docker_base.DockerConnectorTestSuite`), so that both paths
enforce the same expectations.
"""

from __future__ import annotations

from airbyte_cdk.models import Status
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models import ExpectedOutcome


def assert_check_outcome(
    *,
    check_result: EntrypointOutput,
    expected_outcome: ExpectedOutcome,
    connector_name: str,
) -> None:
    """Assert that the outcome of `check` matches the scenario's expected outcome.

    A failing `check` reports `status: FAILED` in a `CONNECTION_STATUS` message and still
    exits 0, so exit-code checks alone do not catch it. We therefore assert the reported
    status explicitly, following the `status` semantics of `acceptance-test-config.yml`:

    - `succeed` (`EXPECT_SUCCESS`): a `CONNECTION_STATUS` message with status `SUCCEEDED`.
    - `failed` (`EXPECT_EXCEPTION`): a `CONNECTION_STATUS` message with status `FAILED`.
    - `exception` (`EXPECT_UNCAUGHT_ERROR`): `check` raises, so a `TRACE` error must be present
      and no `SUCCEEDED` status may be reported. A `CONNECTION_STATUS` message is not required.
    - no `status` (`ALLOW_ANY`): treated as `succeed`, matching the CAT default. A config that
      `check` is expected to reject must declare `status: failed` (or `exception`).

    When more than one `CONNECTION_STATUS` message is present, the last one wins.
    """
    connection_statuses = [
        message.connectionStatus
        for message in check_result.connection_status_messages
        if message.connectionStatus is not None
    ]
    if expected_outcome.expect_uncaught_error():
        assert check_result.errors, (
            f"`check` for connector '{connector_name}' was expected to raise "
            f"(`status: exception`), but emitted no TRACE error. "
            f"Reported statuses: {connection_statuses}"
        )
        assert not connection_statuses or connection_statuses[-1].status != Status.SUCCEEDED, (
            f"`check` for connector '{connector_name}' was expected to raise "
            f"(`status: exception`), but reported: {connection_statuses[-1]}"
        )
        return

    assert connection_statuses, (
        f"`check` for connector '{connector_name}' emitted no CONNECTION_STATUS message. "
        f"A `check` implementation should report its outcome as a CONNECTION_STATUS message "
        f"instead of raising. If raising is the expected outcome for this config, declare "
        f"`status: exception` for it in `acceptance-test-config.yml`.\n"
        + (f"Errors: {check_result.get_formatted_error_message()}\n" if check_result.errors else "")
        + f"Logs: {check_result.logs}"
    )
    reported_status = connection_statuses[-1].status
    if expected_outcome.expect_exception():
        assert reported_status == Status.FAILED, (
            f"`check` for connector '{connector_name}' was expected to fail, but reported: "
            f"{connection_statuses[-1]}"
        )
        return

    # Both `EXPECT_SUCCESS` and `ALLOW_ANY` (no declared status) require a successful `check`.
    assert reported_status == Status.SUCCEEDED, (
        f"`check` for connector '{connector_name}' did not succeed: {connection_statuses[-1]}"
    )
