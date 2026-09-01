# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
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
    """Assert that the reported CONNECTION_STATUS matches the scenario's expected outcome.

    A failing `check` reports `status: FAILED` in a `CONNECTION_STATUS` message and still
    exits 0, so exit-code checks alone do not catch it. We therefore assert the reported
    status explicitly, in both directions:
    - A scenario expecting success must report `SUCCEEDED`.
    - A scenario expecting failure must report `FAILED`.
    - A scenario that does not declare a status is treated as expecting success, matching the
      default of the `status` property in `acceptance-test-config.yml`.
    """
    connection_statuses = [
        message.connectionStatus
        for message in check_result.connection_status_messages
        if message.connectionStatus is not None
    ]
    assert connection_statuses, (
        f"`check` for connector '{connector_name}' emitted no CONNECTION_STATUS message. "
        f"A `check` implementation should report its outcome as a CONNECTION_STATUS message "
        f"instead of raising. Logs: {check_result.logs}"
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
