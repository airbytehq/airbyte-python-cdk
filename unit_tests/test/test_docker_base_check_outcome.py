# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for `_assert_check_outcome` in `airbyte_cdk.test.standard_tests.docker_base`."""

import json

import pytest

from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models import ExpectedOutcome
from airbyte_cdk.test.standard_tests.docker_base import _assert_check_outcome


def _check_output(*statuses: str) -> EntrypointOutput:
    """Build an EntrypointOutput with one CONNECTION_STATUS message per given status."""
    return EntrypointOutput(
        messages=[
            json.dumps({"type": "CONNECTION_STATUS", "connectionStatus": {"status": status}})
            for status in statuses
        ],
        command=["docker", "run", "..."],
    )


@pytest.mark.parametrize(
    "expected_outcome, statuses, should_pass",
    [
        # Scenarios expecting success: only SUCCEEDED passes.
        pytest.param(ExpectedOutcome.EXPECT_SUCCESS, ["SUCCEEDED"], True, id="success_succeeded"),
        pytest.param(ExpectedOutcome.EXPECT_SUCCESS, ["FAILED"], False, id="success_failed"),
        pytest.param(ExpectedOutcome.EXPECT_SUCCESS, [], False, id="success_no_status"),
        # Scenarios expecting failure: a SUCCEEDED status is a regression and must fail;
        # a FAILED status or no status at all (uncaught error) both count as failing as expected.
        pytest.param(ExpectedOutcome.EXPECT_EXCEPTION, ["FAILED"], True, id="failure_failed"),
        pytest.param(
            ExpectedOutcome.EXPECT_EXCEPTION, ["SUCCEEDED"], False, id="failure_succeeded"
        ),
        pytest.param(ExpectedOutcome.EXPECT_EXCEPTION, [], True, id="failure_no_status"),
        # ALLOW_ANY scenarios: either reported outcome passes, but `check` must still
        # emit a CONNECTION_STATUS message.
        pytest.param(ExpectedOutcome.ALLOW_ANY, ["SUCCEEDED"], True, id="allow_any_succeeded"),
        pytest.param(ExpectedOutcome.ALLOW_ANY, ["FAILED"], True, id="allow_any_failed"),
        pytest.param(ExpectedOutcome.ALLOW_ANY, [], False, id="allow_any_no_status"),
        # The last CONNECTION_STATUS message wins.
        pytest.param(
            ExpectedOutcome.EXPECT_SUCCESS,
            ["FAILED", "SUCCEEDED"],
            True,
            id="success_last_status_wins",
        ),
        pytest.param(
            ExpectedOutcome.EXPECT_EXCEPTION,
            ["FAILED", "SUCCEEDED"],
            False,
            id="failure_last_status_wins",
        ),
    ],
)
def test_assert_check_outcome(
    expected_outcome: ExpectedOutcome,
    statuses: list[str],
    should_pass: bool,
) -> None:
    check_result = _check_output(*statuses)
    if should_pass:
        _assert_check_outcome(
            check_result=check_result,
            expected_outcome=expected_outcome,
            connector_name="source-test",
        )
    else:
        with pytest.raises(AssertionError):
            _assert_check_outcome(
                check_result=check_result,
                expected_outcome=expected_outcome,
                connector_name="source-test",
            )
