# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the shared `check` assertion used by the Standard Tests.

The assertion is exercised directly (as the Docker-based suite calls it) and through the
in-process job runner, since both paths must enforce the same expectations.
"""

import json
import logging
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping

import pytest

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
)
from airbyte_cdk.sources import Source
from airbyte_cdk.test.entrypoint_wrapper import AirbyteEntrypointException, EntrypointOutput
from airbyte_cdk.test.models import ConnectorTestScenario, ExpectedOutcome
from airbyte_cdk.test.standard_tests._assertions import assert_check_outcome
from airbyte_cdk.test.standard_tests._job_runner import run_test_job


def _check_output(*statuses: str) -> EntrypointOutput:
    """Build an EntrypointOutput with one CONNECTION_STATUS message per given status."""
    return EntrypointOutput(
        messages=[
            json.dumps({"type": "CONNECTION_STATUS", "connectionStatus": {"status": status}})
            for status in statuses
        ],
        command=["docker", "run", "..."],
    )


OUTCOME_MATRIX = [
    # Scenarios expecting success: only SUCCEEDED passes.
    pytest.param(ExpectedOutcome.EXPECT_SUCCESS, "SUCCEEDED", True, id="success_succeeded"),
    pytest.param(ExpectedOutcome.EXPECT_SUCCESS, "FAILED", False, id="success_failed"),
    pytest.param(ExpectedOutcome.EXPECT_SUCCESS, None, False, id="success_no_status"),
    # Scenarios expecting failure: only a reported FAILED status passes. A `check` that raises
    # without reporting any status is itself a failure to report the outcome gracefully.
    pytest.param(ExpectedOutcome.EXPECT_EXCEPTION, "FAILED", True, id="failure_failed"),
    pytest.param(ExpectedOutcome.EXPECT_EXCEPTION, "SUCCEEDED", False, id="failure_succeeded"),
    pytest.param(ExpectedOutcome.EXPECT_EXCEPTION, None, False, id="failure_no_status"),
    # Scenarios with no declared status default to expecting success, matching the default of
    # the `status` property in `acceptance-test-config.yml`.
    pytest.param(ExpectedOutcome.ALLOW_ANY, "SUCCEEDED", True, id="allow_any_succeeded"),
    pytest.param(ExpectedOutcome.ALLOW_ANY, "FAILED", False, id="allow_any_failed"),
    pytest.param(ExpectedOutcome.ALLOW_ANY, None, False, id="allow_any_no_status"),
]


@pytest.mark.parametrize("expected_outcome, status, should_pass", OUTCOME_MATRIX)
def test_assert_check_outcome(
    expected_outcome: ExpectedOutcome,
    status: str | None,
    should_pass: bool,
) -> None:
    check_result = _check_output(*([status] if status else []))
    if should_pass:
        assert_check_outcome(
            check_result=check_result,
            expected_outcome=expected_outcome,
            connector_name="source-test",
        )
    else:
        with pytest.raises(AssertionError):
            assert_check_outcome(
                check_result=check_result,
                expected_outcome=expected_outcome,
                connector_name="source-test",
            )


@pytest.mark.parametrize(
    "expected_outcome, statuses, should_pass",
    [
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
def test_assert_check_outcome_uses_last_status(
    expected_outcome: ExpectedOutcome,
    statuses: list[str],
    should_pass: bool,
) -> None:
    check_result = _check_output(*statuses)
    if should_pass:
        assert_check_outcome(
            check_result=check_result,
            expected_outcome=expected_outcome,
            connector_name="source-test",
        )
    else:
        with pytest.raises(AssertionError):
            assert_check_outcome(
                check_result=check_result,
                expected_outcome=expected_outcome,
                connector_name="source-test",
            )


class _FakeSource(Source):
    """A source whose `check` reports a fixed status, or raises if no status is given."""

    def __init__(self, status: Status | None) -> None:
        self._status = status

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(
            connectionSpecification={"type": "object", "properties": {}},
        )

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        if self._status is None:
            raise RuntimeError("Uncaught error during check.")

        return AirbyteConnectionStatus(status=self._status)

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        return AirbyteCatalog(streams=[])

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: MutableMapping[str, Any] | None = None,
    ) -> Iterable[AirbyteMessage]:
        yield from []


@pytest.mark.parametrize("expected_outcome, status, should_pass", OUTCOME_MATRIX)
def test_run_test_job_check_asserts_reported_status(
    expected_outcome: ExpectedOutcome,
    status: str | None,
    should_pass: bool,
    tmp_path: Path,
) -> None:
    """The in-process path must enforce the same expectations as the Docker path."""
    scenario = ConnectorTestScenario(
        config_dict={"dummy_setting": "dummy_value"},
        status={
            ExpectedOutcome.EXPECT_SUCCESS: "succeed",
            ExpectedOutcome.EXPECT_EXCEPTION: "failed",
            ExpectedOutcome.ALLOW_ANY: None,
        }[expected_outcome],
    )
    source = _FakeSource(Status(status) if status else None)

    def _run() -> None:
        run_test_job(
            source,
            "check",
            connector_root=tmp_path,
            test_scenario=scenario,
        )

    if should_pass:
        _run()
    else:
        # A `check` that raises instead of reporting a status surfaces as a traced exception
        # before the status assertion is reached.
        with pytest.raises((AssertionError, AirbyteEntrypointException)):
            _run()
