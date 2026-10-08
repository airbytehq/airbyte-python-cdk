# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the shared `check` assertion used by the Standard Tests.

The assertion is exercised directly (as the Docker-based suite calls it), through the
in-process job runner, and through the `test_check` methods of the test-suite base classes,
since all paths must enforce the same expectations:

- `status: succeed`  -> a CONNECTION_STATUS message with status SUCCEEDED.
- `status: failed`   -> a CONNECTION_STATUS message with status FAILED.
- `status: exception` -> an uncaught error (TRACE error), no SUCCEEDED status.
- no `status`        -> treated as `succeed` (the CAT default).
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping

import pytest

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    FailureType,
    Status,
)
from airbyte_cdk.sources import Source
from airbyte_cdk.test.entrypoint_wrapper import AirbyteEntrypointException, EntrypointOutput
from airbyte_cdk.test.models import ConnectorTestScenario, ExpectedOutcome
from airbyte_cdk.test.standard_tests import ConnectorTestSuiteBase, SourceTestSuiteBase
from airbyte_cdk.test.standard_tests._assertions import assert_check_outcome
from airbyte_cdk.test.standard_tests._job_runner import IConnector, run_test_job
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

POKEAPI_RESOURCE_DIR = Path(__file__).parent.parent / "resources" / "source_pokeapi_w_components_py"

# Substrings of the assertion messages, so that each failing case fails for the expected reason.
MSG_NO_STATUS = "emitted no CONNECTION_STATUS message"
MSG_NOT_SUCCEEDED = "did not succeed"
MSG_NOT_FAILED = "was expected to fail, but reported"
MSG_NO_TRACE = "emitted no TRACE error"
MSG_RAISE_BUT_SUCCEEDED = "was expected to raise .*, but reported"

STATUS_STR_BY_OUTCOME: dict[ExpectedOutcome, Literal["succeed", "failed", "exception"] | None] = {
    ExpectedOutcome.EXPECT_SUCCESS: "succeed",
    ExpectedOutcome.EXPECT_EXCEPTION: "failed",
    ExpectedOutcome.EXPECT_UNCAUGHT_ERROR: "exception",
    ExpectedOutcome.ALLOW_ANY: None,
}


def _check_output(*statuses: str, error: bool = False) -> EntrypointOutput:
    """Build an EntrypointOutput with one CONNECTION_STATUS message per status.

    When `error` is set, a TRACE error message is appended, as the entrypoint emits for an
    uncaught exception.
    """
    messages = [
        json.dumps({"type": "CONNECTION_STATUS", "connectionStatus": {"status": status}})
        for status in statuses
    ]
    if error:
        messages.append(
            json.dumps(
                {
                    "type": "TRACE",
                    "trace": {
                        "type": "ERROR",
                        "emitted_at": 0,
                        "error": {"message": "Uncaught error during check."},
                    },
                }
            )
        )
    return EntrypointOutput(messages=messages, command=["docker", "run", "..."])


def _assert_outcome(
    check_result: EntrypointOutput,
    expected_outcome: ExpectedOutcome,
    failure_match: str | None,
) -> None:
    """Call `assert_check_outcome`, expecting it to pass or to fail with `failure_match`."""
    if failure_match is None:
        assert_check_outcome(
            check_result=check_result,
            expected_outcome=expected_outcome,
            connector_name="source-test",
        )
        return

    with pytest.raises(AssertionError, match=failure_match):
        assert_check_outcome(
            check_result=check_result,
            expected_outcome=expected_outcome,
            connector_name="source-test",
        )


# (expected outcome, reported status, TRACE error present, expected failure message or None)
OUTCOME_MATRIX = [
    # `status: succeed`: only a reported SUCCEEDED passes.
    pytest.param(ExpectedOutcome.EXPECT_SUCCESS, "SUCCEEDED", False, None, id="success_succeeded"),
    pytest.param(
        ExpectedOutcome.EXPECT_SUCCESS, "FAILED", False, MSG_NOT_SUCCEEDED, id="success_failed"
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_SUCCESS, None, False, MSG_NO_STATUS, id="success_no_status"
    ),
    pytest.param(ExpectedOutcome.EXPECT_SUCCESS, None, True, MSG_NO_STATUS, id="success_raised"),
    # `status: failed`: only a reported FAILED passes. A `check` that raises without reporting
    # a status is not a graceful failure; `status: exception` exists for that outcome.
    pytest.param(ExpectedOutcome.EXPECT_EXCEPTION, "FAILED", False, None, id="failure_failed"),
    pytest.param(
        ExpectedOutcome.EXPECT_EXCEPTION, "FAILED", True, None, id="failure_failed_with_trace"
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_EXCEPTION, "SUCCEEDED", False, MSG_NOT_FAILED, id="failure_succeeded"
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_EXCEPTION, None, False, MSG_NO_STATUS, id="failure_no_status"
    ),
    pytest.param(ExpectedOutcome.EXPECT_EXCEPTION, None, True, MSG_NO_STATUS, id="failure_raised"),
    # `status: exception`: a TRACE error is required and no SUCCEEDED may be reported. A FAILED
    # status next to the trace (e.g. a `config_error`) is accepted.
    pytest.param(ExpectedOutcome.EXPECT_UNCAUGHT_ERROR, None, True, None, id="exception_raised"),
    pytest.param(
        ExpectedOutcome.EXPECT_UNCAUGHT_ERROR, "FAILED", True, None, id="exception_raised_failed"
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_UNCAUGHT_ERROR,
        "SUCCEEDED",
        True,
        MSG_RAISE_BUT_SUCCEEDED,
        id="exception_raised_succeeded",
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_UNCAUGHT_ERROR,
        "SUCCEEDED",
        False,
        MSG_NO_TRACE,
        id="exception_succeeded",
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_UNCAUGHT_ERROR, "FAILED", False, MSG_NO_TRACE, id="exception_failed"
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_UNCAUGHT_ERROR, None, False, MSG_NO_TRACE, id="exception_no_status"
    ),
    # No declared status: treated as `succeed`, matching the CAT default.
    pytest.param(ExpectedOutcome.ALLOW_ANY, "SUCCEEDED", False, None, id="allow_any_succeeded"),
    pytest.param(
        ExpectedOutcome.ALLOW_ANY, "FAILED", False, MSG_NOT_SUCCEEDED, id="allow_any_failed"
    ),
    pytest.param(ExpectedOutcome.ALLOW_ANY, None, False, MSG_NO_STATUS, id="allow_any_no_status"),
    pytest.param(ExpectedOutcome.ALLOW_ANY, None, True, MSG_NO_STATUS, id="allow_any_raised"),
]


@pytest.mark.parametrize("expected_outcome, status, error, failure_match", OUTCOME_MATRIX)
def test_assert_check_outcome(
    expected_outcome: ExpectedOutcome,
    status: str | None,
    error: bool,
    failure_match: str | None,
) -> None:
    check_result = _check_output(*([status] if status else []), error=error)
    _assert_outcome(check_result, expected_outcome, failure_match)


def test_assert_check_outcome_no_status_message_includes_errors_and_logs() -> None:
    """The 'no status' failure must show why, including the trace error and the logs."""
    check_result = EntrypointOutput(
        messages=[
            json.dumps({"type": "LOG", "log": {"level": "INFO", "message": "Starting check..."}}),
            json.dumps(
                {
                    "type": "TRACE",
                    "trace": {
                        "type": "ERROR",
                        "emitted_at": 0,
                        "error": {"message": "Connection refused by host"},
                    },
                }
            ),
        ],
        command=["docker", "run", "..."],
    )
    with pytest.raises(AssertionError) as exc_info:
        assert_check_outcome(
            check_result=check_result,
            expected_outcome=ExpectedOutcome.EXPECT_SUCCESS,
            connector_name="source-test",
        )
    message = str(exc_info.value)
    assert "Connection refused by host" in message
    assert "Starting check..." in message
    assert "status: exception" in message


@pytest.mark.parametrize(
    "expected_outcome, statuses, failure_match",
    [
        # The last CONNECTION_STATUS message wins.
        pytest.param(
            ExpectedOutcome.EXPECT_SUCCESS,
            ["FAILED", "SUCCEEDED"],
            None,
            id="success_last_status_wins",
        ),
        pytest.param(
            ExpectedOutcome.EXPECT_EXCEPTION,
            ["FAILED", "SUCCEEDED"],
            MSG_NOT_FAILED,
            id="failure_last_status_wins",
        ),
    ],
)
def test_assert_check_outcome_uses_last_status(
    expected_outcome: ExpectedOutcome,
    statuses: list[str],
    failure_match: str | None,
) -> None:
    _assert_outcome(_check_output(*statuses), expected_outcome, failure_match)


class _FakeSource(Source):
    """A source whose `check` behaves as instructed.

    Behaviours:
    - `succeeded` / `failed`: report that CONNECTION_STATUS.
    - `raise`: raise a plain exception (uncaught; TRACE error, no status).
    - `raise_config_error`: raise an `AirbyteTracedException` with `config_error`, which the
      entrypoint turns into a TRACE error plus a FAILED status.
    - `raise_system_error`: raise an `AirbyteTracedException` with `system_error`, which the
      entrypoint re-raises after emitting the TRACE error (no status).
    """

    def __init__(self, behavior: str) -> None:
        self._behavior = behavior

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(
            connectionSpecification={"type": "object", "properties": {}},
        )

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        if self._behavior == "raise":
            raise RuntimeError("Uncaught error during check.")
        if self._behavior == "raise_config_error":
            raise AirbyteTracedException(
                message="Invalid credentials.",
                failure_type=FailureType.config_error,
            )
        if self._behavior == "raise_system_error":
            raise AirbyteTracedException(
                message="Upstream is down.",
                failure_type=FailureType.system_error,
            )
        return AirbyteConnectionStatus(status=Status(self._behavior.upper()))

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        return AirbyteCatalog(streams=[])

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterable[AirbyteMessage]:
        yield from []


# (expected outcome, `check` behaviour, expected exception type or None, failure message)
IN_PROCESS_MATRIX = [
    # `status: succeed`
    pytest.param(ExpectedOutcome.EXPECT_SUCCESS, "succeeded", None, None, id="success_succeeded"),
    pytest.param(
        ExpectedOutcome.EXPECT_SUCCESS,
        "failed",
        AssertionError,
        MSG_NOT_SUCCEEDED,
        id="success_failed",
    ),
    # An uncaught error is surfaced as-is, as on the Docker path (`raise_if_errors=True`).
    pytest.param(
        ExpectedOutcome.EXPECT_SUCCESS,
        "raise",
        AirbyteEntrypointException,
        "Uncaught error during check",
        id="success_raised",
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_SUCCESS,
        "raise_config_error",
        AirbyteEntrypointException,
        "Invalid credentials",
        id="success_raised_config_error",
    ),
    # `status: failed`
    pytest.param(ExpectedOutcome.EXPECT_EXCEPTION, "failed", None, None, id="failure_failed"),
    pytest.param(
        ExpectedOutcome.EXPECT_EXCEPTION,
        "raise_config_error",
        None,
        None,
        id="failure_raised_config_error",
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_EXCEPTION,
        "succeeded",
        AssertionError,
        MSG_NOT_FAILED,
        id="failure_succeeded",
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_EXCEPTION,
        "raise",
        AssertionError,
        MSG_NO_STATUS,
        id="failure_raised",
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_EXCEPTION,
        "raise_system_error",
        AssertionError,
        MSG_NO_STATUS,
        id="failure_raised_system_error",
    ),
    # `status: exception`
    pytest.param(ExpectedOutcome.EXPECT_UNCAUGHT_ERROR, "raise", None, None, id="exception_raised"),
    pytest.param(
        ExpectedOutcome.EXPECT_UNCAUGHT_ERROR,
        "raise_system_error",
        None,
        None,
        id="exception_raised_system_error",
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_UNCAUGHT_ERROR,
        "raise_config_error",
        None,
        None,
        id="exception_raised_config_error",
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_UNCAUGHT_ERROR,
        "succeeded",
        AssertionError,
        MSG_NO_TRACE,
        id="exception_succeeded",
    ),
    pytest.param(
        ExpectedOutcome.EXPECT_UNCAUGHT_ERROR,
        "failed",
        AssertionError,
        MSG_NO_TRACE,
        id="exception_failed",
    ),
    # No declared status: treated as `succeed`.
    pytest.param(ExpectedOutcome.ALLOW_ANY, "succeeded", None, None, id="allow_any_succeeded"),
    pytest.param(
        ExpectedOutcome.ALLOW_ANY,
        "failed",
        AssertionError,
        MSG_NOT_SUCCEEDED,
        id="allow_any_failed",
    ),
    pytest.param(
        ExpectedOutcome.ALLOW_ANY,
        "raise",
        AirbyteEntrypointException,
        "Uncaught error during check",
        id="allow_any_raised",
    ),
]


def _scenario_for(expected_outcome: ExpectedOutcome) -> ConnectorTestScenario:
    return ConnectorTestScenario(
        config_dict={"dummy_setting": "dummy_value"},
        status=STATUS_STR_BY_OUTCOME[expected_outcome],
    )


@pytest.mark.parametrize(
    "expected_outcome, behavior, expected_exception, failure_match", IN_PROCESS_MATRIX
)
def test_run_test_job_check_asserts_reported_status(
    expected_outcome: ExpectedOutcome,
    behavior: str,
    expected_exception: type[Exception] | None,
    failure_match: str | None,
    tmp_path: Path,
) -> None:
    """The in-process path must enforce the same expectations as the Docker path."""
    scenario = _scenario_for(expected_outcome)
    source = _FakeSource(behavior)

    if expected_exception is None:
        run_test_job(source, "check", connector_root=tmp_path, test_scenario=scenario)
        return

    with pytest.raises(expected_exception, match=failure_match):
        run_test_job(source, "check", connector_root=tmp_path, test_scenario=scenario)


class _SuiteWithFakeConnector:
    """Mixin that points a test-suite base class at a fake connector and a temp root dir."""

    connector_root_dir: Path
    connector_behavior: str

    @classmethod
    def get_connector_root_dir(cls) -> Path:
        return cls.connector_root_dir

    @classmethod
    def create_connector(cls, scenario: ConnectorTestScenario | None) -> IConnector:
        return _FakeSource(cls.connector_behavior)


class _FakeSourceSuite(_SuiteWithFakeConnector, SourceTestSuiteBase):
    pass


class _FakeConnectorSuite(_SuiteWithFakeConnector, ConnectorTestSuiteBase):
    pass


@pytest.mark.parametrize("suite_class", [_FakeSourceSuite, _FakeConnectorSuite])
@pytest.mark.parametrize(
    "expected_outcome, behavior, expected_exception, failure_match", IN_PROCESS_MATRIX
)
def test_suite_test_check_asserts_reported_status(
    suite_class: type[_SuiteWithFakeConnector],
    expected_outcome: ExpectedOutcome,
    behavior: str,
    expected_exception: type[Exception] | None,
    failure_match: str | None,
    tmp_path: Path,
) -> None:
    """`test_check` of the suite base classes must enforce the shared expectations too.

    In particular, a `status: exception` scenario must not be failed by the "exactly one
    CONNECTION_STATUS message" assertion that follows the job run.
    """
    suite_class.connector_root_dir = tmp_path
    suite_class.connector_behavior = behavior
    scenario = _scenario_for(expected_outcome)
    suite = suite_class()

    if expected_exception is None:
        suite.test_check(scenario)  # type: ignore[attr-defined]
        return

    with pytest.raises(expected_exception, match=failure_match):
        suite.test_check(scenario)  # type: ignore[attr-defined]


# (expected outcome, container output, expected exception type or None, failure message)
DOCKER_MATRIX = [
    pytest.param(ExpectedOutcome.EXPECT_SUCCESS, "SUCCEEDED", False, None, None, id="success"),
    pytest.param(
        ExpectedOutcome.EXPECT_SUCCESS,
        "FAILED",
        False,
        AssertionError,
        MSG_NOT_SUCCEEDED,
        id="success_failed",
    ),
    # The container raised: `raise_if_errors` surfaces it before the status assertion.
    pytest.param(
        ExpectedOutcome.EXPECT_SUCCESS,
        None,
        True,
        AirbyteEntrypointException,
        "Uncaught error during check",
        id="success_raised",
    ),
    pytest.param(ExpectedOutcome.EXPECT_EXCEPTION, "FAILED", False, None, None, id="failure"),
    pytest.param(
        ExpectedOutcome.EXPECT_EXCEPTION,
        None,
        True,
        AssertionError,
        MSG_NO_STATUS,
        id="failure_raised",
    ),
    pytest.param(ExpectedOutcome.EXPECT_UNCAUGHT_ERROR, None, True, None, None, id="exception"),
    pytest.param(
        ExpectedOutcome.EXPECT_UNCAUGHT_ERROR,
        "SUCCEEDED",
        False,
        AssertionError,
        MSG_NO_TRACE,
        id="exception_succeeded",
    ),
    pytest.param(
        ExpectedOutcome.ALLOW_ANY,
        "FAILED",
        False,
        AssertionError,
        MSG_NOT_SUCCEEDED,
        id="allow_any_failed",
    ),
    pytest.param(
        ExpectedOutcome.ALLOW_ANY,
        None,
        False,
        AssertionError,
        MSG_NO_STATUS,
        id="allow_any_no_status",
    ),
]


@pytest.mark.parametrize(
    "expected_outcome, status, error, expected_exception, failure_match", DOCKER_MATRIX
)
def test_docker_image_build_and_check_asserts_reported_status(
    expected_outcome: ExpectedOutcome,
    status: str | None,
    error: bool,
    expected_exception: type[Exception] | None,
    failure_match: str | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Docker-path `check` test must assert the reported status without needing Docker.

    `run_docker_airbyte_command` is replaced by a stub returning canned container output, and
    the image build is skipped via `connector_image_override`.
    """
    shutil.copy(POKEAPI_RESOURCE_DIR / "metadata.yaml", tmp_path / "metadata.yaml")
    recorded_calls: list[dict[str, Any]] = []

    def _fake_run_docker_airbyte_command(
        cmd: list[str],
        *,
        raise_if_errors: bool = False,
    ) -> EntrypointOutput:
        recorded_calls.append({"cmd": cmd, "raise_if_errors": raise_if_errors})
        result = _check_output(*([status] if status else []), error=error)
        if raise_if_errors:
            result.raise_if_errors()
        return result

    monkeypatch.setattr(
        "airbyte_cdk.test.standard_tests.docker_base.run_docker_airbyte_command",
        _fake_run_docker_airbyte_command,
    )

    class _Suite(DockerConnectorTestSuite):
        @classmethod
        def get_connector_root_dir(cls) -> Path:
            return tmp_path

    scenario = _scenario_for(expected_outcome)

    def _run() -> None:
        _Suite().test_docker_image_build_and_check(
            scenario,
            connector_image_override="airbyte/source-test:dev",
            connector_base_image_override=None,
        )

    if expected_exception is None:
        _run()
    else:
        with pytest.raises(expected_exception, match=failure_match):
            _run()

    assert len(recorded_calls) == 1
    assert recorded_calls[0]["cmd"][-4:-1] == ["airbyte/source-test:dev", "check", "--config"]
    # Only `failed` and `exception` scenarios may let a trace error through to the assertion.
    assert recorded_calls[0]["raise_if_errors"] is (not expected_outcome.expect_exception())
