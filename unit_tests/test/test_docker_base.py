# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for `DockerConnectorTestSuite._assert_check_result_matches_expected_outcome`.

These tests cover the `CONNECTION_STATUS` validation added to
`test_docker_image_build_and_check` (see airbyte-internal-issues#16212). They
construct `EntrypointOutput` objects directly with synthetic messages rather
than invoking Docker, which lets us exercise the assertion logic in isolation
without requiring the Docker CLI.
"""

from __future__ import annotations

import json
from contextlib import AbstractContextManager, nullcontext

import pytest

from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite


def _connection_status_message(status: str, message: str | None = None) -> str:
    payload: dict[str, object] = {"status": status}
    if message is not None:
        payload["message"] = message
    return json.dumps({"type": "CONNECTION_STATUS", "connectionStatus": payload})


_SUCCEEDED = _connection_status_message("SUCCEEDED")
_FAILED = _connection_status_message("FAILED", "bad credentials")


@pytest.mark.parametrize(
    ("scenario_status", "messages", "expectation"),
    [
        pytest.param(
            "succeed",
            [_SUCCEEDED],
            nullcontext(),
            id="succeed_scenario_with_succeeded_status_passes",
        ),
        pytest.param(
            "succeed",
            [_FAILED],
            pytest.raises(AssertionError, match="SUCCEEDED"),
            id="succeed_scenario_with_failed_status_raises__gap_1",
        ),
        pytest.param(
            "failed",
            [_FAILED],
            nullcontext(),
            id="failed_scenario_with_failed_status_passes__gap_2",
        ),
        pytest.param(
            "failed",
            [_SUCCEEDED],
            pytest.raises(AssertionError, match="FAILED"),
            id="failed_scenario_with_succeeded_status_raises",
        ),
        pytest.param(
            "exception",
            [_FAILED],
            nullcontext(),
            id="exception_scenario_with_failed_status_passes",
        ),
        pytest.param(
            None,
            [_SUCCEEDED],
            nullcontext(),
            id="no_expectation_accepts_succeeded",
        ),
        pytest.param(
            None,
            [_FAILED],
            nullcontext(),
            id="no_expectation_accepts_failed",
        ),
        pytest.param(
            "succeed",
            [],
            pytest.raises(AssertionError, match="Expected exactly one CONNECTION_STATUS"),
            id="missing_connection_status_message_raises",
        ),
        pytest.param(
            "succeed",
            [_SUCCEEDED, _SUCCEEDED],
            pytest.raises(AssertionError, match="Expected exactly one CONNECTION_STATUS"),
            id="multiple_connection_status_messages_raises",
        ),
    ],
)
def test_assert_check_result_matches_expected_outcome(
    scenario_status: str | None,
    messages: list[str],
    expectation: AbstractContextManager[object],
) -> None:
    output = EntrypointOutput(messages=messages)
    scenario = ConnectorTestScenario(status=scenario_status)

    with expectation:
        DockerConnectorTestSuite._assert_check_result_matches_expected_outcome(
            output, scenario
        )
