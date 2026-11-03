# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""The secret leak check runs on `discover` and `read` output, before anything prints it.

- In-process, `test_discover`, `test_basic_read` (its `discover` and its `read`) and
  `test_fail_read_with_bad_catalog` fail with the config paths of the leaked values, never the
  values, before an outcome assertion prints the raw error traces.
- On the Docker path, `discover` and `read` run with `raise_if_errors=False`; the leak check runs
  first, then the errors are raised.
- The traceback that `entrypoint_wrapper._run_command` prints for an unexpected error is masked.
- The spec is run once per test, however many commands the test runs.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Mapping, MutableMapping
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
    SyncMode,
    Type,
)
from airbyte_cdk.sources.source import Source
from airbyte_cdk.test.entrypoint_wrapper import AirbyteEntrypointException
from airbyte_cdk.test.standard_tests import connector_base
from airbyte_cdk.test.standard_tests.docker_base import DockerConnectorTestSuite
from airbyte_cdk.test.standard_tests.source_base import SourceTestSuiteBase
from unit_tests.test.standard_test_stubs import (
    catalog_message,
    entry,
    record_message,
    run_without_skip,
    scenario,
    stream_json,
    stub_docker,
    suite_class,
)

SECRET = "sk_live_51UWRAsFuIbeygfIY3"

ENCODED_SECRET = "p@ss/w0rd&123"
"""A secret whose URL-encoded form differs from it, so the CDK's runtime filter, which masks only
the literal value, lets the encoded form through. The leak check finds it."""

_SPEC = ConnectorSpecification(
    connectionSpecification={
        "type": "object",
        "properties": {"api_key": {"type": "string", "airbyte_secret": True}},
    }
)


def _url_with(api_key: str) -> str:
    return f"https://api.example.com/v1/users?key={quote(api_key, safe='')}"


class _Source(Source):
    """A source that logs a URL holding its encoded API key while running `leaks_in`."""

    leaks_in: str = ""

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return _SPEC

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        if self.leaks_in == "discover":
            logger.warning("Retrying %s", _url_with(config["api_key"]))
        return AirbyteCatalog(
            streams=[
                AirbyteStream(
                    name="users",
                    json_schema={"type": "object"},
                    supported_sync_modes=[SyncMode.full_refresh],
                )
            ]
        )

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Any = None,
    ) -> Iterator[AirbyteMessage]:
        if self.leaks_in == "read":
            logger.warning("Retrying %s", _url_with(config["api_key"]))
        yield AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(stream="users", data={"id": 1}, emitted_at=0),
        )


def _source(leaks_in: str) -> type[_Source]:
    return type(f"_Leaks_{leaks_in}", (_Source,), {"leaks_in": leaks_in})


class _ConfigLoggingSource(_Source):
    """Logs the encoded API key while loading the config, before any command runs."""

    @staticmethod
    def read_config(config_path: str) -> MutableMapping[str, Any]:
        config = Source.read_config(config_path)
        logging.getLogger("airbyte").info("Loaded config for %s", _url_with(config["api_key"]))
        return config


class _RaisingSource(_Source):
    """Fails `discover` as the TikTok source does on a 403: the error quotes the request URL."""

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        raise RuntimeError(
            f"'GET' request to 'https://api.example.com/oauth2/advertiser/get/?secret="
            f"{config['api_key']}' failed with status code '403'"
        )


def _suite(tmp_path: Path, source: type[Source]) -> SourceTestSuiteBase:
    return suite_class(
        tmp_path,
        {"full_refresh": {"tests": [entry("secrets/config.json")]}},
        base=SourceTestSuiteBase,
        make_connector=source,
    )()


def _assert_leak_reported(error: pytest.ExceptionInfo[AssertionError], verb: str) -> None:
    text = str(error.value)
    assert f"`{verb}` for connector 'source-test' printed secret values" in text
    assert "`/api_key` in message #" in text
    for secret in (SECRET, ENCODED_SECRET, quote(ENCODED_SECRET, safe="")):
        assert secret not in text


# --- In-process --------------------------------------------------------------------------------


def test_in_process_discover_test_fails_when_discover_prints_a_secret(tmp_path: Path) -> None:
    suite = _suite(tmp_path, _source("discover"))
    with pytest.raises(AssertionError) as error:
        suite.test_discover(scenario("discovery", config={"api_key": ENCODED_SECRET}))
    _assert_leak_reported(error, "discover")


def test_in_process_discover_test_passes_without_a_leak(tmp_path: Path) -> None:
    suite = _suite(tmp_path, _source("read"))
    run_without_skip(suite.test_discover, scenario("discovery", config={"api_key": ENCODED_SECRET}))


def test_in_process_discover_error_is_checked_before_it_is_raised(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The error trace quotes the URL with the secret; the leak check fails before it is raised.

    The traceback that `_run_command` prints for the unexpected error is masked as well.
    """
    suite = _suite(tmp_path, _RaisingSource)
    with pytest.raises(AssertionError) as error:
        suite.test_discover(scenario("discovery", config={"api_key": SECRET}))
    _assert_leak_reported(error, "discover")

    printed = capsys.readouterr()
    assert "Printing unexpected error from entrypoint_wrapper" in printed.out
    assert "secret=****' failed with status code '403'" in printed.out
    assert SECRET not in printed.out + printed.err


@pytest.mark.parametrize("verb", ["discover", "read"])
def test_in_process_basic_read_test_fails_when_a_command_prints_a_secret(
    tmp_path: Path, verb: str
) -> None:
    suite = _suite(tmp_path, _source(verb))
    with pytest.raises(AssertionError) as error:
        suite.test_basic_read(scenario("full_refresh", config={"api_key": ENCODED_SECRET}))
    _assert_leak_reported(error, verb)


def test_in_process_basic_read_test_passes_without_a_leak(tmp_path: Path) -> None:
    suite = _suite(tmp_path, _source(""))
    run_without_skip(
        suite.test_basic_read, scenario("full_refresh", config={"api_key": ENCODED_SECRET})
    )


def test_in_process_bad_catalog_test_fails_when_read_prints_a_secret(tmp_path: Path) -> None:
    suite = _suite(tmp_path, _ConfigLoggingSource)
    with pytest.raises(AssertionError) as error:
        suite.test_fail_read_with_bad_catalog(
            scenario("full_refresh", config={"api_key": ENCODED_SECRET})
        )
    _assert_leak_reported(error, "read")


def test_in_process_leak_check_runs_spec_once_per_test(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec_runs: list[str] = []
    run_test_job = connector_base.run_test_job

    def counting_run_test_job(connector: Any, verb: Any, **kwargs: Any) -> Any:
        if verb == "spec":
            spec_runs.append(verb)
        return run_test_job(connector, verb, **kwargs)

    monkeypatch.setattr(connector_base, "run_test_job", counting_run_test_job)
    suite = _suite(tmp_path, _source(""))
    run_without_skip(
        suite.test_basic_read, scenario("full_refresh", config={"api_key": ENCODED_SECRET})
    )
    assert spec_runs == ["spec"], "`discover` and `read` share the spec"

    # A new test gets a new suite instance, which runs the spec again.
    run_without_skip(
        _suite(tmp_path, _source("")).test_discover,
        scenario("discovery", config={"api_key": ENCODED_SECRET}),
    )
    assert spec_runs == ["spec", "spec"]


# --- Docker ------------------------------------------------------------------------------------

_DOCKER_OPTIONS: dict[str, Any] = {
    "connector_image_override": "airbyte/source-test:dev",
    "connector_base_image_override": None,
}
_DOCKER_READ_OPTIONS = _DOCKER_OPTIONS | {"read_from_streams": "default", "read_scenarios": "all"}

_DISCOVER = [catalog_message(stream_json("users", ["full_refresh"]))]
_READ = [record_message("users")]


def _log(message: str) -> str:
    return json.dumps({"type": "LOG", "log": {"level": "WARN", "message": message}})


def _error_trace(message: str) -> str:
    return json.dumps(
        {
            "type": "TRACE",
            "trace": {
                "type": "ERROR",
                "emitted_at": 0,
                "error": {"message": "Request failed", "internal_message": message},
            },
        }
    )


def _docker_suite(tmp_path: Path) -> DockerConnectorTestSuite:
    return suite_class(
        tmp_path,
        {"full_refresh": {"tests": [entry("secrets/config.json")]}},
        base=DockerConnectorTestSuite,
    )()


def _run_docker_discover(tmp_path: Path) -> None:
    run_without_skip(
        _docker_suite(tmp_path).test_docker_image_build_and_discover,
        scenario("discovery", config={"api_key": SECRET}),
        **_DOCKER_OPTIONS,
    )


def _run_docker_read(tmp_path: Path) -> None:
    run_without_skip(
        _docker_suite(tmp_path).test_docker_image_build_and_read,
        scenario("full_refresh", config={"api_key": SECRET}),
        **_DOCKER_READ_OPTIONS,
    )


@pytest.mark.parametrize(
    "discover_output",
    [
        pytest.param([_log(f"Retrying {_url_with(SECRET)}"), *_DISCOVER], id="log"),
        pytest.param([_error_trace(f"403 for {_url_with(SECRET)}")], id="error_trace"),
    ],
)
def test_docker_discover_test_checks_for_leaks_before_raising(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, discover_output: list[str]
) -> None:
    stub_docker(monkeypatch, {"discover": discover_output})
    with pytest.raises(AssertionError) as error:
        _run_docker_discover(tmp_path)
    _assert_leak_reported(error, "discover")


def test_docker_discover_test_still_raises_errors_without_a_leak(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_docker(monkeypatch, {"discover": [_error_trace("403 Forbidden"), *_DISCOVER]})
    with pytest.raises(AirbyteEntrypointException, match="403 Forbidden"):
        _run_docker_discover(tmp_path)


@pytest.mark.parametrize(
    "responses, verb",
    [
        pytest.param(
            {"discover": [_log(f"Retrying {_url_with(SECRET)}"), *_DISCOVER], "read": _READ},
            "discover",
            id="discover_log",
        ),
        pytest.param(
            {"discover": [_error_trace(f"403 for {_url_with(SECRET)}")], "read": _READ},
            "discover",
            id="discover_error_trace",
        ),
        pytest.param(
            {"discover": _DISCOVER, "read": [_log(f"Retrying {_url_with(SECRET)}"), *_READ]},
            "read",
            id="read_log",
        ),
        pytest.param(
            {"discover": _DISCOVER, "read": [*_READ, _error_trace(f"403 for {SECRET}")]},
            "read",
            id="read_error_trace",
        ),
    ],
)
def test_docker_read_test_checks_for_leaks_before_raising(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, responses: dict[str, list[str]], verb: str
) -> None:
    stub_docker(monkeypatch, responses)
    with pytest.raises(AssertionError) as error:
        _run_docker_read(tmp_path)
    _assert_leak_reported(error, verb)


@pytest.mark.parametrize(
    "responses",
    [
        pytest.param({"discover": [_error_trace("403 Forbidden"), *_DISCOVER]}, id="discover"),
        pytest.param(
            {"discover": _DISCOVER, "read": [*_READ, _error_trace("403 Forbidden")]}, id="read"
        ),
    ],
)
def test_docker_read_test_still_raises_errors_without_a_leak(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, responses: dict[str, list[str]]
) -> None:
    stub_docker(monkeypatch, responses)
    with pytest.raises(AirbyteEntrypointException, match="403 Forbidden"):
        _run_docker_read(tmp_path)


def test_docker_read_test_does_not_search_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A record that holds a secret's value is data, not a leak."""
    record = json.dumps(
        {
            "type": "RECORD",
            "record": {"stream": "users", "data": {"key": SECRET}, "emitted_at": 0},
        }
    )
    stub_docker(monkeypatch, {"discover": _DISCOVER, "read": [record]})
    _run_docker_read(tmp_path)
