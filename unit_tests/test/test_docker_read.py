# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""The Docker read test runs when turned on and checks its records as `test_basic_read` does.

- `--read-from-streams` defaults to `false`, which skips the Docker read; `default` turns it on.
- `--read-scenarios=default` reads the `config`, `valid_config` and `default` configs, or the first
  read config of a connector that has none of those, so it never skips every read.
- `--read-from-streams=default` reads the streams `test_basic_read` reads; `suggestedStreams` play
  no part.
- A `basic_read` config must return records from every stream it reads, and the records must
  match their schemas; a `full_refresh`-only config must return some records.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from airbyte_cdk.test.models.scenario import ConnectorTestScenario
from airbyte_cdk.test.standard_tests._read_assertions import StreamStatusWarning
from airbyte_cdk.test.standard_tests.docker_base import (
    DockerConnectorTestSuite,
    get_default_read_scenario_ids,
)
from unit_tests.test.test_scenario_sections import (
    _catalog_message,
    _entry,
    _record_message,
    _scenario,
    _state_message,
    _stream_json,
    _stub_docker,
    _suite_class,
)

pytest_plugins = ["pytester"]

DEFAULTS: dict[str, Any] = {
    "connector_image_override": "airbyte/source-test:dev",  # skips the image build
    "connector_base_image_override": None,
    "read_from_streams": "default",
    "read_scenarios": "default",
}

DISCOVER = [
    _catalog_message(
        _stream_json("users", ["full_refresh", "incremental"], ["updated_at"]),
        _stream_json("events", ["full_refresh"]),
    )
]


def _read_output(*streams: str) -> list[str]:
    """A read's output: one record and then the final state for each of `streams`."""
    return [
        message
        for stream in streams
        for message in (_record_message(stream), _state_message(stream))
    ]


def _read_suite(
    tmp_path: Path,
    acceptance_tests: dict[str, Any] | None = None,
    *,
    suggested_streams: list[str] | None = None,
) -> DockerConnectorTestSuite:
    suite_class = _suite_class(
        tmp_path,
        acceptance_tests
        if acceptance_tests is not None
        else {"basic_read": {"tests": [_entry("secrets/config.json")]}},
        base=DockerConnectorTestSuite,
    )
    if suggested_streams is not None:
        metadata_path = tmp_path / "source-test" / "metadata.yaml"
        metadata = yaml.safe_load(metadata_path.read_text())
        metadata["data"]["suggestedStreams"] = {"streams": suggested_streams}
        metadata_path.write_text(yaml.safe_dump(metadata))
    return suite_class()


def _read(suite: DockerConnectorTestSuite, scenario: ConnectorTestScenario, **options: Any) -> None:
    suite.test_docker_image_build_and_read(scenario, **(DEFAULTS | options))


def _read_commands(calls: list[list[str]]) -> list[list[str]]:
    return [cmd for cmd in calls if "read" in cmd]


# --- Options -----------------------------------------------------------------------------------

OPTIONS_TEST = """
import json

def test_options(read_from_streams, read_scenarios):
    print("OPTIONS=" + json.dumps([read_from_streams, read_scenarios]))
"""


@pytest.mark.parametrize(
    "args, expected",
    [
        pytest.param([], ["none", "default"], id="omitted-skips-the-read"),
        pytest.param(["--read-from-streams=default"], ["default", "default"], id="default"),
        pytest.param(["--read-from-streams=none"], ["none", "default"], id="none"),
        pytest.param(["--read-from-streams=False"], ["none", "default"], id="false"),
        pytest.param(["--read-from-streams=true"], ["default", "default"], id="true"),
        pytest.param(["--read-from-streams=suggested"], ["default", "default"], id="suggested"),
        pytest.param(["--read-from-streams=all"], ["default", "default"], id="all"),
        pytest.param(["--read-from-streams=users, events"], [["users", "events"], "default"]),
        pytest.param(["--read-scenarios=none"], ["none", "none"], id="scenarios-none"),
        pytest.param(["--read-scenarios=ALL"], ["none", "all"], id="scenarios-all"),
        pytest.param(
            ["--read-scenarios=Config.json,config_oauth"],
            ["none", ["config", "config_oauth"]],
            id="scenarios-list",
        ),
    ],
)
def test_read_options(pytester: pytest.Pytester, args: list[str], expected: list[Any]) -> None:
    """The `addoption` defaults, the fixtures and their docstrings agree: omitted means no read."""
    pytester.makepyfile(test_options=OPTIONS_TEST)
    result = pytester.runpytest_inprocess(
        "-p", "airbyte_cdk.test.standard_tests.pytest_hooks", "-s", *args
    )
    result.assert_outcomes(passed=1)
    match = re.search(r"OPTIONS=(.*)$", result.stdout.str(), re.MULTILINE)
    assert match is not None, result.stdout.str()
    assert json.loads(match.group(1)) == expected


# --- Opt-outs and config selection -------------------------------------------------------------


def test_docker_read_runs_when_turned_on(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _stub_docker(
        monkeypatch,
        {"discover": DISCOVER, "read": _read_output("users", "events")},
    )
    _read(_read_suite(tmp_path), _scenario("basic_read"))
    assert len(_read_commands(calls)) == 1


@pytest.mark.parametrize(
    "options, reason",
    [
        pytest.param({"read_from_streams": "none"}, "--read-from-streams", id="read-from-streams"),
        pytest.param({"read_scenarios": "none"}, "--read-scenarios", id="read-scenarios"),
    ],
)
def test_docker_read_opt_outs_skip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, options: dict[str, Any], reason: str
) -> None:
    calls = _stub_docker(monkeypatch, {})
    with pytest.raises(pytest.skip.Exception, match=f"`{reason}`"):
        _read(_read_suite(tmp_path), _scenario("basic_read"), **options)
    assert calls == []


def test_docker_read_scenario_list_skips_other_configs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A config missing from `--read-scenarios` is skipped; it used to raise a `ValueError`."""
    calls = _stub_docker(
        monkeypatch,
        {"discover": DISCOVER, "read": _read_output("users", "events")},
    )
    suite = _read_suite(tmp_path)
    with pytest.raises(pytest.skip.Exception, match=r"not in `--read-scenarios=config_oauth`"):
        _read(suite, _scenario("basic_read"), read_scenarios=["config_oauth"])
    assert calls == []

    _read(suite, _scenario("basic_read", name="Config_OAuth"), read_scenarios=["config_oauth"])
    assert len(_read_commands(calls)) == 1


@pytest.mark.parametrize(
    "configs, expected",
    [
        pytest.param(
            ["secrets/config_oauth.json", "secrets/config.json", "secrets/valid_config.json"],
            {"config", "valid_config"},
            id="named-defaults",
        ),
        pytest.param(
            ["secrets/config_oauth.json", "secrets/config_token.json"],
            {"config_oauth"},
            id="first-read-config-when-none-is-named-default",
        ),
    ],
)
def test_default_read_scenarios(tmp_path: Path, configs: list[str], expected: set[str]) -> None:
    suite = _read_suite(
        tmp_path,
        {
            # A `connection`-only config is never read, so it is never the fallback.
            "connection": {"tests": [_entry("secrets/check_only.json")]},
            "basic_read": {"tests": [_entry(config) for config in configs]},
        },
    )
    assert get_default_read_scenario_ids(suite.get_scenarios()) == expected


def test_default_read_scenarios_never_pick_an_expected_failure() -> None:
    scenarios = [
        _scenario("basic_read", name="bad", status="failed"),
        _scenario("basic_read", name="config_oauth"),
    ]
    assert get_default_read_scenario_ids(scenarios) == {"config_oauth"}
    assert get_default_read_scenario_ids([_scenario("connection")]) == set()


def test_docker_read_defaults_read_a_connector_without_a_default_named_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`--read-scenarios=default` used to skip every read of such a connector."""
    calls = _stub_docker(
        monkeypatch,
        {"discover": DISCOVER, "read": _read_output("users", "events")},
    )
    suite = _read_suite(
        tmp_path,
        {
            "basic_read": {
                "tests": [_entry("secrets/config_oauth.json"), _entry("secrets/config_token.json")]
            }
        },
    )
    _read(suite, _scenario("basic_read", name="config_oauth"))
    with pytest.raises(
        pytest.skip.Exception, match=r"`--read-scenarios=default` reads 'config_oauth'"
    ):
        _read(suite, _scenario("basic_read", name="config_token"))
    assert len(_read_commands(calls)) == 1


# --- Streams -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "suggested, read_from_streams, expected",
    [
        pytest.param(None, "default", ["users", "events"], id="no-suggested-streams"),
        pytest.param(["events"], "default", ["users", "events"], id="suggested-streams-ignored"),
        pytest.param(["events"], ["users"], ["users"], id="stream-list"),
    ],
)
def test_docker_read_streams(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suggested: list[str] | None,
    read_from_streams: Any,
    expected: list[str],
) -> None:
    catalogs: list[dict[str, Any]] = []
    _stub_docker(
        monkeypatch,
        {"discover": DISCOVER, "read": _read_output("users", "events")},
        read_catalogs=catalogs,
    )
    suite = _read_suite(tmp_path, suggested_streams=suggested)
    _read(suite, _scenario("basic_read"), read_from_streams=read_from_streams)
    assert [stream["stream"]["name"] for stream in catalogs[-1]["streams"]] == expected


def test_docker_read_skips_when_the_stream_list_matches_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _stub_docker(monkeypatch, {"discover": DISCOVER})
    with pytest.raises(pytest.skip.Exception, match=r"`--read-from-streams=other,more`"):
        _read(_read_suite(tmp_path), _scenario("basic_read"), read_from_streams=["other", "more"])
    assert _read_commands(calls) == []


# --- Record checks -----------------------------------------------------------------------------


def test_docker_read_requires_records_from_every_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_docker(monkeypatch, {"discover": DISCOVER, "read": _read_output("users")})
    suite = _read_suite(tmp_path)

    with pytest.raises(AssertionError, match=r"these returned none: 'events'"):
        _read(suite, _scenario("basic_read"))

    # A stream declared in `empty_streams` is not read, so it needs no records.
    _read(
        suite,
        _scenario("basic_read", empty_streams=[{"name": "events", "bypass_reason": "no data"}]),
    )


def test_docker_read_of_a_full_refresh_only_config_needs_some_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    responses = {"discover": DISCOVER, "read": _read_output("users")}
    _stub_docker(monkeypatch, responses)
    suite = _read_suite(tmp_path, {"full_refresh": {"tests": [_entry("secrets/config.json")]}})

    _read(suite, _scenario("full_refresh"))

    responses["read"] = [json.dumps({"type": "LOG", "log": {"level": "INFO", "message": "hi"}})]
    with pytest.raises(AssertionError, match="Expected records but got none."):
        _read(suite, _scenario("full_refresh"))


def _record_with(stream: str, data: dict[str, Any]) -> str:
    return json.dumps(
        {"type": "RECORD", "record": {"stream": stream, "data": data, "emitted_at": 0}}
    )


STRICT_DISCOVER = [
    _catalog_message(
        {
            "name": "users",
            "json_schema": {"type": "object", "properties": {"id": {"type": "string"}}},
            "supported_sync_modes": ["full_refresh"],
        }
    )
]


def test_docker_read_validates_records_against_their_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_docker(
        monkeypatch,
        {
            "discover": STRICT_DISCOVER,
            "read": [_record_with("users", {"id": 31337}), _state_message("users")],
        },
    )
    suite = _read_suite(tmp_path)

    with pytest.raises(AssertionError, match=r"do not match the stream's JSON schema") as exc_info:
        _read(suite, _scenario("basic_read"))
    message = str(exc_info.value)
    assert "Stream 'users'" in message and "`$.id`" in message
    assert "31337" not in message, "record values must never reach the failure message"

    # The opt-out is honoured...
    _read(suite, _scenario("basic_read", validate_schema=False))

    # ...but not at high strictness, where it fails before anything runs in Docker.
    calls = _stub_docker(monkeypatch, {})
    with pytest.raises(AssertionError, match=r"`validate_schema: false` is not allowed"):
        _read(
            suite,
            _scenario("basic_read", validate_schema=False, test_strictness_level="high"),
        )
    assert calls == []


def test_docker_read_runs_in_a_writable_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The platform gives a connector a writable working directory; the image's own is not."""
    calls = _stub_docker(
        monkeypatch,
        {"discover": DISCOVER, "read": _read_output("users", "events")},
    )
    _read(_read_suite(tmp_path), _scenario("basic_read"))
    assert [cmd[3:7] for cmd in calls] == [
        ["--tmpfs", "/source:rw,exec,mode=1777", "--workdir", "/source"]
    ] * 2, "both `discover` and `read`"


KEYED_DISCOVER = [
    _catalog_message(
        {
            "name": "users",
            "json_schema": {"type": "object"},
            "supported_sync_modes": ["full_refresh"],
            "source_defined_primary_key": [["id"]],
        }
    )
]


def test_docker_read_checks_primary_keys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_docker(
        monkeypatch,
        {
            "discover": KEYED_DISCOVER,
            "read": [_record_with("users", {"id": {"token": "SENTINEL"}}), _state_message("users")],
        },
    )
    suite = _read_suite(tmp_path)

    with pytest.raises(AssertionError, match="Records have invalid primary keys") as exc_info:
        _read(suite, _scenario("basic_read"))
    assert "SENTINEL" not in str(exc_info.value), "key values must never reach the message"

    _read(suite, _scenario("basic_read", validate_primary_keys_data_type=False))


def test_docker_read_checks_state_messages(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_docker(
        monkeypatch,
        {"discover": DISCOVER, "read": [_record_message("users"), _record_message("events")]},
    )
    suite = _read_suite(tmp_path)

    with pytest.raises(AssertionError, match="The read's state messages are incomplete"):
        _read(suite, _scenario("basic_read"))

    _read(suite, _scenario("basic_read", validate_state_messages=False))


def test_docker_read_warns_on_missing_stream_statuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_docker(monkeypatch, {"discover": DISCOVER, "read": _read_output("users", "events")})

    with pytest.warns(StreamStatusWarning, match="reported no stream status"):
        _read(_read_suite(tmp_path), _scenario("basic_read"))
