# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base class for connector test suites."""

from __future__ import annotations

import inspect
import shutil
import sys
import tempfile
import warnings
from dataclasses import asdict
from pathlib import Path
from typing import Any, Literal, cast

import orjson
import pytest
import requests
import yaml
from boltons.typeutils import classproperty

from airbyte_cdk.models import (
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
    DestinationSyncMode,
    SyncMode,
)
from airbyte_cdk.models.connector_metadata import MetadataFile
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.models.scenario import ScenarioCommand
from airbyte_cdk.test.standard_tests._acceptance_test_config import (
    JAVA_BYPASS_REASON,
    CollectedScenarios,
    ScenarioBypass,
    collect_scenarios,
    is_java_connector,
    load_acceptance_test_config,
)
from airbyte_cdk.test.standard_tests._assertions import (
    assert_check_outcome,
    assert_config_matches_spec,
)
from airbyte_cdk.test.standard_tests._read_assertions import (
    StreamStatusWarning,
    assert_read_records,
    assert_schema_validation_opt_out_allowed,
    find_stream_status_problems,
)
from airbyte_cdk.test.standard_tests._spec_compatibility import (
    DEPLOYMENT_MODE_ENV_VARS,
    REGISTRY_UNAVAILABLE_ERRORS,
    DeploymentMode,
    compare_specs,
    declared_breaking_changes,
    disabled_for_version,
    fetch_published_spec,
    format_breaking_spec_changes,
    format_spec_change_summary,
    is_newer_version,
)
from airbyte_cdk.test.standard_tests._spec_lint import (
    assert_no_secrets_in_output,
    assert_spec_is_valid,
    get_single_spec,
)
from airbyte_cdk.utils.connector_paths import (
    ACCEPTANCE_TEST_CONFIG,
    find_connector_root,
)
from airbyte_cdk.utils.docker import (
    build_connector_image,
    run_docker_airbyte_command,
    run_docker_command,
)


def skip_unless_runs(scenario: ConnectorTestScenario, *commands: ScenarioCommand) -> None:
    """Skip the current test unless one of `commands` runs for `scenario`.

    A config runs the command of every `acceptance-test-config.yml` section that lists it; see
    `ConnectorTestScenario.commands`.
    """
    if scenario.runs(*commands):
        return

    wanted = ", ".join(f"`{command}`" for command in commands)
    verb = "does not run" if len(commands) == 1 else "do not run"
    pytest.skip(
        f"{wanted} {verb} for scenario '{scenario.id}': its config is listed under "
        f"{', '.join(f'`{section}`' for section in scenario.sections)} in "
        "`acceptance-test-config.yml`, which runs "
        f"{', '.join(f'`{command}`' for command in sorted(scenario.commands))}."
    )


def get_discovered_streams(
    discover_result: EntrypointOutput,
    connector_name: str,
) -> list[AirbyteStream]:
    """Return the streams of a `discover` result, failing on a missing or empty catalog."""
    catalog_message = discover_result.catalog
    assert catalog_message.catalog is not None, "Catalog message missing catalog."
    if not catalog_message.catalog.streams:
        raise ValueError(
            f"Discovered catalog for connector '{connector_name}' is empty. "
            "Please check the connector's discover implementation."
        )
    return list(catalog_message.catalog.streams)


def get_read_streams(
    scenario: ConnectorTestScenario,
    discovered_streams: list[AirbyteStream],
    connector_root: Path,
) -> list[AirbyteStream]:
    """Return the streams that the `read` of `scenario` configures, minus its `empty_streams`.

    When the scenario declares a `configured_catalog_path`, the streams named in that catalog are
    read, in its order, as the Connector Acceptance Tests did: the catalog picks the streams, and
    each stream keeps the definition (and JSON schema) that `discover` reported. A stream of the
    catalog that `discover` does not report fails the test. Without a declared catalog, every
    discovered stream is read.
    """
    streams = discovered_streams
    if scenario.configured_catalog_path:
        catalog_path = scenario.configured_catalog_path
        if not catalog_path.is_absolute():
            catalog_path = connector_root / catalog_path
        if not catalog_path.is_file():
            raise AssertionError(
                f"The configured catalog `{scenario.configured_catalog_path}` of config "
                f"'{scenario.id}' does not exist in connector "
                f"'{connector_root.absolute().name}'. Fix its `configured_catalog_path`, or remove "
                "it to read every discovered stream."
            )
        declared_catalog = orjson.loads(catalog_path.read_bytes())
        # A stream listed twice in the catalog is read once.
        declared_names = list(
            dict.fromkeys(
                configured["stream"]["name"] for configured in declared_catalog["streams"]
            )
        )
        discovered_by_name = {stream.name: stream for stream in discovered_streams}
        undiscovered = [name for name in declared_names if name not in discovered_by_name]
        if undiscovered:
            raise AssertionError(
                f"The configured catalog `{scenario.configured_catalog_path}` of config "
                f"'{scenario.id}' lists streams that connector '{connector_root.absolute().name}' "
                f"does not discover: {', '.join(repr(name) for name in undiscovered)}. Remove "
                "them from the catalog, or point `configured_catalog_path` at a current catalog."
            )
        streams = [discovered_by_name[name] for name in declared_names]

    if scenario.empty_streams:
        empty_stream_names = {stream.name for stream in scenario.empty_streams}
        streams = [stream for stream in streams if stream.name not in empty_stream_names]

    return streams


DEFAULT_READ_SCENARIO_IDS: tuple[str, ...] = ("config", "valid_config", "default")
"""The config ids (file names without `.json`) that `--read-scenarios=default` reads."""


def get_default_read_scenario_ids(scenarios: list[ConnectorTestScenario]) -> set[str]:
    """Return the ids of the configs that the Docker read test reads under `--read-scenarios=default`.

    These are the read configs named in `DEFAULT_READ_SCENARIO_IDS`. A connector that names none of
    its read configs that way reads its first `basic_read` config instead, or its first read
    config when it has no `basic_read` config, so the default never skips every Docker read of a
    connector. When none of the picked configs is listed under `basic_read` but another read config
    is, the first `basic_read` config is read too, so the read gets the `basic_read` checks.
    Configs expected to fail are never picked.
    """
    read_scenarios = [
        scenario
        for scenario in scenarios
        if scenario.runs("read") and not scenario.expected_outcome.expect_exception()
    ]
    read_ids = [scenario.id for scenario in read_scenarios]
    basic_read_ids = [scenario.id for scenario in read_scenarios if scenario.is_basic_read_config]
    picked = {
        scenario_id for scenario_id in read_ids if scenario_id in DEFAULT_READ_SCENARIO_IDS
    } or set((basic_read_ids or read_ids)[:1])
    if basic_read_ids and picked.isdisjoint(basic_read_ids):
        picked.add(basic_read_ids[0])
    return picked


def fail_on_unknown_read_scenarios(
    read_scenarios: list[str],
    scenarios: list[ConnectorTestScenario],
) -> None:
    """Fail when `--read-scenarios` names a config that is not one of the connector's read configs.

    The names were lowercased by the `read_scenarios` fixture, and match config ids in any case.
    Without this check a misspelled name would skip every Docker read, and the run would pass.
    """
    read_ids = sorted({scenario.id for scenario in scenarios if scenario.runs("read")})
    known = {scenario_id.lower() for scenario_id in read_ids}
    unknown = [name for name in read_scenarios if name not in known]
    if unknown:
        pytest.fail(
            f"`--read-scenarios={','.join(read_scenarios)}` names configs that are not listed "
            f"under `basic_read` or `full_refresh` in `acceptance-test-config.yml`: "
            f"{', '.join(map(repr, unknown))}. The read configs are: "
            f"{', '.join(map(repr, read_ids)) or 'none'}."
        )


def fail_on_undiscovered_read_streams(
    read_from_streams: list[str],
    discovered_streams: list[AirbyteStream],
    *,
    scenario_id: str,
) -> None:
    """Fail when `--read-from-streams` names a stream that `discover` does not report for a config.

    Stream names match exactly, case included, as they do in a catalog. Without this check a
    misspelled name would skip every Docker read, and the run would pass.
    """
    discovered_names = sorted(stream.name for stream in discovered_streams)
    unknown = [name for name in read_from_streams if name not in set(discovered_names)]
    if unknown:
        pytest.fail(
            f"`--read-from-streams={','.join(read_from_streams)}` names streams that `discover` "
            f"does not report for config '{scenario_id}': {', '.join(map(repr, unknown))}. "
            "Stream names match exactly, case included. The discovered streams "
            f"are: {', '.join(map(repr, discovered_names)) or 'none'}. If only another config "
            "discovers the stream, pick that config with `--read-scenarios`."
        )


def select_docker_read_streams(
    read_streams: list[AirbyteStream],
    *,
    read_from_streams: Literal["default"] | list[str],
) -> list[AirbyteStream]:
    """Narrow a config's read streams by `--read-from-streams` (see the `read_from_streams` fixture).

    `default` keeps every stream that `get_read_streams` returns, the same streams the in-process
    `test_basic_read` reads. A stream list keeps the listed streams, which may leave none.
    """
    if isinstance(read_from_streams, list):
        wanted = set(read_from_streams)
        return [stream for stream in read_streams if stream.name in wanted]

    return read_streams


CONNECTOR_WORKDIR = "/source"
"""The connector's working directory in the Docker tests: the one the platform gives a source's
`read`. The platform runs `check` and `discover` in `/config`; any writable directory outside the
image serves those tests equally."""

CONNECTOR_WORKDIR_ARGS: tuple[str, ...] = (
    "--tmpfs",
    f"{CONNECTOR_WORKDIR}:rw,exec,mode=1777",
    "--workdir",
    CONNECTOR_WORKDIR,
)
"""`docker run` arguments that give the connector a writable working directory, as the platform does.

The platform runs a connector with its working directory on a writable volume outside the image.
The `WORKDIR` of a Python connector image is owned by root, not by the image's `airbyte` user, so
a connector that writes a temporary file to its working directory (a bulk export download, for
example) would fail only in the tests.

The directory is a `tmpfs`, so what the connector writes there uses memory, where the platform's
volume uses disk. A bind-mounted temporary directory would use disk, but the connector's `airbyte`
user would own what it writes there, and the test could not delete the subdirectories it creates.
"""


_SPEC_FAILURE_OUTPUT_LINES = 50
"""How many trailing lines of the connector's output a failed `spec` run reports."""


def _run_spec_in_image(connector_image: str, deployment_mode: DeploymentMode) -> dict[str, Any]:
    """Run `spec` in the image as the registry publisher does, and return the raw spec object.

    The raw JSON is compared rather than the parsed protocol model, because the registry stores
    the raw output and parsing would drop or add keys that are not in the model. If `spec` exits
    with an error or emits no SPEC message, the error names the exit code and includes what the
    connector reported: its last error TRACE message, its last log lines and its stderr.
    """
    env_args = [
        arg
        for name, value in DEPLOYMENT_MODE_ENV_VARS[deployment_mode].items()
        for arg in ("-e", f"{name}={value}")
    ]
    result = run_docker_command(
        ["docker", "run", "--rm", *env_args, connector_image, "spec"],
        capture_stdout=True,
        capture_stderr=True,
        raise_if_errors=False,
    )

    spec: dict[str, Any] | None = None
    error_trace: dict[str, Any] | None = None
    log_lines: list[str] = []
    for line in result.stdout.splitlines():
        try:
            message = orjson.loads(line)
        except orjson.JSONDecodeError:
            log_lines.append(line)
            continue
        if not isinstance(message, dict):
            continue
        if message.get("type") == "SPEC" and spec is None:
            spec = cast(dict[str, Any], message["spec"])
        elif message.get("type") == "LOG":
            log_lines.append(str((message.get("log") or {}).get("message", "")))
        elif message.get("type") == "TRACE" and (message.get("trace") or {}).get("error"):
            error_trace = message["trace"]["error"]

    if spec is not None and result.returncode == 0:
        return spec

    problem = (
        f"exited with code {result.returncode}"
        if result.returncode != 0
        else "emitted no SPEC message"
    )
    details = [
        f"`spec` in image '{connector_image}' ({deployment_mode} mode) {problem}.",
    ]
    if error_trace:
        details.append(f"Error reported by the connector: {error_trace.get('message')}")
        if error_trace.get("internal_message"):
            details.append(f"Internal message: {error_trace['internal_message']}")
        if error_trace.get("stack_trace"):
            details.append(f"Stack trace:\n{_tail(str(error_trace['stack_trace']))}")
    if log_lines:
        log_text = "\n".join(log_lines)
        details.append(f"Last log lines:\n{_tail(log_text)}")
    details.append(f"Stderr:\n{_tail(result.stderr or '') or '(empty)'}")
    raise AssertionError("\n".join(details))


def _tail(text: str) -> str:
    lines = text.rstrip().splitlines()
    if len(lines) <= _SPEC_FAILURE_OUTPUT_LINES:
        return "\n".join(lines)
    omitted = len(lines) - _SPEC_FAILURE_OUTPUT_LINES
    return "\n".join(
        [f"... ({omitted} earlier lines omitted)", *lines[-_SPEC_FAILURE_OUTPUT_LINES:]]
    )


_docker_spec_cache: dict[str, ConnectorSpecification | Exception] = {}
"""The spec, or the error from running `spec`, of each connector image in this test session."""


def _run_docker_spec(connector_image: str, *, connector_name: str) -> ConnectorSpecification:
    """Run `spec` in the connector image and return the single spec it emits.

    The outcome is cached per image for the test session, so the spec test and every check
    scenario share one `docker run`. A failure is cached too and raised again, so an image
    whose `spec` fails is not run again for each scenario. The image under a tag does not
    change within a session.
    """
    outcome = _docker_spec_cache.get(connector_image)
    if outcome is None:
        try:
            spec_result = run_docker_airbyte_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    connector_image,
                    "spec",
                ],
                raise_if_errors=True,
            )
            outcome = get_single_spec(spec_result, connector_name=connector_name)
        except Exception as error:  # `get_single_spec` raises `AssertionError`.
            outcome = error
        _docker_spec_cache[connector_image] = outcome
    if isinstance(outcome, Exception):
        raise outcome
    return outcome


_SPEC_FAILED_SKIP_REASON = (
    "`spec` failed, so the secret leak check cannot tell which config values are secret, and "
    "the check outcome is not asserted without it. The spec tests report the `spec` failure."
)

_READ_CHECK_OPT_OUTS = (
    "validate_schema",
    "validate_primary_keys_data_type",
    "validate_state_messages",
    "expect_trace_message_on_failure",
)
"""The `basic_read` opt-outs that a config keeps if any of its entries declares them `false`."""


class DockerConnectorTestSuite:
    """Base class for connector test suites."""

    @classmethod
    def get_test_class_dir(cls) -> Path:
        """Get the file path that contains the class."""
        module = sys.modules[cls.__module__]
        # Get the directory containing the test file
        return Path(inspect.getfile(module)).parent

    @classmethod
    def get_connector_root_dir(cls) -> Path:
        """Get the root directory of the connector."""
        return find_connector_root([cls.get_test_class_dir(), Path.cwd()])

    @classproperty
    def connector_name(self) -> str:
        """Get the name of the connector."""
        connector_root = self.get_connector_root_dir()
        return connector_root.absolute().name

    @classmethod
    def is_destination_connector(cls) -> bool:
        """Check if the connector is a destination."""
        return cast(str, cls.connector_name).startswith("destination-")

    @classproperty
    def acceptance_test_config(cls) -> Any:
        """Get the contents of acceptance test config file.

        Also perform some basic validation that the file has the expected structure: an
        `acceptance_tests` mapping, or only a top-level `bypass_reason`.

        Raises:
            MissingAcceptanceTestConfigError: The file does not exist (a `FileNotFoundError`).
            AcceptanceTestConfigError: The file has an unusable layout, such as the legacy
                top-level `tests:` key (a `ValueError`).
        """
        return load_acceptance_test_config(cls.get_connector_root_dir() / ACCEPTANCE_TEST_CONFIG)

    @staticmethod
    def _dedup_scenarios(scenarios: list[ConnectorTestScenario]) -> list[ConnectorTestScenario]:
        """
        For FAST tests, we treat each config as a separate test scenario to run against, whereas CATs defined
        a series of more granular scenarios specifying a config_path and empty_streams among other things.

        This method deduplicates the CATs scenarios based on their config_path. In doing so, we choose to
        take the union of any defined empty_streams, to have high confidence that runnning a read with the
        config will not error on the lack of data in the empty streams or lack of permissions to read them.

        We also carry over any explicitly declared `status`. Only the `connection` section declares one,
        so without this an entry from another section (e.g. `spec`) would win and silently drop the
        declared expectation. Conflicting non-null statuses for the same config raise a `ValueError`.

        The sections a config is listed under are unioned as well, so a config that appears under
        `discovery` and `basic_read` runs both `discover` and `read` (see
        `ConnectorTestScenario.commands`).

        An opt-out (`validate_schema`, `validate_primary_keys_data_type`,
        `validate_state_messages` or `expect_trace_message_on_failure` set to `false`) declared
        by any entry for the config is kept.

        A config is read once, however many read sections (`basic_read`, `full_refresh`) list it.
        It keeps a `configured_catalog_path` only when every read entry for it declares that same
        catalog. If one of them declares none, or they declare different catalogs, the merged
        scenario declares none, and every discovered stream is read: a superset of what each
        entry asked for.
        """
        read_catalogs: dict[Path | None, set[Path | None]] = {}
        for scenario in scenarios:
            if scenario.runs("read"):
                read_catalogs.setdefault(scenario.config_path, set()).add(
                    scenario.configured_catalog_path
                )

        deduped_scenarios: list[ConnectorTestScenario] = []

        for scenario in scenarios:
            for existing_scenario in deduped_scenarios:
                if scenario.config_path == existing_scenario.config_path:
                    # If a scenario with the same config_path already exists, we merge the empty streams.
                    # scenarios are immutable, so we create a new one.
                    all_empty_streams = (existing_scenario.empty_streams or []) + (
                        scenario.empty_streams or []
                    )
                    if (
                        existing_scenario.status is not None
                        and scenario.status is not None
                        and existing_scenario.status != scenario.status
                    ):
                        raise ValueError(
                            f"Conflicting expected statuses declared for config "
                            f"'{scenario.config_path}': '{existing_scenario.status}' and "
                            f"'{scenario.status}'."
                        )
                    merged_scenario = existing_scenario.model_copy(
                        update={
                            "empty_streams": list(set(all_empty_streams)),
                            "status": existing_scenario.status or scenario.status,
                            **{
                                opt_out: getattr(existing_scenario, opt_out)
                                and getattr(scenario, opt_out)
                                for opt_out in _READ_CHECK_OPT_OUTS
                            },
                            "sections": tuple(
                                dict.fromkeys(existing_scenario.sections + scenario.sections)
                            ),
                        }
                    )
                    deduped_scenarios.remove(existing_scenario)
                    deduped_scenarios.append(merged_scenario)
                    break
            else:
                # If a scenario does not exist with the config, add the new scenario to the list.
                deduped_scenarios.append(scenario)

        for index, scenario in enumerate(deduped_scenarios):
            catalogs = read_catalogs.get(scenario.config_path)
            if catalogs is None:
                continue
            catalog = next(iter(catalogs)) if len(catalogs) == 1 else None
            if scenario.configured_catalog_path != catalog:
                deduped_scenarios[index] = scenario.model_copy(
                    update={"configured_catalog_path": catalog}
                )
        return deduped_scenarios

    @classmethod
    def _collect_scenarios(cls) -> CollectedScenarios:
        """Read the runnable scenarios and the declared bypasses of `acceptance-test-config.yml`.

        A Java connector without the file is reported as bypassed: Java connectors are tested by
        their Gradle suites. Every other connector without it fails, with
        `MissingAcceptanceTestConfigError`.
        """
        connector_root = cls.get_connector_root_dir()
        config_path = (connector_root / ACCEPTANCE_TEST_CONFIG).absolute()
        if not config_path.exists() and is_java_connector(connector_root):
            return CollectedScenarios(
                config_path=config_path,
                bypasses=[ScenarioBypass(reason=JAVA_BYPASS_REASON, location="metadata.yaml")],
            )
        return collect_scenarios(cls.acceptance_test_config, config_path)

    @classmethod
    def get_scenarios(
        cls,
    ) -> list[ConnectorTestScenario]:
        """Collect one scenario per config listed in `acceptance-test-config.yml`.

        Every section with a `tests` list contributes its configs (`SCENARIO_SECTIONS`), and each
        config runs the command of the section(s) that list it: `spec` validates the config
        against the spec, `connection` runs `check` against the declared `status`, `discovery`
        runs `discover`, and `basic_read` and `full_refresh` run a full-refresh `read` of the
        streams of the config's `configured_catalog_path`, or of every discovered stream when it
        declares none (`SECTION_COMMANDS`). The `incremental` section is not collected. A section
        in the legacy list layout (`spec: [...]`) is read as its `tests` list.

        The file is required (except for Java connectors), and a legacy top-level `tests:` key
        fails; see `load_acceptance_test_config`. An empty result is not an error here:
        `pytest_generate_tests` fails collection when there is no scenario and no declared
        bypass (`get_scenario_bypasses`).

        An entry of a collected section that sets a key the standard tests do not read, or one
        they fill in themselves (`sections`, `test_strictness_level`), fails collection, with one
        line per key naming the key, the section and the config, including entries that are
        skipped (no `config_path`, or an `iam_role` config). See `collect_scenarios`.

        This has to be a separate function because pytest does not allow
        parametrization of fixtures with arguments from the test class itself.
        """
        return cls._dedup_scenarios(cls._collect_scenarios().scenarios)

    @classmethod
    def get_scenario_bypasses(cls) -> list[ScenarioBypass]:
        """The `bypass_reason`s that `acceptance-test-config.yml` declares for collected parts.

        A top-level `bypass_reason` bypasses the connector; one on a collected section with no
        runnable config, or on an entry without `config_path`, bypasses that section or entry.
        `pytest_generate_tests` reports them as skipped tests, and accepts a connector with no
        scenario only when it declares at least one.

        A suite that overrides `get_scenarios` supplies its own scenarios, so the file is not read
        and nothing is bypassed.
        """
        if cls._supplies_own_scenarios():
            return []
        return cls._collect_scenarios().bypasses

    @classmethod
    def describe_empty_scenarios(cls) -> str:
        """Why `acceptance-test-config.yml` yields neither a scenario nor a bypass."""
        if cls._supplies_own_scenarios():
            return f"{cls.__name__}.get_scenarios() returned no test scenarios."
        return cls._collect_scenarios().describe_empty(cast(str, cls.connector_name))

    @classmethod
    def _supplies_own_scenarios(cls) -> bool:
        """Whether the suite overrides `get_scenarios`, so its scenarios do not come from the file."""
        subclasses = cls.__mro__[: cls.__mro__.index(DockerConnectorTestSuite)]
        return any("get_scenarios" in vars(subclass) for subclass in subclasses)

    @classmethod
    def _connector_image(
        cls,
        connector_image_override: str | None,
        connector_base_image_override: str | None,
    ) -> str:
        """Return the image under test: the override if given, else a fresh `dev-latest` build.

        Note:
          - It is expected for docker image caches to be reused between test runs.
          - In the rare case that image caches need to be cleared, please clear
            the local docker image cache using `docker image prune -a` command.
        """
        if connector_image_override:
            return connector_image_override

        connector_root = cls.get_connector_root_dir().absolute()
        metadata = MetadataFile.from_file(connector_root / "metadata.yaml")
        return build_connector_image(
            connector_name=connector_root.name,
            connector_directory=connector_root,
            metadata=metadata,
            tag="dev-latest",
            no_verify=False,
            base_image_override=connector_base_image_override,
        )

    @pytest.mark.skipif(
        shutil.which("docker") is None,
        reason="docker CLI not found in PATH, skipping docker image tests",
    )
    @pytest.mark.image_tests
    def test_docker_image_build_and_spec(
        self,
        connector_image_override: str | None,
        connector_base_image_override: str | None,
    ) -> None:
        """Run `spec` in the connector image and lint the spec it emits.

        The image must emit exactly one SPEC message. `airbyte-cdk image test` builds the
        image with `no_verify=True`, which skips the build-time spec check, so this test is
        what catches an image that emits no spec on that path.
        """
        connector_root = self.get_connector_root_dir().absolute()
        connector_image = self._connector_image(
            connector_image_override, connector_base_image_override
        )

        spec = _run_docker_spec(connector_image, connector_name=connector_root.name)
        assert_spec_is_valid(spec, connector_name=connector_root.name)

    @pytest.mark.skipif(
        shutil.which("docker") is None,
        reason="docker CLI not found in PATH, skipping docker image tests",
    )
    @pytest.mark.image_tests
    def test_docker_image_spec_backward_compatibility(
        self,
        connector_image_override: str | None,
        connector_base_image_override: str | None,
    ) -> None:
        """Fail if the image's spec rejects configs that the published version accepted.

        The spec is compared against the `latest` version in the public connector registry
        (connectors.airbyte.com), once per registry the connector is published to, with `spec`
        run in the matching deployment mode. A connector that was never published, or whose
        version is older than the published one, has nothing to compare against. If the
        registry cannot be reached after retries, the test is skipped with a warning rather
        than failing every connector's tests during an outage.

        Breaking findings are waived, and the test is skipped, in two cases:

        - The change goes through the breaking-change process: `releases.breakingChanges` in
          metadata.yaml declares a version after the published one, up to `dockerImageTag`.
        - `backward_compatibility_tests_config.disable_for_version` in an entry of the `spec`
          section of acceptance-test-config.yml names the published version. The waiver
          expires when a newer version is published. A connector without that file, such as
          one rebuilt on a new base image without a version bump, can add a file that holds
          only this entry.

        The compatible changes, and the waived breaking ones, are reported as a warning, so they
        appear in the warnings summary of the test run.
        """
        connector_root = self.get_connector_root_dir().absolute()
        metadata = MetadataFile.from_file(connector_root / "metadata.yaml")
        metadata_data = metadata.data.model_dump()
        current_version = metadata.data.dockerImageTag

        try:
            published_specs = {
                registry: published
                for registry in DEPLOYMENT_MODE_ENV_VARS
                if (published := fetch_published_spec(metadata.data.dockerRepository, registry))
            }
        except REGISTRY_UNAVAILABLE_ERRORS as error:
            message = (
                f"Skipping the spec backward-compatibility check of `{self.connector_name}`: the "
                f"connector registry could not be reached ({error}). The spec was not compared "
                "with the published version."
            )
            warnings.warn(message, category=UserWarning, stacklevel=1)
            pytest.skip(message)
        except requests.RequestException as error:
            pytest.fail(
                f"Could not fetch the published spec of `{self.connector_name}` from the connector "
                f"registry: {error}"
            )
        if not published_specs:
            pytest.skip(f"`{self.connector_name}` has no published version to compare against.")

        newer_versions = sorted(
            {
                published.version
                for published in published_specs.values()
                if is_newer_version(published.version, current_version)
            }
        )
        published_specs = {
            registry: published
            for registry, published in published_specs.items()
            if published.version not in newer_versions
        }
        if not published_specs:
            pytest.skip(
                f"`{self.connector_name}` {current_version} is older than the published version "
                f"{', '.join(newer_versions)}, so there is no earlier spec to compare against."
            )

        connector_image: str | None = connector_image_override
        if not connector_image:
            connector_image = build_connector_image(
                connector_name=connector_root.name,
                connector_directory=connector_root,
                metadata=metadata,
                tag="dev-latest",
                no_verify=False,
                base_image_override=connector_base_image_override,
            )

        failures: list[str] = []
        waivers: list[str] = []
        summaries: list[str] = []
        for registry, published in published_specs.items():
            comparison = compare_specs(
                published.spec, _run_spec_in_image(connector_image, registry)
            )
            if comparison.is_backward_compatible:
                if comparison.compatible:
                    summaries.append(
                        format_spec_change_summary(
                            registry=registry, published=published, comparison=comparison
                        )
                    )
                continue

            declared = declared_breaking_changes(metadata_data, published.version, current_version)
            if declared:
                waiver: str | None = f"declared as breaking in {', '.join(declared)}"
            elif published.version in self._spec_compatibility_disabled_versions():
                waiver = f"waived by `disable_for_version: {published.version}`"
            else:
                waiver = None

            if waiver:
                summaries.append(
                    format_spec_change_summary(
                        registry=registry, published=published, comparison=comparison, waiver=waiver
                    )
                )
                waivers.append(
                    f"{len(comparison.breaking)} breaking change(s) to the {registry.upper()} "
                    f"spec since {published.version}, {waiver}"
                )
            else:
                failures.append(
                    format_breaking_spec_changes(
                        connector_name=self.connector_name,
                        current_version=current_version,
                        registry=registry,
                        published=published,
                        comparison=comparison,
                    )
                )

        if summaries:
            warnings.warn(
                f"Spec changes of `{self.connector_name}` {current_version} judged safe or "
                "waived:\n" + "\n".join(summaries),
                category=UserWarning,
                stacklevel=1,
            )
        if failures:
            pytest.fail("\n\n".join(failures))
        if waivers:
            pytest.skip("; ".join(waivers))

    def _spec_compatibility_disabled_versions(self) -> list[str]:
        """The `disable_for_version` waivers of acceptance-test-config.yml, if it has any.

        A missing, empty or malformed file has none. The file is only read once a comparison
        is breaking, so its layout cannot fail a connector whose spec is compatible.
        """
        try:
            acceptance_test_config = self.acceptance_test_config
        except (FileNotFoundError, ValueError, TypeError, yaml.YAMLError):
            return []
        return disabled_for_version(acceptance_test_config)

    @pytest.mark.skipif(
        shutil.which("docker") is None,
        reason="docker CLI not found in PATH, skipping docker image tests",
    )
    @pytest.mark.image_tests
    def test_docker_image_build_and_check(
        self,
        scenario: ConnectorTestScenario,
        connector_image_override: str | None,
        connector_base_image_override: str | None,
    ) -> None:
        """Run `docker_image` acceptance tests.

        This test builds the connector image and runs the `check` command inside the container,
        for configs listed under `connection` in `acceptance-test-config.yml`.
        """
        skip_unless_runs(scenario, "check")
        connector_root = self.get_connector_root_dir()
        connector_image = self._connector_image(
            connector_image_override, connector_base_image_override
        )

        container_config_path = "/secrets/config.json"
        with scenario.with_temp_config_file(
            connector_root=connector_root,
        ) as temp_config_file:
            check_result = run_docker_airbyte_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    *CONNECTOR_WORKDIR_ARGS,
                    "-v",
                    f"{temp_config_file}:{container_config_path}:rw",
                    connector_image,
                    "check",
                    "--config",
                    container_config_path,
                ],
                raise_if_errors=False,
            )

        # The leak check runs first, whatever the outcome: the assertions below print the raw
        # CONNECTION_STATUS and error messages, which are the likeliest to carry a secret.
        connector_name = connector_root.absolute().name
        try:
            spec = _run_docker_spec(connector_image, connector_name=connector_name)
        except Exception:
            pytest.skip(_SPEC_FAILED_SKIP_REASON)
        assert_no_secrets_in_output(
            check_result,
            spec=spec,
            config=scenario.get_config_dict(connector_root=connector_root, empty_if_missing=True),
            verb="check",
            connector_name=connector_name,
        )
        # For expected-failure scenarios, a non-zero exit or trace error is an acceptable way
        # for `check` to fail, so only the other scenarios raise on errors.
        if not scenario.expected_outcome.expect_exception():
            check_result.raise_if_errors()

        # This makes the image test exercise the connector's actual `check` outcome inside the
        # container, in both directions (e.g. it fails if bundled custom components are rejected
        # by the CDK baked into the base image, and it fails if a `check` that is expected to
        # fail starts succeeding).
        assert_check_outcome(
            check_result=check_result,
            expected_outcome=scenario.expected_outcome,
            connector_name=connector_name,
        )

    @pytest.mark.skipif(
        shutil.which("docker") is None,
        reason="docker CLI not found in PATH, skipping docker image tests",
    )
    @pytest.mark.image_tests
    def test_docker_image_build_and_config_matches_spec(
        self,
        scenario: ConnectorTestScenario,
        connector_image_override: str | None,
        connector_base_image_override: str | None,
    ) -> None:
        """Validate the scenario's config against the `spec` the image reports.

        Runs for configs listed under `spec` in `acceptance-test-config.yml` (the CAT
        `test_config_match_spec` check).
        """
        skip_unless_runs(scenario, "spec")
        connector_root = self.get_connector_root_dir()
        connector_image = self._connector_image(
            connector_image_override, connector_base_image_override
        )

        spec_result = run_docker_airbyte_command(
            [
                "docker",
                "run",
                "--rm",
                connector_image,
                "spec",
            ],
            raise_if_errors=True,
        )
        assert_config_matches_spec(
            config=scenario.get_config_dict(connector_root=connector_root, empty_if_missing=False),
            spec_result=spec_result,
            connector_name=connector_root.absolute().name,
            scenario_id=scenario.id,
        )

    @pytest.mark.skipif(
        shutil.which("docker") is None,
        reason="docker CLI not found in PATH, skipping docker image tests",
    )
    @pytest.mark.image_tests
    def test_docker_image_build_and_discover(
        self,
        scenario: ConnectorTestScenario,
        connector_image_override: str | None,
        connector_base_image_override: str | None,
    ) -> None:
        """Run `discover` inside the connector image and require a non-empty catalog.

        Runs for configs listed under `discovery` in `acceptance-test-config.yml`.
        """
        if self.is_destination_connector():
            pytest.skip("Skipping discover test for destination connector.")

        skip_unless_runs(scenario, "discover")

        if scenario.expected_outcome.expect_exception():
            pytest.skip("Skipping (expected to fail).")

        connector_root = self.get_connector_root_dir()
        connector_image = self._connector_image(
            connector_image_override, connector_base_image_override
        )

        container_config_path = "/secrets/config.json"
        with scenario.with_temp_config_file(
            connector_root=connector_root,
        ) as temp_config_file:
            discover_result = run_docker_airbyte_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    *CONNECTOR_WORKDIR_ARGS,
                    "-v",
                    f"{temp_config_file}:{container_config_path}:rw",
                    connector_image,
                    "discover",
                    "--config",
                    container_config_path,
                ],
                raise_if_errors=True,
            )

        get_discovered_streams(discover_result, connector_root.absolute().name)

    @pytest.mark.skipif(
        shutil.which("docker") is None,
        reason="docker CLI not found in PATH, skipping docker image tests",
    )
    @pytest.mark.image_tests
    def test_docker_image_build_and_read(
        self,
        scenario: ConnectorTestScenario,
        connector_image_override: str | None,
        connector_base_image_override: str | None,
        read_from_streams: Literal["default", "none"] | list[str],
        read_scenarios: Literal["all", "none", "default"] | list[str],
    ) -> None:
        """Read from the connector's Docker image, and check the records as `test_basic_read` does.

        This test builds the connector image and runs a full-refresh `read` command inside the
        container, for configs listed under `basic_read` or `full_refresh` in
        `acceptance-test-config.yml`. The read covers the streams of the config's
        `configured_catalog_path`, or every discovered stream when it declares none, minus its
        `empty_streams` (see `get_read_streams`), narrowed further by `--read-from-streams`.

        The read gets the same checks as `test_basic_read`. For a config listed under
        `basic_read`: every stream it reads must return at least one record; every record must
        match its stream's JSON schema (opt out with `validate_schema: false`, which a connector
        at `test_strictness_level: high` cannot); every record of a stream with a
        `source_defined_primary_key` must have a usable primary key (opt out with
        `validate_primary_keys_data_type: false`); and every stream must emit state with
        `sourceStats` (opt out with `validate_state_messages: false`). Incomplete stream statuses
        are reported as a `StreamStatusWarning`, not enforced. A config listed only under
        `full_refresh` must return some records.

        The test is skipped unless `--read-from-streams` turns it on: the in-process
        `test_basic_read` already reads every connector whose in-process tests run, and this read
        is meant to be turned on only for a connector whose in-process tests cannot run.
        `--read-from-streams=default` reads the streams above; a stream list narrows them.
        `--read-scenarios` picks which configs are read (by default `config`, `valid_config` and
        `default`, plus a `basic_read` config if none of those is one; see
        `get_default_read_scenario_ids`). A config or stream name that matches nothing fails the
        test, so a typo cannot skip every read. See the `read_from_streams` and `read_scenarios`
        fixtures.

        Note:
          - If the --connector-image arg is provided, it will be used instead of building the image.
        """
        if self.is_destination_connector():
            pytest.skip("Skipping read test for destination connector.")

        skip_unless_runs(scenario, "read")

        if scenario.expected_outcome.expect_exception():
            pytest.skip("Skipping (expected to fail).")

        if read_from_streams == "none":
            pytest.skip(
                "Skipping the Docker read test: it is off unless `--read-from-streams` turns it "
                "on (for example `--read-from-streams=default`)."
            )

        if read_scenarios == "none":
            pytest.skip("Skipping the Docker read test: it was disabled with `--read-scenarios`.")

        if read_scenarios == "default":
            default_ids = get_default_read_scenario_ids(self.get_scenarios())
            if scenario.id not in default_ids:
                pytest.skip(
                    f"Skipping the Docker read of config '{scenario.id}': "
                    f"`--read-scenarios=default` reads {', '.join(sorted(map(repr, default_ids)))}. "
                    "Pass `--read-scenarios=all` to read every config."
                )
        elif read_scenarios != "all":
            fail_on_unknown_read_scenarios(read_scenarios, self.get_scenarios())
            if scenario.id.lower() not in read_scenarios:
                pytest.skip(
                    f"Skipping the Docker read of config '{scenario.id}': not in "
                    f"`--read-scenarios={','.join(read_scenarios)}`."
                )

        if scenario.is_basic_read_config:
            assert_schema_validation_opt_out_allowed(
                validate_schema=scenario.validate_schema,
                test_strictness_level=scenario.test_strictness_level,
            )

        connector_root = self.get_connector_root_dir()
        connector_name = connector_root.absolute().name
        connector_image = self._connector_image(
            connector_image_override, connector_base_image_override
        )

        container_config_path = "/secrets/config.json"
        container_catalog_path = "/secrets/catalog.json"

        with (
            scenario.with_temp_config_file(
                connector_root=connector_root,
            ) as temp_config_file,
            tempfile.TemporaryDirectory(
                prefix=f"{connector_name}-test",
                ignore_cleanup_errors=True,
            ) as temp_dir_str,
        ):
            temp_dir = Path(temp_dir_str)
            discover_result = run_docker_airbyte_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    *CONNECTOR_WORKDIR_ARGS,
                    "-v",
                    f"{temp_config_file}:{container_config_path}:rw",
                    connector_image,
                    "discover",
                    "--config",
                    container_config_path,
                ],
                raise_if_errors=True,
            )

            discovered_streams = get_discovered_streams(discover_result, connector_name)
            if isinstance(read_from_streams, list):
                fail_on_undiscovered_read_streams(
                    read_from_streams, discovered_streams, scenario_id=scenario.id
                )
            read_streams = select_docker_read_streams(
                get_read_streams(scenario, discovered_streams, connector_root),
                read_from_streams=read_from_streams,
            )
            if not read_streams and isinstance(read_from_streams, list):
                pytest.skip(
                    f"Skipping the Docker read of config '{scenario.id}': none of the streams in "
                    f"`--read-from-streams={','.join(read_from_streams)}` is read for it (its "
                    "`configured_catalog_path` leaves them out, or its `empty_streams` lists them)."
                )

            configured_catalog: ConfiguredAirbyteCatalog = ConfiguredAirbyteCatalog(
                streams=[
                    ConfiguredAirbyteStream(
                        stream=stream,
                        sync_mode=SyncMode.full_refresh,
                        destination_sync_mode=DestinationSyncMode.append,
                    )
                    for stream in read_streams
                ]
            )
            configured_catalog_path = temp_dir / "catalog.json"
            configured_catalog_path.write_text(
                orjson.dumps(asdict(configured_catalog)).decode("utf-8")
            )
            read_result: EntrypointOutput = run_docker_airbyte_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    *CONNECTOR_WORKDIR_ARGS,
                    "-v",
                    f"{temp_config_file}:{container_config_path}:rw",
                    "-v",
                    f"{configured_catalog_path}:{container_catalog_path}",
                    connector_image,
                    "read",
                    "--config",
                    container_config_path,
                    "--catalog",
                    container_catalog_path,
                ],
                raise_if_errors=True,
            )

        if scenario.is_basic_read_config:
            status_problems = find_stream_status_problems(
                read_result.trace_messages, configured_catalog
            )
            if status_problems:
                warnings.warn(
                    "The read's stream statuses are incomplete. This check is not enforced "
                    "yet:\n" + "\n".join(status_problems),
                    category=StreamStatusWarning,
                    stacklevel=1,
                )
            # Also asserts that the read returned records, in the same pass over them.
            assert_read_records(
                records=read_result.records_iterator,
                configured_catalog=configured_catalog,
                require_records_per_stream=True,
                validate_schema=scenario.validate_schema,
                validate_primary_keys=scenario.validate_primary_keys_data_type,
                state_messages=(
                    read_result.state_messages if scenario.validate_state_messages else None
                ),
            )
        elif next(read_result.records_iterator, None) is None:
            raise AssertionError("Expected records but got none.")
