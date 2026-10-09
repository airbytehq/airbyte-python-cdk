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
    AirbyteCatalog,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    SyncMode,
)
from airbyte_cdk.models.connector_metadata import MetadataFile
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models import ConnectorTestScenario, ExpectedOutcome
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
from airbyte_cdk.utils.connector_paths import (
    ACCEPTANCE_TEST_CONFIG,
    find_connector_root,
)
from airbyte_cdk.utils.docker import (
    build_connector_image,
    run_docker_airbyte_command,
    run_docker_command,
)


def _assert_check_outcome(
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
    - A scenario expecting failure must not report `SUCCEEDED` (a graceful `FAILED` status
      or an uncaught error with no status message both count as the expected failure).
    - `ALLOW_ANY` scenarios accept either outcome.
    """
    connection_statuses = [
        message.connectionStatus
        for message in check_result.connection_status_messages
        if message.connectionStatus is not None
    ]
    if expected_outcome.expect_exception():
        assert not connection_statuses or connection_statuses[-1].status != Status.SUCCEEDED, (
            f"`check` for connector '{connector_name}' was expected to fail, but reported: "
            f"{connection_statuses[-1]}"
        )
        return

    assert connection_statuses, (
        f"`check` for connector '{connector_name}' emitted no CONNECTION_STATUS message. "
        f"Logs: {check_result.logs}"
    )
    if expected_outcome.expect_success():
        assert connection_statuses[-1].status == Status.SUCCEEDED, (
            f"`check` for connector '{connector_name}' did not succeed: {connection_statuses[-1]}"
        )


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

        Also perform some basic validation that the file has the expected structure.
        """
        acceptance_test_config_path = cls.get_connector_root_dir() / ACCEPTANCE_TEST_CONFIG
        if not acceptance_test_config_path.exists():
            raise FileNotFoundError(
                f"Acceptance test config file not found at: {str(acceptance_test_config_path)}"
            )

        tests_config = yaml.safe_load(acceptance_test_config_path.read_text())

        if "acceptance_tests" not in tests_config:
            raise ValueError(
                f"Acceptance tests config not found in {acceptance_test_config_path}."
                f" Found only: {str(tests_config)}."
            )
        return tests_config

    @staticmethod
    def _dedup_scenarios(scenarios: list[ConnectorTestScenario]) -> list[ConnectorTestScenario]:
        """
        For FAST tests, we treat each config as a separate test scenario to run against, whereas CATs defined
        a series of more granular scenarios specifying a config_path and empty_streams among other things.

        This method deduplicates the CATs scenarios based on their config_path. In doing so, we choose to
        take the union of any defined empty_streams, to have high confidence that runnning a read with the
        config will not error on the lack of data in the empty streams or lack of permissions to read them.

        We also carry over any explicitly declared `status`. Only the `connection` section declares one,
        so without this an entry from another section (e.g. `spec`) would win and silently downgrade the
        scenario to `ALLOW_ANY`, which accepts a failing `check`.
        """
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
                        }
                    )
                    deduped_scenarios.remove(existing_scenario)
                    deduped_scenarios.append(merged_scenario)
                    break
            else:
                # If a scenario does not exist with the config, add the new scenario to the list.
                deduped_scenarios.append(scenario)
        return deduped_scenarios

    @classmethod
    def get_scenarios(
        cls,
    ) -> list[ConnectorTestScenario]:
        """Get acceptance tests for a given category.

        This has to be a separate function because pytest does not allow
        parametrization of fixtures with arguments from the test class itself.
        """
        try:
            all_tests_config = cls.acceptance_test_config
        except FileNotFoundError as e:
            # Destinations sometimes do not have an acceptance tests file.
            warnings.warn(
                f"Acceptance test config file not found: {e!s}. No scenarios will be loaded.",
                category=UserWarning,
                stacklevel=1,
            )
            return []

        test_scenarios: list[ConnectorTestScenario] = []
        # we look in the basic_read section to find any empty streams
        for category in ["spec", "connection", "basic_read"]:
            if (
                category not in all_tests_config["acceptance_tests"]
                or "tests" not in all_tests_config["acceptance_tests"][category]
            ):
                continue

            for test in all_tests_config["acceptance_tests"][category]["tests"]:
                if "config_path" not in test:
                    # Skip tests without a config_path
                    continue

                if "iam_role" in test["config_path"]:
                    # We skip iam_role tests for now, as they are not supported in the test suite.
                    continue

                scenario = ConnectorTestScenario.model_validate(test)

                test_scenarios.append(scenario)

        deduped_test_scenarios = cls._dedup_scenarios(test_scenarios)

        return deduped_test_scenarios

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
        """Run `docker_image` acceptance tests."""
        connector_root = self.get_connector_root_dir().absolute()
        metadata = MetadataFile.from_file(connector_root / "metadata.yaml")

        connector_image: str | None = connector_image_override
        if not connector_image:
            tag = "dev-latest"
            connector_image = build_connector_image(
                connector_name=connector_root.absolute().name,
                connector_directory=connector_root,
                metadata=metadata,
                tag=tag,
                no_verify=False,
                base_image_override=connector_base_image_override,
            )

        _ = run_docker_airbyte_command(
            [
                "docker",
                "run",
                "--rm",
                connector_image,
                "spec",
            ],
            raise_if_errors=True,
        )

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

        This test builds the connector image and runs the `check` command inside the container.

        Note:
          - It is expected for docker image caches to be reused between test runs.
          - In the rare case that image caches need to be cleared, please clear
            the local docker image cache using `docker image prune -a` command.
        """
        tag = "dev-latest"
        connector_root = self.get_connector_root_dir()
        metadata = MetadataFile.from_file(connector_root / "metadata.yaml")
        connector_image: str | None = connector_image_override
        if not connector_image:
            tag = "dev-latest"
            connector_image = build_connector_image(
                connector_name=connector_root.absolute().name,
                connector_directory=connector_root,
                metadata=metadata,
                tag=tag,
                no_verify=False,
                base_image_override=connector_base_image_override,
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
                    "-v",
                    f"{temp_config_file}:{container_config_path}:rw",
                    connector_image,
                    "check",
                    "--config",
                    container_config_path,
                ],
                # For expected-failure scenarios, a non-zero exit or trace error is an
                # acceptable way for `check` to fail; don't raise before we assert on it.
                raise_if_errors=not scenario.expected_outcome.expect_exception(),
            )

        # This makes the image test exercise the connector's actual `check` outcome inside the
        # container, in both directions (e.g. it fails if bundled custom components are rejected
        # by the CDK baked into the base image, and it fails if a `check` that is expected to
        # fail starts succeeding).
        _assert_check_outcome(
            check_result=check_result,
            expected_outcome=scenario.expected_outcome,
            connector_name=connector_root.absolute().name,
        )

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
        read_from_streams: Literal["all", "none", "default"] | list[str],
        read_scenarios: Literal["all", "none", "default"] | list[str],
    ) -> None:
        """Read from the connector's Docker image.

        This test builds the connector image and runs the `read` command inside the container.

        Note:
          - It is expected for docker image caches to be reused between test runs.
          - In the rare case that image caches need to be cleared, please clear
            the local docker image cache using `docker image prune -a` command.
          - If the --connector-image arg is provided, it will be used instead of building the image.
        """
        if self.is_destination_connector():
            pytest.skip("Skipping read test for destination connector.")

        if scenario.expected_outcome.expect_exception():
            pytest.skip("Skipping (expected to fail).")

        if read_from_streams == "none":
            pytest.skip("Skipping read test (`--read-from-streams=false`).")

        if read_scenarios == "none":
            pytest.skip("Skipping (`--read-scenarios=none`).")

        default_scenario_ids = ["config", "valid_config", "default"]
        if read_scenarios == "all":
            pass
        elif read_scenarios == "default":
            if scenario.id not in default_scenario_ids:
                pytest.skip(
                    f"Skipping read test for scenario '{scenario.id}' "
                    f"(not in default scenarios list '{default_scenario_ids}')."
                )
        elif scenario.id not in read_scenarios:
            # pytest.skip(
            raise ValueError(
                f"Skipping read test for scenario '{scenario.id}' "
                f"(not in --read-scenarios={read_scenarios})."
            )

        tag = "dev-latest"
        connector_root = self.get_connector_root_dir()
        connector_name = connector_root.absolute().name
        metadata = MetadataFile.from_file(connector_root / "metadata.yaml")
        connector_image: str | None = connector_image_override
        if not connector_image:
            tag = "dev-latest"
            connector_image = build_connector_image(
                connector_name=connector_name,
                connector_directory=connector_root,
                metadata=metadata,
                tag=tag,
                no_verify=False,
                base_image_override=connector_base_image_override,
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
                    "-v",
                    f"{temp_config_file}:{container_config_path}:rw",
                    connector_image,
                    "discover",
                    "--config",
                    container_config_path,
                ],
                raise_if_errors=True,
            )

            catalog_message = discover_result.catalog  # Get catalog message
            assert catalog_message.catalog is not None, "Catalog message missing catalog."
            discovered_catalog: AirbyteCatalog = catalog_message.catalog
            if not discovered_catalog.streams:
                raise ValueError(
                    f"Discovered catalog for connector '{connector_name}' is empty. "
                    "Please check the connector's discover implementation."
                )

            streams_list = [stream.name for stream in discovered_catalog.streams]
            if read_from_streams == "default" and metadata.data.suggestedStreams:
                # set `streams_list` to be the intersection of discovered and suggested streams.
                streams_list = list(set(streams_list) & set(metadata.data.suggestedStreams.streams))

            if isinstance(read_from_streams, list):
                # If `read_from_streams` is a list, we filter the discovered streams.
                streams_list = list(set(streams_list) & set(read_from_streams))

            if scenario.empty_streams:
                # Filter out streams marked as empty in the scenario.
                empty_stream_names = [stream.name for stream in scenario.empty_streams]
                streams_list = [s for s in streams_list if s.name not in empty_stream_names]

            configured_catalog: ConfiguredAirbyteCatalog = ConfiguredAirbyteCatalog(
                streams=[
                    ConfiguredAirbyteStream(
                        stream=stream,
                        sync_mode=(
                            stream.supported_sync_modes[0]
                            if stream.supported_sync_modes
                            else SyncMode.full_refresh
                        ),
                        destination_sync_mode=DestinationSyncMode.append,
                    )
                    for stream in discovered_catalog.streams
                    if stream.name in streams_list
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
