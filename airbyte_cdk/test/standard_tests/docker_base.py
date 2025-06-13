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
from subprocess import CompletedProcess, SubprocessError
from typing import Literal

import orjson
import pytest
import yaml
from boltons.typeutils import classproperty

from airbyte_cdk.models import (
    AirbyteCatalog,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from airbyte_cdk.models.airbyte_protocol_serializers import (
    AirbyteCatalogSerializer,
    AirbyteStreamSerializer,
)
from airbyte_cdk.models.connector_metadata import MetadataFile
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.utils.reading import catalog
from airbyte_cdk.utils.connector_paths import (
    ACCEPTANCE_TEST_CONFIG,
    find_connector_root,
)
from airbyte_cdk.utils.docker import build_connector_image, run_docker_command


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
    def acceptance_test_config_path(cls) -> Path:
        """Get the path to the acceptance test config file."""
        result = cls.get_connector_root_dir() / ACCEPTANCE_TEST_CONFIG
        if result.exists():
            return result

        raise FileNotFoundError(f"Acceptance test config file not found at: {str(result)}")

    @classmethod
    def get_scenarios(
        cls,
    ) -> list[ConnectorTestScenario]:
        """Get acceptance tests for a given category.

        This has to be a separate function because pytest does not allow
        parametrization of fixtures with arguments from the test class itself.
        """
        categories = ["connection", "spec"]
        try:
            acceptance_test_config_path = cls.acceptance_test_config_path
        except FileNotFoundError as e:
            # Destinations sometimes do not have an acceptance tests file.
            warnings.warn(
                f"Acceptance test config file not found: {e!s}. No scenarios will be loaded.",
                category=UserWarning,
                stacklevel=1,
            )
            return []

        all_tests_config = yaml.safe_load(cls.acceptance_test_config_path.read_text())
        if "acceptance_tests" not in all_tests_config:
            raise ValueError(
                f"Acceptance tests config not found in {cls.acceptance_test_config_path}."
                f" Found only: {str(all_tests_config)}."
            )

        test_scenarios: list[ConnectorTestScenario] = []
        for category in categories:
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

                if scenario.config_path and scenario.config_path in [
                    s.config_path for s in test_scenarios
                ]:
                    # Skip duplicate scenarios based on config_path
                    continue

                test_scenarios.append(scenario)

        return test_scenarios

    @pytest.mark.skipif(
        shutil.which("docker") is None,
        reason="docker CLI not found in PATH, skipping docker image tests",
    )
    @pytest.mark.image_tests
    def test_docker_image_build_and_spec(
        self,
        connector_image_override: str | None,
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
            )

        try:
            result: CompletedProcess[str] = run_docker_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    connector_image,
                    "spec",
                ],
                check=True,  # Raise an error if the command fails
                capture_stderr=True,
                capture_stdout=True,
            )
        except SubprocessError as ex:
            raise AssertionError(
                f"Failed to run `spec` command in docker image {connector_image!r}. Error: {ex!s}"
            ) from None

    @pytest.mark.skipif(
        shutil.which("docker") is None,
        reason="docker CLI not found in PATH, skipping docker image tests",
    )
    @pytest.mark.image_tests
    def test_docker_image_build_and_check(
        self,
        scenario: ConnectorTestScenario,
        connector_image_override: str | None,
    ) -> None:
        """Run `docker_image` acceptance tests.

        This test builds the connector image and runs the `check` command inside the container.

        Note:
          - It is expected for docker image caches to be reused between test runs.
          - In the rare case that image caches need to be cleared, please clear
            the local docker image cache using `docker image prune -a` command.
        """
        if scenario.expected_outcome.expect_exception():
            pytest.skip("Skipping test_docker_image_build_and_check (expected to fail).")

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
            )

        container_config_path = "/secrets/config.json"
        with scenario.with_temp_config_file(
            connector_root=connector_root,
        ) as temp_config_file:
            _ = run_docker_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{temp_config_file}:{container_config_path}",
                    connector_image,
                    "check",
                    "--config",
                    container_config_path,
                ],
                check=True,  # Raise an error if the command fails
                capture_stderr=True,
                capture_stdout=True,
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
            discover_result = run_docker_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{temp_config_file}:{container_config_path}",
                    connector_image,
                    "discover",
                    "--config",
                    container_config_path,
                ],
                check=True,  # Raise an error if the command fails
                capture_stderr=True,
                capture_stdout=True,
            )
            parsed_output = EntrypointOutput(messages=discover_result.stdout.splitlines())
            try:
                catalog_message = parsed_output.catalog  # Get catalog message
                assert catalog_message.catalog is not None, "Catalog message missing catalog."
                discovered_catalog: AirbyteCatalog = parsed_output.catalog.catalog
            except Exception as ex:
                raise AssertionError(
                    f"Failed to load discovered catalog from {discover_result.stdout}. "
                    f"Error: {ex!s}"
                ) from None
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
            read_result: CompletedProcess[str] = run_docker_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{temp_config_file}:{container_config_path}",
                    "-v",
                    f"{configured_catalog_path}:{container_catalog_path}",
                    connector_image,
                    "read",
                    "--config",
                    container_config_path,
                    "--catalog",
                    container_catalog_path,
                ],
                check=False,
                capture_stderr=True,
                capture_stdout=True,
            )
            if read_result.returncode != 0:
                raise AssertionError(
                    f"Failed to run `read` command in docker image {connector_image!r}. "
                    "\n-----------------"
                    f"EXIT CODE: {read_result.returncode}\n"
                    "STDERR:\n"
                    f"{read_result.stderr}\n"
                    f"STDOUT:\n"
                    f"{read_result.stdout}\n"
                    "\n-----------------"
                ) from None
