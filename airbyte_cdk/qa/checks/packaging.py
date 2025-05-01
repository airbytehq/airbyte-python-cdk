"""Packaging checks for Airbyte connectors."""

import semver
import toml
from pydash.objects import get

from airbyte_cdk.qa import consts
from airbyte_cdk.qa.connector import Connector, ConnectorLanguage
from airbyte_cdk.qa.models import Check, CheckCategory, CheckResult


class PackagingCheck(Check):
    """Base class for packaging checks."""

    category = CheckCategory.PACKAGING


class CheckConnectorUsesPoetry(PackagingCheck):
    """Check that connectors use Poetry for dependency management."""

    name = "Connectors must use Poetry for dependency management"
    description = "Connectors must use [Poetry](https://python-poetry.org/) for dependency management. This is to ensure that all connectors use a dependency management tool which locks dependencies and ensures reproducible installs."
    requires_metadata = False
    runs_on_released_connectors = False
    applies_to_connector_languages = [
        ConnectorLanguage.PYTHON,
        ConnectorLanguage.LOW_CODE,
    ]

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        if not (connector.code_directory / consts.PYPROJECT_FILE_NAME).exists():
            return self.create_check_result(
                connector=connector,
                passed=False,
                message=f"{consts.PYPROJECT_FILE_NAME} file is missing",
            )
        if not (connector.code_directory / consts.POETRY_LOCK_FILE_NAME).exists():
            return self.fail(connector=connector, message=f"{consts.POETRY_LOCK_FILE_NAME} file is missing")
        if (connector.code_directory / "setup.py").exists():
            return self.fail(
                connector=connector,
                message=f"setup.py file exists. Please remove it and use {consts.PYPROJECT_FILE_NAME} instead",
            )
        return self.pass_(
            connector=connector,
            message="Poetry is used for dependency management",
        )


class CheckPublishToPyPiIsDeclared(PackagingCheck):
    """Check that Python connectors have PyPi publishing declared."""

    name = "Python connectors must have PyPi publishing declared."
    description = f"Python connectors must have [PyPi](https://pypi.org/) publishing enabled in their `{consts.METADATA_FILE_NAME}` file. This is declared by setting `remoteRegistries.pypi.enabled` to `true` in {consts.METADATA_FILE_NAME}. This is to ensure that all connectors can be published to PyPi and can be used in `PyAirbyte`."
    applies_to_connector_languages = [
        ConnectorLanguage.PYTHON,
        ConnectorLanguage.LOW_CODE,
    ]
    applies_to_connector_types = ["source"]

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        publish_to_pypi_is_enabled = get(connector.metadata, "remoteRegistries.pypi.enabled")
        if publish_to_pypi_is_enabled is None:
            return self.create_check_result(
                connector=connector,
                passed=False,
                message=f"PyPi publishing is not declared. Please set it in the {consts.METADATA_FILE_NAME} file",
            )
        return self.create_check_result(connector=connector, passed=True, message="PyPi publishing is declared")


class CheckManifestOnlyConnectorBaseImage(PackagingCheck):
    """Check that manifest-only connectors use the correct base image."""

    name = "Manifest-only connectors must use `source-declarative-manifest` as their base image"
    description = "Manifest-only connectors must use `airbyte/source-declarative-manifest` as their base image."
    applies_to_connector_languages = [ConnectorLanguage.MANIFEST_ONLY]

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        base_image = get(connector.metadata, "connectorBuildOptions.baseImage")
        base_image_name = base_image.split(":")[0] if base_image else None

        if base_image_name != "docker.io/airbyte/source-declarative-manifest":
            return self.create_check_result(
                connector=connector,
                passed=False,
                message=f"A manifest-only connector must use `source-declarative-manifest` base image. Replace the base image in {consts.METADATA_FILE_NAME} file",
            )
        return self.create_check_result(connector=connector, passed=True, message="Connector uses source-declarative-manifest base image")


class CheckConnectorLicense(PackagingCheck):
    """Check that connectors are licensed under MIT or Elv2."""

    name = "Connectors must be licensed under MIT or Elv2"
    description = "Connectors must be licensed under the MIT or Elv2 license. This is to ensure that all connectors are licensed under a permissive license."

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        VALID_LICENSES = ["MIT", "ELV2"]
        metadata_license = get(connector.metadata, "license")
        if metadata_license is None:
            return self.fail(
                connector=connector,
                message="License is missing in the metadata file",
            )
        elif metadata_license.upper() not in VALID_LICENSES:
            return self.fail(
                connector=connector,
                message=f"Connector is not using a valid license. Please use any of: {', '.join(VALID_LICENSES)}",
            )
        else:
            return self.pass_(
                connector=connector,
                message=f"Connector is licensed under {metadata_license}",
            )


class CheckConnectorLicenseMatchInPyproject(PackagingCheck):
    """Check that connector license in metadata.yaml and pyproject.toml match."""

    name = f"Connector license in {consts.METADATA_FILE_NAME} and {consts.PYPROJECT_FILE_NAME} file must match"
    description = f"Connectors license in {consts.METADATA_FILE_NAME} and {consts.PYPROJECT_FILE_NAME} file must match. This is to ensure that all connectors are consistently licensed."
    applies_to_connector_languages = [
        ConnectorLanguage.PYTHON,
        ConnectorLanguage.LOW_CODE,
    ]

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        metadata_license = get(connector.metadata, "license")
        if metadata_license is None:
            return self.fail(
                connector=connector,
                message=f"License is missing in the {consts.METADATA_FILE_NAME} file",
            )
        if not (connector.code_directory / consts.PYPROJECT_FILE_NAME).exists():
            return self.fail(
                connector=connector,
                message=f"{consts.PYPROJECT_FILE_NAME} file is missing",
            )
        try:
            pyproject = toml.load((connector.code_directory / consts.PYPROJECT_FILE_NAME))
        except toml.TomlDecodeError:
            return self.fail(
                connector=connector,
                message=f"{consts.PYPROJECT_FILE_NAME} is invalid toml file",
            )

        poetry_license = get(pyproject, "tool.poetry.license")

        if poetry_license is None:
            return self.fail(
                connector=connector,
                message=f"Connector is missing license in {consts.PYPROJECT_FILE_NAME}. Please add it",
            )

        if poetry_license.lower() != metadata_license.lower():
            return self.fail(
                connector=connector,
                message=f"Connector is licensed under {poetry_license} in {consts.PYPROJECT_FILE_NAME}, but licensed under {metadata_license} in {consts.METADATA_FILE_NAME}. These two files have to be consistent",
            )

        return self.pass_(
            connector=connector,
            message=f"License in {consts.METADATA_FILE_NAME} and {consts.PYPROJECT_FILE_NAME} file match",
        )


class CheckConnectorVersionMatchInPyproject(PackagingCheck):
    """Check that connector version in metadata.yaml and pyproject.toml match."""

    name = f"Connector version in {consts.METADATA_FILE_NAME} and {consts.PYPROJECT_FILE_NAME} file must match"
    description = f"Connector version in {consts.METADATA_FILE_NAME} and {consts.PYPROJECT_FILE_NAME} file must match. This is to ensure that connector release is consistent."
    applies_to_connector_languages = [
        ConnectorLanguage.PYTHON,
        ConnectorLanguage.LOW_CODE,
    ]

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        metadata_version = get(connector.metadata, "dockerImageTag")
        if metadata_version is None:
            return self.fail(
                connector=connector,
                message=f"dockerImageTag field is missing in the {consts.METADATA_FILE_NAME} file",
            )

        if not (connector.code_directory / consts.PYPROJECT_FILE_NAME).exists():
            return self.fail(
                connector=connector,
                message=f"{consts.PYPROJECT_FILE_NAME} file is missing",
            )

        try:
            pyproject = toml.load((connector.code_directory / consts.PYPROJECT_FILE_NAME))
        except toml.TomlDecodeError:
            return self.fail(
                connector=connector,
                message=f"{consts.PYPROJECT_FILE_NAME} is invalid toml file",
            )

        poetry_version = get(pyproject, "tool.poetry.version")

        if poetry_version is None:
            return self.fail(
                connector=connector,
                message=f"Version field is missing in the {consts.PYPROJECT_FILE_NAME} file",
            )

        if poetry_version != metadata_version:
            return self.fail(
                connector=connector,
                message=f"Version is {metadata_version} in {consts.METADATA_FILE_NAME}, but version is {poetry_version} in {consts.PYPROJECT_FILE_NAME}. These two files have to be consistent",
            )

        return self.pass_(
            connector=connector,
            message=f"Version in {consts.METADATA_FILE_NAME} and {consts.PYPROJECT_FILE_NAME} file match",
        )


class CheckVersionFollowsSemver(PackagingCheck):
    """Check that connector version follows Semantic Versioning."""

    name = "Connector version must follow Semantic Versioning"
    description = "Connector version must follow the Semantic Versioning scheme. This is to ensure that all connectors follow a consistent versioning scheme."

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        if not connector.metadata or "dockerImageTag" not in connector.metadata:
            return self.create_check_result(
                connector=connector,
                passed=False,
                message=f"dockerImageTag is missing in {consts.METADATA_FILE_NAME}",
            )
        try:
            semver.Version.parse(str(connector.metadata["dockerImageTag"]))
        except ValueError:
            return self.create_check_result(
                connector=connector,
                passed=False,
                message=f"Connector version {connector.metadata['dockerImageTag']} does not follow semantic versioning",
            )
        return self.create_check_result(
            connector=connector,
            passed=True,
            message="Connector version follows semantic versioning",
        )


class CheckVersionBump(PackagingCheck):
    """Check that connector version has been bumped."""

    name = "Connector version must be bumped"
    description = "Connector version must be bumped when making changes to the connector. This is to ensure that connector releases are properly versioned."
    requires_metadata = True

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        if not connector.metadata or "dockerImageTag" not in connector.metadata:
            return self.create_check_result(
                connector=connector,
                passed=False,
                message=f"dockerImageTag is missing in {consts.METADATA_FILE_NAME}",
            )
        
        try:
            semver.Version.parse(str(connector.metadata["dockerImageTag"]))
        except ValueError:
            return self.create_check_result(
                connector=connector,
                passed=False,
                message=f"Connector version {connector.metadata['dockerImageTag']} does not follow semantic versioning",
            )
        
        return self.create_check_result(
            connector=connector,
            passed=True,
            message="Connector version is valid. Note: This check does not verify if the version has been bumped from the previous version.",
        )


ENABLED_CHECKS = [
    CheckConnectorUsesPoetry(),
    CheckConnectorLicense(),
    CheckConnectorLicenseMatchInPyproject(),
    CheckVersionFollowsSemver(),
    CheckConnectorVersionMatchInPyproject(),
    CheckPublishToPyPiIsDeclared(),
    CheckManifestOnlyConnectorBaseImage(),
    CheckVersionBump(),  # Added version bump check
]
