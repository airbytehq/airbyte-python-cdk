"""Metadata checks for Airbyte connectors implemented as pytest tests."""

import os
from datetime import datetime, timedelta

import pytest
import toml
import yaml

from airbyte_cdk.qa import consts
from airbyte_cdk.qa.connector import Connector, ConnectorLanguage


@pytest.mark.qa_check
@pytest.mark.check_category("metadata")
@pytest.mark.requires_metadata
class TestMetadata:
    """Test class for metadata checks."""

    @pytest.mark.parametrize("connector_fixture", ["connector"], indirect=True)
    def test_validate_metadata(self, connector: Connector) -> None:
        """Check that connectors have a valid metadata.yaml file.

        Args:
            connector: The connector to check
        """
        assert connector.metadata_file_path.exists(), f"Metadata file {consts.METADATA_FILE_NAME} does not exist"
        
        try:
            with open(connector.metadata_file_path, "r") as f:
                yaml.safe_load(f)
        except yaml.YAMLError as e:
            pytest.fail(f"Metadata file is invalid YAML: {str(e)}")


    @pytest.mark.parametrize("connector_fixture", ["connector"], indirect=True)
    def test_connector_language_tag(self, connector: Connector) -> None:
        """Check that connectors have a language tag in metadata.

        Args:
            connector: The connector to check
        """
        PYTHON_LANGUAGE_TAG = "language:python"
        JAVA_LANGUAGE_TAG = "language:java"
        MANIFEST_ONLY_LANGUAGE_TAG = "language:manifest-only"

        def get_expected_language_tag(connector: Connector) -> str:
            """Get the expected language tag for the connector.

            Args:
                connector: The connector to check

            Returns:
                str: The expected language tag

            Raises:
                ValueError: If the language tag cannot be inferred
            """
            if (connector.code_directory / "manifest.yaml").exists():
                return MANIFEST_ONLY_LANGUAGE_TAG
            if (connector.code_directory / "setup.py").exists() or (
                connector.code_directory / consts.PYPROJECT_FILE_NAME
            ).exists():
                return PYTHON_LANGUAGE_TAG
            elif (connector.code_directory / "build.gradle").exists() or (
                connector.code_directory / "build.gradle.kts"
            ).exists():
                return JAVA_LANGUAGE_TAG
            else:
                raise ValueError("Could not infer the language tag from the connector directory")

        try:
            expected_language_tag = get_expected_language_tag(connector)
        except ValueError:
            pytest.fail("Could not infer the language tag from the connector directory")

        current_language_tags = [t for t in (connector.metadata.get("tags", []) if connector.metadata else []) if t.startswith("language:")]
        assert current_language_tags, "Language tag is missing in the metadata file"
        assert len(current_language_tags) == 1, f"Multiple language tags found in the metadata file: {current_language_tags}"
        
        current_language_tag = current_language_tags[0]
        assert current_language_tag == expected_language_tag, f"Expected language tag '{expected_language_tag}' in the {consts.METADATA_FILE_NAME} file, but found '{current_language_tag}'"


    @pytest.mark.parametrize("connector_fixture", ["connector"], indirect=True)
    @pytest.mark.connector_language(["python", "low-code"])
    def test_connector_cdk_tag(self, connector: Connector) -> None:
        """Check that Python connectors have a CDK tag in metadata.

        Args:
            connector: The connector to check
        """
        class CDKTag:
            """CDK tag values."""

            LOW_CODE = "cdk:low-code"
            PYTHON = "cdk:python"
            FILE = "cdk:python-file-based"

        def get_expected_cdk_tag(connector: Connector) -> str:
            """Get the expected CDK tag for the connector.

            Args:
                connector: The connector to check

            Returns:
                str: The expected CDK tag
            """
            manifest_file = connector.code_directory / connector.technical_name.replace("-", "_") / "manifest.yaml"
            pyproject_file = connector.code_directory / consts.PYPROJECT_FILE_NAME
            setup_py_file = connector.code_directory / "setup.py"
            if manifest_file.exists():
                return CDKTag.LOW_CODE
            if pyproject_file.exists():
                pyproject = toml.load((connector.code_directory / consts.PYPROJECT_FILE_NAME))
                cdk_deps = pyproject["tool"]["poetry"]["dependencies"].get("airbyte-cdk", None)
                if cdk_deps and isinstance(cdk_deps, dict) and "file-based" in cdk_deps.get("extras", []):
                    return CDKTag.FILE
            if setup_py_file.exists():
                if "airbyte-cdk[file-based]" in (connector.code_directory / "setup.py").read_text():
                    return CDKTag.FILE
            return CDKTag.PYTHON

        current_cdk_tags = [t for t in (connector.metadata.get("tags", []) if connector.metadata else []) if t.startswith("cdk:")]
        expected_cdk_tag = get_expected_cdk_tag(connector)
        
        assert current_cdk_tags, "CDK tag is missing in the metadata file"
        assert len(current_cdk_tags) == 1, f"Multiple CDK tags found in the metadata file: {current_cdk_tags}"
        assert current_cdk_tags[0] == expected_cdk_tag, f"Expected CDK tag '{get_expected_cdk_tag(connector)}' in the {consts.METADATA_FILE_NAME} file, but found '{current_cdk_tags[0]}'"


    @pytest.mark.parametrize("connector_fixture", ["connector"], indirect=True)
    @pytest.mark.runs_on_released_connectors(False)
    def test_breaking_changes_deadlines(self, connector: Connector) -> None:
        """Check that breaking change deadlines are at least a week in the future.

        Args:
            connector: The connector to check
        """
        minimum_days_until_deadline = 7
        
        current_version = connector.version
        assert current_version is not None, "Can't verify breaking changes deadline: connector version is not defined."

        breaking_changes = (connector.metadata.get("releases", {}) if connector.metadata else {}).get("breakingChanges")

        if not breaking_changes:
            pytest.skip("No breaking changes found on this connector.")

        current_version_breaking_changes = breaking_changes.get(current_version)

        if not current_version_breaking_changes:
            pytest.skip("No breaking changes found for the current version.")

        upgrade_deadline = current_version_breaking_changes.get("upgradeDeadline")

        assert upgrade_deadline, f"No upgrade deadline found for the breaking changes in {current_version}."

        upgrade_deadline_datetime = datetime.strptime(upgrade_deadline, "%Y-%m-%d")
        one_week_from_now = datetime.utcnow() + timedelta(days=minimum_days_until_deadline)

        assert upgrade_deadline_datetime > one_week_from_now, f"The upgrade deadline for the breaking changes in {current_version} is less than {minimum_days_until_deadline} days from today. Please extend the deadline"


    @pytest.mark.parametrize("connector_fixture", ["connector"], indirect=True)
    @pytest.mark.connector_type(["source"])
    @pytest.mark.connector_support_level(["certified"])
    def test_connector_max_seconds_between_messages_value(self, connector: Connector) -> None:
        """Check that certified source connectors have a maxSecondsBetweenMessages value.

        Args:
            connector: The connector to check
        """
        max_seconds_between_messages = connector.metadata.get("maxSecondsBetweenMessages") if connector.metadata else None
        assert max_seconds_between_messages, "Missing required for certified connectors field 'maxSecondsBetweenMessages'"
