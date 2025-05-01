"""Packaging checks for Airbyte connectors implemented as pytest tests."""

import os
import re
from pathlib import Path

import pytest
import semver
import toml
import yaml

from airbyte_cdk.qa import consts
from airbyte_cdk.qa.connector import Connector, ConnectorLanguage


@pytest.mark.qa_check
@pytest.mark.check_category("packaging")
class TestPackaging:
    """Test class for packaging checks."""

    @pytest.mark.parametrize("connector_fixture", ["connector"], indirect=True)
    @pytest.mark.connector_language(["python"])
    def test_connector_uses_poetry(self, connector: Connector) -> None:
        """Check that Python connectors use Poetry for dependency management.

        Args:
            connector: The connector to check
        """
        assert connector.pyproject_file_path.exists(), f"Python connector must use Poetry. {consts.PYPROJECT_FILE_NAME} file not found."
        
        try:
            with open(connector.pyproject_file_path, "r") as f:
                pyproject = toml.load(f)
        except Exception as e:
            pytest.fail(f"Failed to parse {consts.PYPROJECT_FILE_NAME}: {str(e)}")
            
        assert "tool" in pyproject, f"{consts.PYPROJECT_FILE_NAME} must have a [tool] section"
        assert "poetry" in pyproject["tool"], f"{consts.PYPROJECT_FILE_NAME} must have a [tool.poetry] section"


    @pytest.mark.parametrize("connector_fixture", ["connector"], indirect=True)
    @pytest.mark.connector_language(["python"])
    def test_version_bump(self, connector: Connector) -> None:
        """Check that the connector version has been bumped.

        Args:
            connector: The connector to check
        """
        current_version = connector.version
        assert current_version is not None, "Could not determine current version from metadata"
        
        try:
            import subprocess
            
            cmd = ["git", "show", "HEAD~1:metadata.yaml"]
            process = subprocess.run(cmd, cwd=connector.code_directory, capture_output=True, text=True)
            
            if process.returncode != 0:
                pytest.skip("Could not get previous version from git history")
                
            previous_metadata = yaml.safe_load(process.stdout)
            previous_version = previous_metadata.get("data", {}).get("dockerImageTag")
            
            if not previous_version:
                pytest.skip("Could not determine previous version from git history")
                
            try:
                current_semver = semver.Version.parse(current_version)
                previous_semver = semver.Version.parse(previous_version)
                
                assert current_semver > previous_semver, f"Version must be bumped. Current: {current_version}, Previous: {previous_version}"
            except ValueError:
                assert current_version != previous_version, f"Version must be bumped. Current: {current_version}, Previous: {previous_version}"
                
        except Exception as e:
            pytest.skip(f"Could not check version bump: {str(e)}")


    @pytest.mark.parametrize("connector_fixture", ["connector"], indirect=True)
    @pytest.mark.connector_language(["python"])
    def test_license_in_pyproject(self, connector: Connector) -> None:
        """Check that Python connectors have a license in pyproject.toml.

        Args:
            connector: The connector to check
        """
        assert connector.pyproject_file_path.exists(), f"{consts.PYPROJECT_FILE_NAME} file not found"
        
        try:
            with open(connector.pyproject_file_path, "r") as f:
                pyproject = toml.load(f)
        except Exception as e:
            pytest.fail(f"Failed to parse {consts.PYPROJECT_FILE_NAME}: {str(e)}")
            
        assert "tool" in pyproject, f"{consts.PYPROJECT_FILE_NAME} must have a [tool] section"
        assert "poetry" in pyproject["tool"], f"{consts.PYPROJECT_FILE_NAME} must have a [tool.poetry] section"
        assert "license" in pyproject["tool"]["poetry"], f"{consts.PYPROJECT_FILE_NAME} must have a license field"
        
        license_value = pyproject["tool"]["poetry"]["license"]
        assert license_value.lower() in ["mit", "elv2", "elastic license v2"], f"License must be MIT, ELv2, or Elastic License v2, got {license_value}"
