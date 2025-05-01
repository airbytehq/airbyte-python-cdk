"""Tests for the Connector class."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from airbyte_cdk.qa.connector import Connector, ConnectorLanguage


class TestConnector:
    """Tests for the Connector class."""

    def test_technical_name(self):
        """Test the technical_name property."""
        connector = Connector("source-test")
        assert connector.technical_name == "source-test"

    def test_name(self):
        """Test the name property."""
        connector = Connector("source-test")
        assert connector.name == "source-test"

    def test_connector_type(self):
        """Test the connector_type property."""
        connector = Connector("source-test")
        assert connector.connector_type == "source"
        connector = Connector("destination-test")
        assert connector.connector_type == "destination"

    def test_code_directory(self):
        """Test the code_directory property."""
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.code_directory == Path("/path/to/connector")

    def test_metadata_file_path(self):
        """Test the metadata_file_path property."""
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.metadata_file_path == Path("/path/to/connector/metadata.yaml")

    @patch("airbyte_cdk.qa.connector.yaml.safe_load")
    def test_metadata(self, mock_safe_load):
        """Test the metadata property."""
        mock_metadata = {"data": {"name": "Test Connector"}}
        mock_safe_load.return_value = mock_metadata
        
        with patch("builtins.open", MagicMock()):
            with patch("pathlib.Path.exists", return_value=True):
                connector = Connector("source-test", Path("/path/to/connector"))
                assert connector.metadata == mock_metadata
                assert connector.metadata == mock_metadata
                mock_safe_load.assert_called_once()

    def test_metadata_file_not_exists(self):
        """Test the metadata property when the metadata file doesn't exist."""
        with patch("pathlib.Path.exists", return_value=False):
            connector = Connector("source-test", Path("/path/to/connector"))
            assert connector.metadata is None

    def test_language_python(self):
        """Test the language property for Python connectors."""
        with patch("pathlib.Path.exists", side_effect=lambda p: p.name == "pyproject.toml"):
            connector = Connector("source-test", Path("/path/to/connector"))
            assert connector.language == ConnectorLanguage.PYTHON

    def test_language_java(self):
        """Test the language property for Java connectors."""
        with patch("pathlib.Path.exists", side_effect=lambda p: "src/main/java" in str(p)):
            connector = Connector("source-test", Path("/path/to/connector"))
            assert connector.language == ConnectorLanguage.JAVA

    def test_language_low_code(self):
        """Test the language property for low-code connectors."""
        with patch("pathlib.Path.exists", side_effect=lambda p: p.name == "manifest.yaml"):
            connector = Connector("source-test", Path("/path/to/connector"))
            assert connector.language == ConnectorLanguage.LOW_CODE

    def test_language_unknown(self):
        """Test the language property for unknown connectors."""
        with patch("pathlib.Path.exists", return_value=False):
            connector = Connector("source-test", Path("/path/to/connector"))
            assert connector.language is None

    def test_manifest_path(self):
        """Test the manifest_path property."""
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.manifest_path == Path("/path/to/connector/manifest.yaml")

    @patch("airbyte_cdk.qa.connector.Connector.metadata", new_callable=MagicMock)
    def test_version(self, mock_metadata):
        """Test the version property."""
        mock_metadata.get.return_value = {"data": {"dockerImageTag": "1.0.0"}}
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.version == "1.0.0"

    def test_version_in_dockerfile_label(self):
        """Test the version_in_dockerfile_label property."""
        dockerfile_content = 'FROM python:3.9\nLABEL io.airbyte.version="1.0.0"\n'
        with patch("builtins.open", MagicMock(return_value=MagicMock(read=MagicMock(return_value=dockerfile_content)))):
            with patch("pathlib.Path.exists", return_value=True):
                connector = Connector("source-test", Path("/path/to/connector"))
                assert connector.version_in_dockerfile_label == "1.0.0"

    def test_version_in_dockerfile_label_no_dockerfile(self):
        """Test the version_in_dockerfile_label property when the Dockerfile doesn't exist."""
        with patch("pathlib.Path.exists", return_value=False):
            connector = Connector("source-test", Path("/path/to/connector"))
            assert connector.version_in_dockerfile_label is None

    @patch("airbyte_cdk.qa.connector.Connector.metadata", new_callable=MagicMock)
    def test_name_from_metadata(self, mock_metadata):
        """Test the name_from_metadata property."""
        mock_metadata.get.return_value = {"data": {"name": "Test Connector"}}
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.name_from_metadata == "Test Connector"

    @patch("airbyte_cdk.qa.connector.Connector.metadata", new_callable=MagicMock)
    def test_support_level(self, mock_metadata):
        """Test the support_level property."""
        mock_metadata.get.return_value = {"data": {"supportLevel": "certified"}}
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.support_level == "certified"

    @patch("airbyte_cdk.qa.connector.Connector.metadata", new_callable=MagicMock)
    def test_cloud_usage(self, mock_metadata):
        """Test the cloud_usage property."""
        mock_metadata.get.return_value = {"data": {"cloudUsage": "enabled"}}
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.cloud_usage == "enabled"

    @patch("airbyte_cdk.qa.connector.Connector.metadata", new_callable=MagicMock)
    def test_ab_internal_sl(self, mock_metadata):
        """Test the ab_internal_sl property."""
        mock_metadata.get.return_value = {"data": {"ab_internal": {"sl": 200}}}
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.ab_internal_sl == 200

    def test_is_released(self):
        """Test the is_released property."""
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.is_released is True

    def test_pyproject_file_path(self):
        """Test the pyproject_file_path property."""
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.pyproject_file_path == Path("/path/to/connector/pyproject.toml")

    def test_dockerfile_file_path(self):
        """Test the dockerfile_file_path property."""
        connector = Connector("source-test", Path("/path/to/connector"))
        assert connector.dockerfile_file_path == Path("/path/to/connector/Dockerfile")

    def test_has_dockerfile(self):
        """Test the has_dockerfile property."""
        with patch("pathlib.Path.exists", return_value=True):
            connector = Connector("source-test", Path("/path/to/connector"))
            assert connector.has_dockerfile is True
