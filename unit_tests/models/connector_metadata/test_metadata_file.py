# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Tests for metadata file validation."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from airbyte_cdk.models.connector_metadata import (
    ValidationResult,
    get_metadata_schema,
    validate_metadata_file,
)


@pytest.fixture
def test_schema_path():
    """Path to test schema file."""
    return Path(__file__).parent / "test_schema.json"


@pytest.fixture
def test_schema(test_schema_path):
    """Load test schema."""
    return json.loads(test_schema_path.read_text())


@pytest.fixture
def valid_metadata_file(tmp_path):
    """Create a valid metadata.yaml file."""
    metadata_path = tmp_path / "metadata.yaml"
    metadata_path.write_text(
        """
data:
  dockerRepository: airbyte/source-test
  dockerImageTag: 0.1.0
  tags:
    - language:python
"""
    )
    return metadata_path


@pytest.fixture
def invalid_metadata_file(tmp_path):
    """Create an invalid metadata.yaml file (missing required field)."""
    metadata_path = tmp_path / "metadata.yaml"
    metadata_path.write_text(
        """
data:
  dockerRepository: airbyte/source-test
"""
    )
    return metadata_path


class TestGetMetadataSchema:
    """Tests for get_metadata_schema function."""

    def test_load_from_file_path(self, test_schema_path, test_schema):
        """Test loading schema from file path."""
        schema = get_metadata_schema(test_schema_path)
        assert schema == test_schema

    def test_load_from_string_path(self, test_schema_path, test_schema):
        """Test loading schema from string path."""
        schema = get_metadata_schema(str(test_schema_path))
        assert schema == test_schema

    def test_file_not_found(self, tmp_path):
        """Test error when schema file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            get_metadata_schema(tmp_path / "nonexistent.json")

    @patch("airbyte_cdk.models.connector_metadata.metadata_file.urlopen")
    def test_load_from_url(self, mock_urlopen, test_schema):
        """Test loading schema from URL."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(test_schema).encode("utf-8")
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        schema = get_metadata_schema("https://example.com/schema.json")
        assert schema == test_schema
        mock_urlopen.assert_called_once()

    @patch("airbyte_cdk.models.connector_metadata.metadata_file.urlopen")
    def test_url_fetch_error(self, mock_urlopen):
        """Test error when URL fetch fails."""
        mock_urlopen.side_effect = Exception("Network error")

        with pytest.raises(RuntimeError, match="Failed to fetch schema"):
            get_metadata_schema("https://example.com/schema.json")


class TestValidateMetadataFile:
    """Tests for validate_metadata_file function."""

    def test_valid_metadata(self, valid_metadata_file, test_schema_path):
        """Test validation of valid metadata file."""
        result = validate_metadata_file(valid_metadata_file, test_schema_path)
        assert isinstance(result, ValidationResult)
        assert result.valid is True
        assert len(result.errors) == 0
        assert result.metadata is not None

    def test_invalid_metadata_missing_field(self, invalid_metadata_file, test_schema_path):
        """Test validation of invalid metadata file (missing required field)."""
        result = validate_metadata_file(invalid_metadata_file, test_schema_path)
        assert isinstance(result, ValidationResult)
        assert result.valid is False
        assert len(result.errors) > 0
        assert result.metadata is not None

    def test_file_not_found(self, tmp_path, test_schema_path):
        """Test validation when metadata file doesn't exist."""
        result = validate_metadata_file(tmp_path / "nonexistent.yaml", test_schema_path)
        assert result.valid is False
        assert len(result.errors) == 1
        assert result.errors[0]["type"] == "file_not_found"

    def test_invalid_yaml(self, tmp_path, test_schema_path):
        """Test validation when YAML is malformed."""
        metadata_path = tmp_path / "metadata.yaml"
        metadata_path.write_text("invalid: yaml: content: [")
        result = validate_metadata_file(metadata_path, test_schema_path)
        assert result.valid is False
        assert len(result.errors) == 1
        assert result.errors[0]["type"] == "yaml_parse_error"

    def test_missing_data_field(self, tmp_path, test_schema_path):
        """Test validation when 'data' field is missing."""
        metadata_path = tmp_path / "metadata.yaml"
        metadata_path.write_text("notdata: {}")
        result = validate_metadata_file(metadata_path, test_schema_path)
        assert result.valid is False
        assert len(result.errors) == 1
        assert result.errors[0]["type"] == "missing_field"

    def test_schema_load_error(self, valid_metadata_file, tmp_path):
        """Test validation when schema can't be loaded."""
        result = validate_metadata_file(valid_metadata_file, tmp_path / "nonexistent.json")
        assert result.valid is False
        assert len(result.errors) == 1
        assert result.errors[0]["type"] == "schema_load_error"

    @patch("airbyte_cdk.models.connector_metadata.metadata_file.get_metadata_schema")
    def test_default_schema_url(self, mock_get_schema, valid_metadata_file, test_schema):
        """Test that default schema URL is used when none provided."""
        mock_get_schema.return_value = test_schema
        result = validate_metadata_file(valid_metadata_file)
        mock_get_schema.assert_called_once_with(None)
