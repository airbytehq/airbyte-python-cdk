from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from airbyte_cdk.cli.airbyte_cdk._secrets import (
    _write_secret_file,
    fetch,
    secretmanager,
)
from airbyte_cdk.cli.airbyte_cdk.exceptions import ConnectorSecretWithNoValidVersionsError


class TestWriteSecretFile:
    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def mock_secret(self):
        secret = MagicMock()
        secret.name = "projects/test-project/secrets/test-secret"
        return secret

    @pytest.fixture
    def mock_file_path(self, tmp_path):
        return tmp_path / "test_secret.json"

    def test_write_secret_file_with_enabled_version(self, mock_client, mock_secret, mock_file_path):
        # Mock list_secret_versions to return an enabled version
        mock_version = MagicMock()
        mock_version.name = f"{mock_secret.name}/versions/1"
        mock_client.list_secret_versions.return_value = [mock_version]

        # Mock access_secret_version to return a payload
        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = '{"key": "value"}'
        mock_client.access_secret_version.return_value = mock_response

        # Call the function
        _write_secret_file(
            secret=mock_secret,
            client=mock_client,
            file_path=mock_file_path,
            connector_name="test-connector",
            gcp_project_id="test-project",
        )

        # Verify that list_secret_versions was called with the correct parameters
        mock_client.list_secret_versions.assert_called_once()
        assert "state:ENABLED" in str(mock_client.list_secret_versions.call_args)

        # Verify that access_secret_version was called with the correct version
        mock_client.access_secret_version.assert_called_once_with(name=mock_version.name)

        # Verify that the file was created with the correct content
        assert mock_file_path.read_text() == '{"key": "value"}'

    def test_write_secret_file_with_no_enabled_versions(
        self, mock_client, mock_secret, mock_file_path
    ):
        # Mock list_secret_versions to return an empty list (no enabled versions)
        mock_client.list_secret_versions.return_value = []

        # Call the function and expect an exception
        with pytest.raises(ConnectorSecretWithNoValidVersionsError) as excinfo:
            _write_secret_file(
                secret=mock_secret,
                client=mock_client,
                file_path=mock_file_path,
                connector_name="test-connector",
                gcp_project_id="test-project",
            )

        # Verify that list_secret_versions was called with the correct parameters
        mock_client.list_secret_versions.assert_called_once()
        assert "state:ENABLED" in str(mock_client.list_secret_versions.call_args)

        # Verify that access_secret_version was not called
        mock_client.access_secret_version.assert_not_called()

        # Verify that the file was not created
        assert not mock_file_path.exists()

        # Verify the exception details
        assert excinfo.value.secret_name == "test-secret"
        assert excinfo.value.connector_name == "test-connector"
        assert excinfo.value.gcp_project_id == "test-project"


@patch("airbyte_cdk.cli.airbyte_cdk._secrets._get_gsm_secrets_client")
@patch("airbyte_cdk.cli.airbyte_cdk._secrets.resolve_connector_name_and_directory")
@patch("airbyte_cdk.cli.airbyte_cdk._secrets._get_secrets_dir")
@patch("airbyte_cdk.cli.airbyte_cdk._secrets._fetch_secret_handles")
class TestFetch:
    def test_fetch_with_some_failed_secrets(
        self,
        mock_fetch_secret_handles,
        mock_get_secrets_dir,
        mock_resolve,
        mock_get_client,
        tmp_path,
    ):
        # Setup mocks
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_resolve.return_value = ("test-connector", tmp_path)

        secrets_dir = tmp_path / "secrets"
        mock_get_secrets_dir.return_value = secrets_dir

        # Create two secrets, one that will succeed and one that will fail
        secret1 = MagicMock()
        secret1.name = "projects/test-project/secrets/test-secret-1"
        secret1.labels = {}

        secret2 = MagicMock()
        secret2.name = "projects/test-project/secrets/test-secret-2"
        secret2.labels = {}

        mock_fetch_secret_handles.return_value = [secret1, secret2]

        # Mock _write_secret_file to succeed for secret1 and fail for secret2
        with patch(
            "airbyte_cdk.cli.airbyte_cdk._secrets._write_secret_file"
        ) as mock_write_secret_file:
            # First call succeeds, second call raises exception
            mock_write_secret_file.side_effect = [
                None,  # Success for secret1
                ConnectorSecretWithNoValidVersionsError(
                    connector_name="test-connector",
                    secret_name="test-secret-2",
                    gcp_project_id="test-project",
                ),  # Failure for secret2
            ]

            # Call the function
            runner = CliRunner()
            result = runner.invoke(fetch)

            # Verify that _write_secret_file was called twice
            assert mock_write_secret_file.call_count == 2

            # Verify that the error message was printed
            assert "Failed to retrieve secret 'test-secret-2'" in result.output
            assert "Failed to retrieve 1 secret(s)" in result.output

            # Verify that the function did not raise an exception
            assert result.exit_code == 0

    def test_fetch_with_all_failed_secrets(
        self,
        mock_fetch_secret_handles,
        mock_get_secrets_dir,
        mock_resolve,
        mock_get_client,
        tmp_path,
    ):
        # Setup mocks
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_resolve.return_value = ("test-connector", tmp_path)

        secrets_dir = tmp_path / "secrets"
        mock_get_secrets_dir.return_value = secrets_dir

        # Create two secrets that will both fail
        secret1 = MagicMock()
        secret1.name = "projects/test-project/secrets/test-secret-1"
        secret1.labels = {}

        secret2 = MagicMock()
        secret2.name = "projects/test-project/secrets/test-secret-2"
        secret2.labels = {}

        mock_fetch_secret_handles.return_value = [secret1, secret2]

        # Mock _write_secret_file to fail for both secrets
        with patch(
            "airbyte_cdk.cli.airbyte_cdk._secrets._write_secret_file"
        ) as mock_write_secret_file:
            mock_write_secret_file.side_effect = [
                ConnectorSecretWithNoValidVersionsError(
                    connector_name="test-connector",
                    secret_name="test-secret-1",
                    gcp_project_id="test-project",
                ),  # Failure for secret1
                ConnectorSecretWithNoValidVersionsError(
                    connector_name="test-connector",
                    secret_name="test-secret-2",
                    gcp_project_id="test-project",
                ),  # Failure for secret2
            ]

            # Call the function
            runner = CliRunner()
            result = runner.invoke(fetch)

            # Verify that _write_secret_file was called twice
            assert mock_write_secret_file.call_count == 2

            # Verify that the error message was printed
            assert "Failed to retrieve secret 'test-secret-1'" in result.output
            assert "Failed to retrieve secret 'test-secret-2'" in result.output
            assert "Failed to retrieve 2 secret(s)" in result.output

            # Verify that the function raised an exception
            assert result.exit_code != 0
