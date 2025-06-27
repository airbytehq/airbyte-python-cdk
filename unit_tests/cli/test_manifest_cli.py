"""Tests for the manifest CLI commands."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from airbyte_cdk.cli.airbyte_cdk._manifest import manifest_cli_group
from airbyte_cdk.sources.declarative.parsers.manifest_normalizer import ManifestNormalizer


class TestManifestValidateCommand:
    """Test cases for the manifest validate command."""

    def test_validate_valid_manifest_up_to_date(self, tmp_path: Path) -> None:
        """Test validate command with a valid manifest that needs migration (exit code 0 without --strict, 1 with --strict)."""
        manifest_content = {
            "version": "0.29.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()

        result = runner.invoke(
            manifest_cli_group, ["validate", "--manifest-path", str(manifest_file)]
        )
        assert result.exit_code == 0
        assert "✅ Manifest" in result.output
        assert "is valid and up to date" in result.output

        result_strict = runner.invoke(
            manifest_cli_group, ["validate", "--manifest-path", str(manifest_file), "--strict"]
        )
        assert result_strict.exit_code == 1
        assert "⚠️" in result_strict.output
        assert "could benefit from migration" in result_strict.output

    def test_validate_manifest_needs_migration(self, tmp_path: Path) -> None:
        """Test validate command with manifest that needs migration (exit code 0 without --strict, 1 with --strict)."""
        manifest_content = {
            "version": "0.1.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()

        result = runner.invoke(
            manifest_cli_group, ["validate", "--manifest-path", str(manifest_file)]
        )
        assert result.exit_code == 0
        assert "✅ Manifest" in result.output
        assert "is valid and up to date" in result.output

        result_strict = runner.invoke(
            manifest_cli_group, ["validate", "--manifest-path", str(manifest_file), "--strict"]
        )
        assert result_strict.exit_code == 1
        assert "⚠️" in result_strict.output
        assert "could benefit from migration" in result_strict.output

    def test_validate_manifest_unfixable_validation_errors(self, tmp_path: Path) -> None:
        """Test validate command with unfixable validation errors (exit code 2)."""
        manifest_content = {
            "version": "0.29.0",
            "type": "DeclarativeSource",
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["validate", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 2
        assert "❌ Validation failed" in result.output
        assert "'check' is a required property" in result.output

    def test_validate_manifest_file_not_found(self, tmp_path: Path) -> None:
        """Test validate command with non-existent manifest file (exit code 2 from Click)."""
        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["validate", "--manifest-path", str(tmp_path / "nonexistent.yaml")]
        )

        assert result.exit_code == 2
        assert "--manifest-path" in result.output
        assert "does not exist" in result.output

    def test_validate_manifest_invalid_yaml(self, tmp_path: Path) -> None:
        """Test validate command with invalid YAML (exit code 3)."""
        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            f.write("invalid: yaml: content: [")

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["validate", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 3
        assert "❌ Error: Invalid YAML" in result.output

    def test_validate_manifest_strict_flag_help(self, tmp_path: Path) -> None:
        """Test that the --strict flag appears in help text."""
        runner = CliRunner()
        result = runner.invoke(manifest_cli_group, ["validate", "--help"])

        assert result.exit_code == 0
        assert "--strict" in result.output
        assert "Enable strict mode" in result.output

    def test_validate_manifest_not_dict(self, tmp_path: Path) -> None:
        """Test validate command with YAML that's not a dictionary (exit code 3)."""
        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(["not", "a", "dict"], f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["validate", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 3
        assert "❌ Error: Manifest file" in result.output
        assert "does not contain a valid YAML dictionary" in result.output


class TestManifestMigrateCommand:
    """Test cases for the manifest migrate command."""

    def test_migrate_manifest_needs_migration(self, tmp_path: Path) -> None:
        """Test migrate command with manifest that needs migration (default stdout behavior)."""
        manifest_content = {
            "version": "0.1.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["migrate", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 0
        assert "version:" in result.output
        assert "type: DeclarativeSource" in result.output
        assert "applied_migrations:" in result.output

        with open(manifest_file, "r") as f:
            unchanged_content = yaml.safe_load(f)

        assert unchanged_content == manifest_content

    def test_migrate_manifest_already_up_to_date(self, tmp_path: Path) -> None:
        """Test migrate command with manifest that gets migrated to latest version (default stdout behavior)."""
        manifest_content = {
            "version": "0.29.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["migrate", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 0
        assert "version:" in result.output
        assert "type: DeclarativeSource" in result.output
        assert "applied_migrations:" in result.output

    def test_migrate_manifest_stdout_output(self, tmp_path: Path) -> None:
        """Test migrate command outputs to stdout by default."""
        manifest_content = {
            "version": "0.1.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        original_content = manifest_content.copy()

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["migrate", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 0
        assert "version:" in result.output
        assert "type: DeclarativeSource" in result.output

        with open(manifest_file, "r") as f:
            unchanged_content = yaml.safe_load(f)

        assert unchanged_content == original_content

    def test_migrate_manifest_in_place(self, tmp_path: Path) -> None:
        """Test migrate command with --in-place flag."""
        manifest_content = {
            "version": "0.1.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["migrate", "--manifest-path", str(manifest_file), "--in-place"]
        )

        assert result.exit_code == 0
        assert "Successfully migrated" in result.output

        with open(manifest_file, "r") as f:
            modified_content = yaml.safe_load(f)

        assert modified_content != manifest_content
        assert modified_content["version"] != "0.1.0"

    def test_migrate_manifest_exit_non_zero(self, tmp_path: Path) -> None:
        """Test migrate command with --exit-non-zero flag."""
        manifest_content = {
            "version": "0.1.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group,
            ["migrate", "--manifest-path", str(manifest_file), "--exit-non-zero"],
        )

        assert result.exit_code == 1
        assert "version:" in result.output

    def test_migrate_manifest_quiet(self, tmp_path: Path) -> None:
        """Test migrate command with --quiet flag."""
        manifest_content = {
            "version": "0.1.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["migrate", "--manifest-path", str(manifest_file), "--quiet"]
        )

        assert result.exit_code == 1
        assert "version:" in result.output
        assert "Successfully migrated" not in result.output

    def test_migrate_manifest_file_not_found(self, tmp_path: Path) -> None:
        """Test migrate command with non-existent manifest file (exit code 2 from Click)."""
        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["migrate", "--manifest-path", str(tmp_path / "nonexistent.yaml")]
        )

        assert result.exit_code == 2
        assert "--manifest-path" in result.output
        assert "does not exist" in result.output

    def test_migrate_manifest_invalid_yaml(self, tmp_path: Path) -> None:
        """Test migrate command with invalid YAML (exit code 3)."""
        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            f.write("invalid: yaml: content: [")

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["migrate", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 3
        assert "❌ Error: Invalid YAML" in result.output

    def test_migrate_manifest_not_dict(self, tmp_path: Path) -> None:
        """Test migrate command with YAML that's not a dictionary (exit code 3)."""
        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(["not", "a", "dict"], f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["migrate", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 3
        assert "❌ Error: Manifest file" in result.output
        assert "does not contain a valid YAML dictionary" in result.output


class TestManifestNormalizeCommand:
    """Test cases for the manifest normalize command."""

    def test_normalize_valid_manifest_no_changes(self, tmp_path: Path) -> None:
        """Test normalize command with a manifest that doesn't need normalization."""
        manifest_content = {
            "version": "0.29.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["normalize", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 0
        assert "✅ Manifest" in result.output
        assert "is already normalized" in result.output

    def test_normalize_manifest_stdout_output(self, tmp_path: Path) -> None:
        """Test normalize command outputs to stdout by default."""
        manifest_content = {
            "version": "0.29.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["normalize", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 0
        assert ("is already normalized" in result.output) or ("version:" in result.output)

    def test_normalize_manifest_in_place(self, tmp_path: Path) -> None:
        """Test normalize command with --in-place flag."""
        manifest_content = {
            "version": "0.29.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["normalize", "--manifest-path", str(manifest_file), "--in-place"]
        )

        assert result.exit_code == 0
        assert ("is already normalized" in result.output) or (
            "Successfully normalized" in result.output
        )

    def test_normalize_manifest_quiet(self, tmp_path: Path) -> None:
        """Test normalize command with --quiet flag."""
        manifest_content = {
            "version": "0.29.0",
            "type": "DeclarativeSource",
            "check": {"type": "CheckStream", "stream_names": ["users"]},
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "name": "users",
                    "primary_key": [],
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "type": "HttpRequester",
                            "url_base": "https://api.example.com",
                            "path": "/users",
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {"type": "DpathExtractor", "field_path": []},
                        },
                    },
                }
            ],
        }

        manifest_file = tmp_path / "manifest.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["normalize", "--manifest-path", str(manifest_file), "--quiet"]
        )

        assert result.exit_code == 0
        assert "Successfully normalized" not in result.output

    def test_normalize_manifest_file_not_found(self) -> None:
        """Test normalize command with non-existent manifest file."""
        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["normalize", "--manifest-path", "nonexistent.yaml"]
        )

        assert result.exit_code == 2
        assert "--manifest-path" in result.output
        assert "does not exist" in result.output

    def test_normalize_invalid_yaml(self, tmp_path: Path) -> None:
        """Test normalize command with invalid YAML file."""
        manifest_file = tmp_path / "invalid.yaml"
        manifest_file.write_text("invalid: yaml: content: [")

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["normalize", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 3
        assert "❌ Error: Invalid YAML" in result.output

    def test_normalize_non_dict_yaml(self, tmp_path: Path) -> None:
        """Test normalize command with YAML that's not a dictionary."""
        manifest_file = tmp_path / "list.yaml"
        manifest_file.write_text("- item1\n- item2")

        runner = CliRunner()
        result = runner.invoke(
            manifest_cli_group, ["normalize", "--manifest-path", str(manifest_file)]
        )

        assert result.exit_code == 3
        assert "does not contain a valid YAML dictionary" in result.output


class TestManifestCliHelp:
    """Test cases for CLI help text and command structure."""

    def test_manifest_group_help(self) -> None:
        """Test that the manifest command group shows help correctly."""
        runner = CliRunner()
        result = runner.invoke(manifest_cli_group, ["--help"])

        assert result.exit_code == 0
        assert "Manifest related commands" in result.output
        assert "validate" in result.output
        assert "migrate" in result.output
        assert "normalize" in result.output

    def test_validate_command_help(self) -> None:
        """Test that the validate command shows help with exit codes."""
        runner = CliRunner()
        result = runner.invoke(manifest_cli_group, ["validate", "--help"])

        assert result.exit_code == 0
        assert "codes:" in result.output
        assert "0:" in result.output and "valid and up to date" in result.output
        assert "1:" in result.output and "fixable via migration" in result.output
        assert "2:" in result.output and "validation errors" in result.output
        assert "3:" in result.output and "General errors" in result.output

    def test_migrate_command_help(self) -> None:
        """Test that the migrate command shows help correctly."""
        runner = CliRunner()
        result = runner.invoke(manifest_cli_group, ["migrate", "--help"])

        assert result.exit_code == 0
        assert "Apply migrations" in result.output
        assert "--in-place" in result.output
        assert "--exit-non-zero" in result.output
        assert "--quiet" in result.output
        assert "--manifest-path" in result.output

    def test_normalize_command_help(self) -> None:
        """Test that normalize command help text is displayed correctly."""
        runner = CliRunner()
        result = runner.invoke(manifest_cli_group, ["normalize", "--help"])

        assert result.exit_code == 0
        assert "Normalize a manifest file by removing duplicated definitions" in result.output
        assert "--in-place" in result.output
        assert "--exit-non-zero" in result.output
        assert "--quiet" in result.output
        assert "--manifest-path" in result.output
