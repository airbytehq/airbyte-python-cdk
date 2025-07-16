#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from airbyte_cdk.cli.source_declarative_manifest._run import (
    _parse_manifest_from_file,
    create_declarative_source,
    handle_command,
)
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource

REMOTE_MANIFEST_SPEC_SUBSTRING = '"required":["__injected_declarative_manifest"]'


def test_spec_does_not_raise_value_error(capsys):
    handle_command(["spec"])
    stdout = capsys.readouterr()
    assert REMOTE_MANIFEST_SPEC_SUBSTRING in stdout.out


def test_given_no_injected_declarative_manifest_then_raise_value_error(invalid_remote_config):
    with pytest.raises(ValueError):
        create_declarative_source(["check", "--config", str(invalid_remote_config)])


def test_given_injected_declarative_manifest_then_return_declarative_manifest(valid_remote_config):
    source = create_declarative_source(["check", "--config", str(valid_remote_config)])
    assert isinstance(source, ManifestDeclarativeSource)


def test_parse_manifest_from_file(valid_remote_config: Path) -> None:
    mock_manifest_content = '{"test_manifest": "fancy_declarative_components"}'
    with patch("builtins.open", mock_open(read_data=mock_manifest_content)):
        # Test with manifest path
        result = _parse_manifest_from_file("manifest.yaml")
        assert result == {"test_manifest": "fancy_declarative_components"}
