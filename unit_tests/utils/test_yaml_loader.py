#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import io
import pkgutil
from pathlib import Path
from typing import Any

import pytest
import yaml

from airbyte_cdk.cli.source_declarative_manifest._run import _parse_manifest_from_file
from airbyte_cdk.legacy.sources.declarative.manifest_declarative_source import (
    _get_declarative_component_schema as _legacy_get_declarative_component_schema,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    _get_declarative_component_schema,
)
from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource
from airbyte_cdk.utils.yaml_loader import get_safe_loader, safe_load_yaml

requires_libyaml = pytest.mark.skipif(
    not yaml.__with_libyaml__, reason="PyYAML was built without libyaml"
)

MALFORMED_YAML = """
version: "version"
definitions:
  this is not parsable yaml: " at all
streams:
  - type: DeclarativeStream
    $parameters:
      name: "lists"
      primary_key: id
      url_base: "https://api.sendgrid.com"
check:
  type: CheckStream
  stream_names: ["lists"]
"""


@requires_libyaml
def test_get_safe_loader_uses_c_loader_when_available() -> None:
    assert get_safe_loader() is yaml.CSafeLoader


def test_falls_back_to_pure_python_loader_without_libyaml(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(yaml, "CSafeLoader", raising=False)

    assert get_safe_loader() is yaml.SafeLoader
    assert safe_load_yaml("a: 1\nb: [x, y]") == {"a": 1, "b": ["x", "y"]}
    assert YamlDeclarativeSource._parse("version: 1.0.0\nstreams: []") == {
        "version": "1.0.0",
        "streams": [],
    }


@requires_libyaml
def test_component_schema_parses_identically_with_both_loaders() -> None:
    raw = pkgutil.get_data("airbyte_cdk", "sources/declarative/declarative_component_schema.yaml")
    assert raw is not None
    pure_python_schema = yaml.load(raw, Loader=yaml.SafeLoader)
    c_loader_schema = yaml.load(raw, Loader=yaml.CSafeLoader)
    assert pure_python_schema == c_loader_schema
    assert c_loader_schema == _get_declarative_component_schema()


@pytest.mark.parametrize(
    "loader",
    [
        yaml.SafeLoader,
        pytest.param(
            yaml.CSafeLoader,
            marks=pytest.mark.skipif(
                not yaml.__with_libyaml__, reason="PyYAML was built without libyaml"
            ),
        ),
    ],
)
def test_malformed_yaml_raises_parser_error_with_both_loaders(loader: Any) -> None:
    with pytest.raises(yaml.parser.ParserError):
        yaml.load(MALFORMED_YAML, Loader=loader)


@requires_libyaml
def test_declarative_call_sites_use_c_loader(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    instantiated: list[Any] = []

    class _Spy(yaml.CSafeLoader):
        def __init__(self, stream: Any) -> None:
            instantiated.append(stream)
            super().__init__(stream)

    monkeypatch.setattr(yaml, "CSafeLoader", _Spy)

    _get_declarative_component_schema()
    assert len(instantiated) == 1

    _legacy_get_declarative_component_schema()
    assert len(instantiated) == 2

    YamlDeclarativeSource._parse("a: 1")
    assert len(instantiated) == 3

    yaml_file = tmp_path / "source.yaml"
    yaml_file.write_text("a: 1")
    YamlDeclarativeSource._read_and_parse_yaml_file(
        YamlDeclarativeSource.__new__(YamlDeclarativeSource), str(yaml_file)
    )
    assert len(instantiated) == 4

    manifest_file = tmp_path / "m.yaml"
    manifest_file.write_text("version: 1.0.0\nstreams: []")
    _parse_manifest_from_file(str(manifest_file))
    assert len(instantiated) == 5


# A list item whose nested mapping is indented one space short.
MISINDENTED_YAML = (
    'version: "1"\nstreams:\n  - type: DeclarativeStream\n    name: a\n   primary_key: id\n'
)


@requires_libyaml
def test_syntax_error_in_string_matches_pure_python_message() -> None:
    with pytest.raises(yaml.YAMLError) as expected_exc_info:
        yaml.load(MISINDENTED_YAML, Loader=yaml.SafeLoader)

    with pytest.raises(yaml.YAMLError) as actual_exc_info:
        safe_load_yaml(MISINDENTED_YAML)

    assert str(actual_exc_info.value) == str(expected_exc_info.value)
    assert type(actual_exc_info.value) is type(expected_exc_info.value)
    assert "^" in str(actual_exc_info.value)

    with pytest.raises(yaml.YAMLError) as parse_exc_info:
        YamlDeclarativeSource._parse(MISINDENTED_YAML)

    assert str(parse_exc_info.value) == str(expected_exc_info.value)
    assert type(parse_exc_info.value) is type(expected_exc_info.value)


@requires_libyaml
def test_syntax_error_in_file_matches_pure_python_message(tmp_path: Path) -> None:
    path = tmp_path / "manifest.yaml"
    path.write_text(MISINDENTED_YAML)

    with open(path) as f:
        with pytest.raises(yaml.YAMLError) as expected_exc_info:
            yaml.load(f, Loader=yaml.SafeLoader)

    with open(path) as f:
        with pytest.raises(yaml.YAMLError) as actual_exc_info:
            safe_load_yaml(f)

    assert str(actual_exc_info.value) == str(expected_exc_info.value)
    assert str(path) in str(actual_exc_info.value)


@requires_libyaml
def test_syntax_error_in_non_seekable_stream_keeps_c_loader_message() -> None:
    class _NonSeekable(io.StringIO):
        def seekable(self) -> bool:
            return False

    with pytest.raises(yaml.YAMLError) as expected_exc_info:
        yaml.load(_NonSeekable(MISINDENTED_YAML), Loader=yaml.CSafeLoader)

    with pytest.raises(yaml.YAMLError) as actual_exc_info:
        safe_load_yaml(_NonSeekable(MISINDENTED_YAML))

    assert str(actual_exc_info.value) == str(expected_exc_info.value)


@requires_libyaml
def test_successful_parse_never_uses_pure_python_loader(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    instantiated: list[Any] = []

    class _Spy(yaml.SafeLoader):
        def __init__(self, stream: Any) -> None:
            instantiated.append(stream)
            super().__init__(stream)

    monkeypatch.setattr(yaml, "SafeLoader", _Spy)

    assert safe_load_yaml("a: 1") == {"a": 1}
    assert safe_load_yaml(b"a: 1") == {"a": 1}

    yaml_file = tmp_path / "source.yaml"
    yaml_file.write_text("a: 1")
    with open(yaml_file) as f:
        assert safe_load_yaml(f) == {"a": 1}

    assert instantiated == []


@requires_libyaml
def test_pure_python_result_returned_if_only_c_loader_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingLoader(yaml.CSafeLoader):
        def get_single_data(self) -> Any:
            raise yaml.YAMLError("boom")

    monkeypatch.setattr(yaml, "CSafeLoader", _FailingLoader)

    assert safe_load_yaml("a: 1") == {"a": 1}
