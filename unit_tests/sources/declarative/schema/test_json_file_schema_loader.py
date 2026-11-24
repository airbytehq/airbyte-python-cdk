#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from unittest.mock import patch

import pytest

from airbyte_cdk.sources.declarative.schema.json_file_schema_loader import (
    JsonFileSchemaLoader,
    _default_file_path,
)


@pytest.mark.parametrize(
    "test_name, input_path, expected_resource, expected_path",
    [
        (
            "path_prefixed_with_dot",
            "./source_example/schemas/lists.json",
            "source_example",
            "schemas/lists.json",
        ),
        (
            "path_prefixed_with_slash",
            "/source_example/schemas/lists.json",
            "source_example",
            "schemas/lists.json",
        ),
        (
            "path_starting_with_source",
            "source_example/schemas/lists.json",
            "source_example",
            "schemas/lists.json",
        ),
        ("path_starting_missing_source", "schemas/lists.json", "schemas", "lists.json"),
        ("path_with_file_only", "lists.json", "", "lists.json"),
        ("empty_path_does_not_crash", "", "", ""),
        ("empty_path_with_slash_does_not_crash", "/", "", ""),
    ],
)
def test_extract_resource_and_schema_path(test_name, input_path, expected_resource, expected_path):
    json_schema = JsonFileSchemaLoader({}, {}, input_path)
    actual_resource, actual_path = json_schema.extract_resource_and_schema_path(input_path)

    assert actual_resource == expected_resource
    assert actual_path == expected_path


@patch("airbyte_cdk.sources.declarative.schema.json_file_schema_loader.sys")
def test_exclude_cdk_packages(mocked_sys):
    keys = [
        "airbyte_cdk.sources.concurrent_source.concurrent_source_adapter",
        "source_gitlab.utils",
    ]
    mocked_sys.modules = {key: "" for key in keys}

    default_file_path = _default_file_path()

    assert "source_gitlab" in default_file_path


TRAVERSAL_PREFIX = "airbyte_cdk/" + "../" * 20


@pytest.fixture
def outside_json_file(tmp_path):
    secret_file = tmp_path / "secret.json"
    secret_file.write_text('{"leaked": "FILE_OUTSIDE_PACKAGE"}')
    return secret_file


def test_traversal_outside_package_is_rejected(outside_json_file):
    loader = JsonFileSchemaLoader({}, {}, TRAVERSAL_PREFIX + str(outside_json_file).lstrip("/"))

    with pytest.raises(ValueError, match="points outside the 'airbyte_cdk' package"):
        loader.get_json_schema()


def test_interpolated_traversal_from_config_is_rejected(outside_json_file):
    config = {"schema_path": TRAVERSAL_PREFIX + str(outside_json_file).lstrip("/")}
    loader = JsonFileSchemaLoader(config, {}, "{{ config['schema_path'] }}")

    with pytest.raises(ValueError, match="points outside the 'airbyte_cdk' package"):
        loader.get_json_schema()


@pytest.mark.parametrize(
    "outside_path",
    [
        pytest.param("etc/passwd", id="existing_non_json_file"),
        pytest.param("etc/does-not-exist.json", id="missing_file"),
        pytest.param("proc/self/environ", id="process_environment"),
    ],
)
def test_outside_paths_fail_before_file_access(outside_path):
    loader = JsonFileSchemaLoader({}, {}, TRAVERSAL_PREFIX + outside_path)

    with patch(
        "airbyte_cdk.sources.declarative.schema.json_file_schema_loader.pkgutil.get_data"
    ) as get_data:
        with pytest.raises(ValueError, match="points outside the 'airbyte_cdk' package"):
            loader.get_json_schema()

    get_data.assert_not_called()


def test_path_inside_package_still_loads():
    loader = JsonFileSchemaLoader({}, {}, "airbyte_cdk/cli/source_declarative_manifest/spec.json")

    assert loader.get_json_schema()


def test_parent_segments_that_stay_inside_package_still_load():
    loader = JsonFileSchemaLoader(
        {}, {}, "airbyte_cdk/sources/../cli/source_declarative_manifest/spec.json"
    )

    assert loader.get_json_schema()


@pytest.mark.parametrize(
    "file_path",
    [
        pytest.param("not_an_installed_package/schemas/stream.json", id="unknown_package"),
        pytest.param("/tmp/schemas/stream.json", id="leading_slash_then_not_a_package"),
        pytest.param("os/../../../etc/passwd.json", id="plain_module_is_not_a_package"),
        pytest.param("json.decoder/schemas/stream.json", id="submodule_is_not_a_package"),
    ],
)
def test_path_without_an_installed_package_is_rejected(file_path):
    loader = JsonFileSchemaLoader({}, {}, file_path)

    with patch(
        "airbyte_cdk.sources.declarative.schema.json_file_schema_loader.pkgutil.get_data"
    ) as get_data:
        with pytest.raises(ValueError, match="does not start with an installed package"):
            loader.get_json_schema()

    get_data.assert_not_called()


def test_symlink_inside_package_pointing_outside_is_rejected(
    tmp_path, monkeypatch, outside_json_file
):
    package_dir = tmp_path / "symlink_test_package"
    (package_dir / "schemas").mkdir(parents=True)
    (package_dir / "__init__.py").write_text("")
    (package_dir / "schemas" / "stream.json").symlink_to(outside_json_file)
    monkeypatch.syspath_prepend(str(tmp_path))
    loader = JsonFileSchemaLoader({}, {}, "symlink_test_package/schemas/stream.json")

    with pytest.raises(ValueError, match="points outside the 'symlink_test_package' package"):
        loader.get_json_schema()


def test_doubled_slash_inside_package_still_loads():
    loader = JsonFileSchemaLoader(
        {}, {}, "airbyte_cdk//sources/declarative/declarative_component_schema.yaml"
    )

    with patch(
        "airbyte_cdk.sources.declarative.schema.json_file_schema_loader.pkgutil.get_data",
        return_value=b"{}",
    ) as get_data:
        loader.get_json_schema()

    get_data.assert_called_once_with(
        "airbyte_cdk", "/sources/declarative/declarative_component_schema.yaml"
    )
