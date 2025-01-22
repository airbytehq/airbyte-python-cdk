#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import datetime
import json
import logging
import os
import sys
import types
from collections.abc import Callable, Mapping
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pytest
import yaml
from airbyte_protocol_dataclasses.models.airbyte_protocol import AirbyteCatalog

from airbyte_cdk.cli.source_declarative_manifest._run import (
    create_declarative_source,
)
from airbyte_cdk.models import ConfiguredAirbyteCatalog, ConfiguredAirbyteStream
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
from airbyte_cdk.sources.declarative.parsers.custom_code_compiler import (
    ENV_VAR_ALLOW_CUSTOM_CODE,
    INJECTED_COMPONENTS_PY,
    INJECTED_COMPONENTS_PY_CHECKSUMS,
    INJECTED_MANIFEST,
    AirbyteCodeTamperedError,
    AirbyteCustomCodeNotPermittedError,
    _hash_text,
    custom_code_execution_permitted,
    register_components_module_from_string,
)

SAMPLE_COMPONENTS_PY_TEXT = """
def sample_function() -> str:
    return "Hello, World!"

class SimpleClass:
    def sample_method(self) -> str:
        return sample_function()
"""


def get_fixture_path(file_name) -> str:
    return os.path.join(os.path.dirname(__file__), file_name)


def test_components_module_from_string() -> None:
    # Call the function to get the module
    components_module: types.ModuleType = register_components_module_from_string(
        components_py_text=SAMPLE_COMPONENTS_PY_TEXT,
        checksums={
            "md5": _hash_text(SAMPLE_COMPONENTS_PY_TEXT, "md5"),
        },
    )

    # Check that the module is created and is of the correct type
    assert isinstance(components_module, types.ModuleType)

    # Check that the function is correctly defined in the module
    assert hasattr(components_module, "sample_function")

    # Check that simple functions are callable
    assert components_module.sample_function() == "Hello, World!"

    # Check class definitions work as expected
    assert isinstance(components_module.SimpleClass, type)
    obj = components_module.SimpleClass()
    assert isinstance(obj, components_module.SimpleClass)
    assert obj.sample_method() == "Hello, World!"

    # Check we can get the class definition from sys.modules
    module_lookup = sys.modules[components_module.__name__]
    class_lookup = getattr(sys.modules[components_module.__name__], "SimpleClass")

    assert module_lookup == components_module
    assert class_lookup == components_module.SimpleClass
    assert class_lookup().sample_method() == "Hello, World!"

    # Check we can import the module by name
    from source_declarative_manifest.components import sample_function as imported_sample_function  # type: ignore [import]  # noqa: I001

    assert imported_sample_function() == "Hello, World!"


def get_py_components_config_dict(
    *,
    failing_components: bool = False,
    needs_secrets: bool = True,
) -> dict[str, Any]:
    connector_dir = Path(get_fixture_path("resources/source_the_guardian_api"))
    manifest_yml_path: Path = connector_dir / "manifest.yaml"
    custom_py_code_path: Path = connector_dir / (
        "components.py" if not failing_components else "components_failing.py"
    )
    config_yaml_path: Path = connector_dir / "valid_config.yaml"
    secrets_yaml_path: Path = connector_dir / "secrets.yaml"

    manifest_dict = yaml.safe_load(manifest_yml_path.read_text())
    assert manifest_dict, "Failed to load the manifest file."
    assert isinstance(
        manifest_dict, Mapping
    ), f"Manifest file is type {type(manifest_dict).__name__}, not a mapping: {manifest_dict}"

    custom_py_code = custom_py_code_path.read_text()
    combined_config_dict = {
        INJECTED_MANIFEST: manifest_dict,
        INJECTED_COMPONENTS_PY: custom_py_code,
        INJECTED_COMPONENTS_PY_CHECKSUMS: {
            "md5": _hash_text(custom_py_code, "md5"),
            "sha256": _hash_text(custom_py_code, "sha256"),
        },
    }
    combined_config_dict.update(yaml.safe_load(config_yaml_path.read_text()))
    if needs_secrets:
        combined_config_dict.update(yaml.safe_load(secrets_yaml_path.read_text()))

    return combined_config_dict


def test_missing_checksum_fails_to_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assert that missing checksum in the config will raise an error."""
    monkeypatch.setenv(ENV_VAR_ALLOW_CUSTOM_CODE, "true")

    py_components_config_dict = get_py_components_config_dict(
        needs_secrets=False,
    )
    # Truncate the start_date to speed up tests
    py_components_config_dict["start_date"] = (
        datetime.datetime.now() - datetime.timedelta(days=2)
    ).strftime("%Y-%m-%d")

    py_components_config_dict.pop("__injected_components_py_checksums")

    with NamedTemporaryFile(delete=False, suffix=".json") as temp_config_file:
        json_str = json.dumps(py_components_config_dict)
        Path(temp_config_file.name).write_text(json_str)
        temp_config_file.flush()
        with pytest.raises(ValueError):
            source = create_declarative_source(
                ["check", "--config", temp_config_file.name],
            )


@pytest.mark.parametrize(
    "hash_type",
    [
        "md5",
        "sha256",
    ],
)
def test_invalid_checksum_fails_to_run(
    hash_type: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assert that an invalid checksum in the config will raise an error."""
    monkeypatch.setenv(ENV_VAR_ALLOW_CUSTOM_CODE, "true")

    py_components_config_dict = get_py_components_config_dict(
        needs_secrets=False,
    )
    # Truncate the start_date to speed up tests
    py_components_config_dict["start_date"] = (
        datetime.datetime.now() - datetime.timedelta(days=2)
    ).strftime("%Y-%m-%d")

    py_components_config_dict["__injected_components_py_checksums"][hash_type] = "invalid_checksum"

    with NamedTemporaryFile(delete=False, suffix=".json") as temp_config_file:
        json_str = json.dumps(py_components_config_dict)
        Path(temp_config_file.name).write_text(json_str)
        temp_config_file.flush()
        with pytest.raises(AirbyteCodeTamperedError):
            source = create_declarative_source(
                ["check", "--config", temp_config_file.name],
            )


@pytest.mark.parametrize(
    "env_value, should_raise",
    [
        ("true", False),
        ("True", False),
        ("TRUE", False),
        ("1", True),  # Not accepted as truthy as of now
        ("false", True),
        ("False", True),
        ("", True),
        ("0", True),
    ],
)
def test_fail_unless_custom_code_enabled_explicitly(
    env_value: str | None,
    should_raise: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assert that we properly fail if the environment variable to allow custom code is not set.

    A missing value should fail.
    Any value other than "true" (case insensitive) should fail.
    """
    monkeypatch.delenv(ENV_VAR_ALLOW_CUSTOM_CODE, raising=False)
    if env_value is not None:
        monkeypatch.setenv(ENV_VAR_ALLOW_CUSTOM_CODE, env_value)

    assert custom_code_execution_permitted() == (not should_raise)

    py_components_config_dict = get_py_components_config_dict(
        needs_secrets=False,
    )
    # Truncate the start_date to speed up tests
    py_components_config_dict["start_date"] = (
        datetime.datetime.now() - datetime.timedelta(days=2)
    ).strftime("%Y-%m-%d")

    with NamedTemporaryFile(delete=False, suffix=".json") as temp_config_file:
        json_str = json.dumps(py_components_config_dict)
        Path(temp_config_file.name).write_text(json_str)
        temp_config_file.flush()
        fn: Callable = lambda: create_declarative_source(
            ["check", "--config", temp_config_file.name],
        )
        if should_raise:
            with pytest.raises(AirbyteCustomCodeNotPermittedError):
                fn()

            return  # Success

        fn()


# TODO: Create a new test source that doesn't require credentials to run.
@pytest.mark.skipif(
    condition=not Path(get_fixture_path("resources/source_the_guardian_api/secrets.yaml")).exists(),
    reason="Skipped due to missing 'secrets.yaml'.",
)
@pytest.mark.parametrize(
    "failing_components",
    [
        pytest.param(False, marks=pytest.mark.slow),  # Slow because we run a full sync
        True,
    ],
)
def test_sync_with_injected_py_components(
    failing_components: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENV_VAR_ALLOW_CUSTOM_CODE, "true")

    py_components_config_dict = get_py_components_config_dict(
        failing_components=failing_components,
    )
    # Truncate the start_date to speed up tests
    py_components_config_dict["start_date"] = (
        datetime.datetime.now() - datetime.timedelta(days=2)
    ).strftime("%Y-%m-%d")
    assert isinstance(py_components_config_dict, dict)
    assert "__injected_declarative_manifest" in py_components_config_dict
    assert "__injected_components_py" in py_components_config_dict
    assert "__injected_components_py_checksums" in py_components_config_dict

    with NamedTemporaryFile(delete=False, suffix=".json") as temp_config_file:
        json_str = json.dumps(py_components_config_dict)
        Path(temp_config_file.name).write_text(json_str)
        temp_config_file.flush()
        source = create_declarative_source(
            ["check", "--config", temp_config_file.name],
        )
        assert isinstance(source, ManifestDeclarativeSource)
        source.check(logger=logging.getLogger(), config=py_components_config_dict)
        catalog: AirbyteCatalog = source.discover(
            logger=logging.getLogger(), config=py_components_config_dict
        )
        assert isinstance(catalog, AirbyteCatalog)
        configured_catalog = ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=stream,
                    sync_mode="full_refresh",
                    destination_sync_mode="overwrite",
                )
                for stream in catalog.streams
            ]
        )

        msg_iterator = source.read(
            logger=logging.getLogger(),
            config=py_components_config_dict,
            catalog=configured_catalog,
            state=None,
        )
        if failing_components:
            with pytest.raises(Exception):
                for msg in msg_iterator:
                    assert msg
            return

        for msg in msg_iterator:
            assert msg
