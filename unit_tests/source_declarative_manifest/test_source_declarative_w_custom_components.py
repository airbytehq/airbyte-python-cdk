#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import datetime
import json
import logging
import os
import types
from collections.abc import Mapping
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
from airbyte_cdk.test.utils.manifest_only_fixtures import components_module_from_string
from unit_tests.connector_builder.test_connector_builder_handler import configured_catalog
from unit_tests.source_declarative_manifest.conftest import hash_text

SAMPLE_COMPONENTS_PY_TEXT = """
def sample_function() -> str:
    return "Hello, World!"

class SimpleClass:
    def sample_method(self) -> str:
        return sample_function()
"""


def get_fixture_path(file_name) -> str:
    """
    Construct the absolute path to a fixture file relative to the current script's directory.
    
    Parameters:
        file_name (str): The name of the fixture file to locate
    
    Returns:
        str: The full absolute path to the specified fixture file
    """
    return os.path.join(os.path.dirname(__file__), file_name)


def test_components_module_from_string() -> None:
    # Call the function to get the module
    """
    Test the functionality of dynamically creating a Python module from a string containing code.
    
    This test verifies that the `components_module_from_string` function can successfully:
    - Create a module from a string of Python code
    - Define functions within the module
    - Define classes within the module
    - Allow instantiation and method calls on dynamically created classes
    
    Assertions:
        - Checks that the returned object is a module
        - Verifies the existence of a sample function
        - Confirms the sample function returns the expected string
        - Validates class definition and method invocation
    """
    components_module: types.ModuleType = components_module_from_string(SAMPLE_COMPONENTS_PY_TEXT)

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


def get_py_components_config_dict() -> dict[str, Any]:
    """
    Construct a configuration dictionary for a declarative source with custom Python components.
    
    This function loads and combines configuration data from multiple YAML files and a Python components file
    for a specific Airbyte connector. It prepares a comprehensive configuration dictionary that includes:
    - The declarative manifest
    - Custom Python components
    - Checksums for the Python components
    - Configuration and secrets from YAML files
    
    Parameters:
        None
    
    Returns:
        dict[str, Any]: A configuration dictionary containing:
            - '__injected_declarative_manifest': The loaded manifest configuration
            - '__injected_components_py': The raw Python components code
            - '__injected_components_py_checksum': MD5 and SHA256 checksums of the components
            - Additional configuration and secret key-value pairs from YAML files
    
    Raises:
        AssertionError: If the manifest file cannot be loaded or is not a mapping
    """
    connector_dir = Path(get_fixture_path("resources/source_the_guardian_api"))
    manifest_yml_path: Path = connector_dir / "manifest.yaml"
    custom_py_code_path: Path = connector_dir / "components.py"
    config_yaml_path: Path = connector_dir / "valid_config.yaml"
    secrets_yaml_path: Path = connector_dir / "secrets.yaml"

    manifest_dict = yaml.safe_load(manifest_yml_path.read_text())
    assert manifest_dict, "Failed to load the manifest file."
    assert isinstance(
        manifest_dict, Mapping
    ), f"Manifest file is type {type(manifest_dict).__name__}, not a mapping: {manifest_dict}"

    custom_py_code = custom_py_code_path.read_text()
    combined_config_dict = {
        "__injected_declarative_manifest": manifest_dict,
        "__injected_components_py": custom_py_code,
        "__injected_components_py_checksum": {
            "md5": hash_text(custom_py_code, "md5"),
            "sha256": hash_text(custom_py_code, "sha256"),
        },
    }
    combined_config_dict.update(yaml.safe_load(config_yaml_path.read_text()))
    combined_config_dict.update(yaml.safe_load(secrets_yaml_path.read_text()))
    return combined_config_dict


@pytest.mark.skipif(
    condition=not Path(get_fixture_path("resources/source_the_guardian_api/secrets.yaml")).exists(),
    reason="Skipped due to missing 'secrets.yaml'.",
)
def test_given_injected_declarative_manifest_and_py_components() -> None:
    """
    Test the integration of a declarative source with custom Python components.
    
    This test function validates the end-to-end functionality of a declarative source by:
    1. Retrieving a configuration dictionary with injected components
    2. Modifying the start date to limit test duration
    3. Creating a temporary configuration file
    4. Creating a declarative source
    5. Performing source check and discovery operations
    6. Reading messages from the source and validating them
    
    The test ensures that:
    - The configuration dictionary is correctly structured
    - A declarative source can be created from the configuration
    - The source can perform check and discover operations
    - The source can read messages without errors
    
    Args:
        None
    
    Raises:
        AssertionError: If any of the validation checks fail during the test process
    """
    py_components_config_dict = get_py_components_config_dict()
    # Truncate the start_date to speed up tests
    py_components_config_dict["start_date"] = (
        datetime.datetime.now() - datetime.timedelta(days=2)
    ).strftime("%Y-%m-%d")
    assert isinstance(py_components_config_dict, dict)
    assert "__injected_declarative_manifest" in py_components_config_dict
    assert "__injected_components_py" in py_components_config_dict

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
        for msg in msg_iterator:
            assert msg
