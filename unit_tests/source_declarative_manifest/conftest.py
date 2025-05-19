#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import sys
import tempfile
from pathlib import Path
from typing import Generator

import pytest
import yaml


def get_resource_path(file_name) -> str:
    return Path(__file__).parent.parent / "resources" / file_name


@pytest.fixture
def valid_remote_config():
    return get_resource_path("valid_remote_config.json")


@pytest.fixture
def invalid_remote_config():
    return get_resource_path("invalid_remote_config.json")


@pytest.fixture
def valid_local_manifest():
    return get_resource_path("valid_local_manifest.yaml")


@pytest.fixture
def invalid_local_manifest():
    return get_resource_path("invalid_local_manifest.yaml")


@pytest.fixture
def valid_local_manifest_yaml(valid_local_manifest):
    with open(valid_local_manifest, "r") as file:
        return yaml.safe_load(file)


@pytest.fixture
def invalid_local_manifest_yaml(invalid_local_manifest):
    with open(invalid_local_manifest, "r") as file:
        return yaml.safe_load(file)


@pytest.fixture
def valid_local_config_file():
    return get_resource_path("valid_local_pokeapi_config.json")


@pytest.fixture
def invalid_local_config_file():
    return get_resource_path("invalid_local_pokeapi_config.json")


# Sample component code for testing
SAMPLE_COMPONENTS_PY_TEXT = """
def sample_function() -> str:
    return "Hello, World!"

class SimpleClass:
    def sample_method(self) -> str:
        return sample_function()
"""


def verify_components_loaded() -> None:
    """Verify that components were properly loaded."""
    import components

    assert hasattr(components, "sample_function")
    assert components.sample_function() == "Hello, World!"

    # Verify the components module is registered in sys.modules
    assert "components" in sys.modules
    assert "source_declarative_manifest.components" in sys.modules

    # Verify they are the same module
    assert sys.modules["components"] is sys.modules["source_declarative_manifest.components"]


@pytest.fixture
def components_file() -> Generator[str, None, None]:
    """Create a temporary file with sample components code and clean up modules afterwards."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as temp_file:
        temp_file.write(SAMPLE_COMPONENTS_PY_TEXT)
        temp_file.flush()
        file_path = temp_file.name

    try:
        yield file_path
    finally:
        # Clean up the modules
        if "components" in sys.modules:
            del sys.modules["components"]
        if "source_declarative_manifest.components" in sys.modules:
            del sys.modules["source_declarative_manifest.components"]
        # Clean up the temporary file
        Path(file_path).unlink(missing_ok=True)
