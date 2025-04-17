#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from pathlib import Path

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
