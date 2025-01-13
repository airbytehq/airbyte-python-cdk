#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import hashlib
import os
from typing import Literal

import pytest
import yaml


def hash_text(input_text: str, hash_type: Literal["md5", "sha256"] = "md5") -> str:
    """
    Compute the hash of the input text using the specified hashing algorithm.
    
    Parameters:
        input_text (str): The text to be hashed.
        hash_type (Literal["md5", "sha256"], optional): The hashing algorithm to use. 
            Defaults to "md5". Supports "md5" and "sha256" algorithms.
    
    Returns:
        str: The hexadecimal digest of the hashed input text.
    
    Examples:
        >>> hash_text("hello world")
        '5eb63bbbe01eeed093cb22bb8f5acdc3'
        >>> hash_text("hello world", hash_type="sha256")
        'b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9'
    """
    hashers = {
        "md5": hashlib.md5,
        "sha256": hashlib.sha256,
    }
    hash_object = hashers[hash_type]()
    hash_object.update(input_text.encode())
    return hash_object.hexdigest()


def get_fixture_path(file_name) -> str:
    """
    Construct the full path to a fixture file relative to the current script's directory.
    
    Parameters:
        file_name (str): The name of the fixture file to locate.
    
    Returns:
        str: The absolute path to the specified fixture file.
    
    Example:
        >>> get_fixture_path('config.json')
        '/path/to/current/directory/config.json'
    """
    return os.path.join(os.path.dirname(__file__), file_name)


@pytest.fixture
def valid_remote_config():
    return get_fixture_path("resources/valid_remote_config.json")


@pytest.fixture
def invalid_remote_config():
    return get_fixture_path("resources/invalid_remote_config.json")


@pytest.fixture
def valid_local_manifest():
    return get_fixture_path("resources/valid_local_manifest.yaml")


@pytest.fixture
def invalid_local_manifest():
    return get_fixture_path("resources/invalid_local_manifest.yaml")


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
    return get_fixture_path("resources/valid_local_pokeapi_config.json")


@pytest.fixture
def invalid_local_config_file():
    return get_fixture_path("resources/invalid_local_pokeapi_config.json")
