#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import os

import pytest

from airbyte_cdk.sources.declarative.parsers.custom_code_compiler import (
    ENV_VAR_ALLOW_CUSTOM_CODE,
)


@pytest.fixture(autouse=True)
def _allow_custom_code():
    """Enable custom-component code execution for the declarative unit tests.

    `ModelToComponentFactory.create_custom_component` requires
    `AIRBYTE_ENABLE_UNSAFE_CODE` to be set before it will resolve and instantiate a
    component's `class_name`, matching the gate already applied to injected
    `components.py` code. These tests exercise custom components extensively, so enable
    the flag by default here. Tests that verify the gate itself opt out by deleting the
    env var (e.g. through their own `monkeypatch` fixture).
    """
    previous = os.environ.get(ENV_VAR_ALLOW_CUSTOM_CODE)
    os.environ[ENV_VAR_ALLOW_CUSTOM_CODE] = "true"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(ENV_VAR_ALLOW_CUSTOM_CODE, None)
        else:
            os.environ[ENV_VAR_ALLOW_CUSTOM_CODE] = previous
