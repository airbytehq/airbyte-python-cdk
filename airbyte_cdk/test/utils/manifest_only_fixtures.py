# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Optional

import pytest

# The following fixtures are used to load a manifest-only connector's components module and manifest file.
# They can be accessed from any test file in the connector's unit_tests directory by importing them as follows:

# from airbyte_cdk.test.utils.manifest_only_fixtures import components_module, connector_dir, manifest_path

# individual components can then be referenced as: components_module.<CustomComponentClass>


@pytest.fixture(scope="session")
def connector_dir(request: pytest.FixtureRequest) -> Path:
    """Return the connector's root directory.

    This assumes tests are being run from the unit_tests directory,
    and that it is a direct child of the connector directory.
    """
    test_dir = Path(request.config.invocation_params.dir)
    return test_dir.parent


@pytest.fixture(scope="session")
def components_module(connector_dir: Path) -> Optional[ModuleType]:
    """Load and return the components module from the connector directory.

    This assumes the components module is located at <connector_dir>/components.py.
    """
    components_path = connector_dir / "components.py"
    if not components_path.exists():
        return None

    components_spec = importlib.util.spec_from_file_location("components", components_path)
    if components_spec is None:
        return None

    components_module = importlib.util.module_from_spec(components_spec)
    if components_spec.loader is None:
        return None

    components_spec.loader.exec_module(components_module)
    return components_module


@pytest.fixture(scope="session")
def manifest_path(connector_dir: Path) -> Path:
    """Return the path to the connector's manifest file."""
    return connector_dir / "manifest.yaml"
