# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


import importlib.util
import types
from pathlib import Path
from types import ModuleType

import pytest

# The following fixtures are used to load a manifest-only connector's components module and manifest file.
# They can be accessed from any test file in the connector's unit_tests directory by importing them as follows:

# from airbyte_cdk.test.utils.manifest_only_fixtures import components_module, connector_dir, manifest_path

# individual components can then be referenced as: components_module.<CustomComponentClass>


@pytest.fixture(scope="session")
def connector_dir(request: pytest.FixtureRequest) -> Path:
    """Return the connector's root directory."""

    current_dir = Path(request.config.invocation_params.dir)

    # If the tests are run locally from the connector's unit_tests directory, return the parent (connector) directory
    if current_dir.name == "unit_tests":
        return current_dir.parent
    # In CI, the tests are run from the connector directory itself
    return current_dir


@pytest.fixture(scope="session")
def components_module(connector_dir: Path) -> ModuleType | None:
    """
    Load and return the components module from the connector directory.
    
    This function attempts to load the 'components.py' module from the specified connector directory. It handles various potential failure scenarios during module loading.
    
    Parameters:
        connector_dir (Path): The root directory of the connector containing the components module.
    
    Returns:
        ModuleType | None: The loaded components module if successful, or None if:
            - The components.py file does not exist
            - The module specification cannot be created
            - The module loader is unavailable
    
    Raises:
        No explicit exceptions are raised; returns None on failure.
    
    Example:
        components = components_module(Path('/path/to/connector'))
        if components:
            # Use the loaded module
            some_component = components.SomeComponent()
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


def components_module_from_string(components_py_text: str) -> ModuleType | None:
    """
    Load a Python module from a string containing module code.
    
    Parameters:
        components_py_text (str): A string containing valid Python code representing a module.
    
    Returns:
        ModuleType | None: A dynamically created module object containing the executed code, or None if execution fails.
    
    Raises:
        Exception: Potential runtime errors during code execution.
    
    Example:
        components_code = '''
        def sample_component():
            return "Hello, World!"
        '''
        module = components_module_from_string(components_code)
        result = module.sample_component()  # Returns "Hello, World!"
    """
    module_name = "components"

    # Create a new module object
    components_module = types.ModuleType(name=module_name)

    # Execute the module text in the module's namespace
    exec(components_py_text, components_module.__dict__)

    # Now you can import and use the module
    return components_module


@pytest.fixture(scope="session")
def manifest_path(connector_dir: Path) -> Path:
    """
    Return the path to the connector's manifest file.
    
    Parameters:
        connector_dir (Path): The root directory of the connector.
    
    Returns:
        Path: The absolute path to the manifest.yaml file.
    
    Raises:
        FileNotFoundError: If the manifest.yaml file does not exist in the specified connector directory.
    
    Example:
        manifest_file = manifest_path(Path('/path/to/connector'))
        # Returns Path('/path/to/connector/manifest.yaml')
    """
    path = connector_dir / "manifest.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Manifest file not found at {path}")
    return path
