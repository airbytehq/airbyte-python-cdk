#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import sys

from airbyte_cdk.entrypoint import AirbyteEntrypoint, launch
from airbyte_cdk.sources.declarative.yaml_declarative_source import (
    YamlDeclarativeSource,
)


def debug_manifest(source: YamlDeclarativeSource, args: list[str]) -> None:
    """
    Run the debug manifest with the given source and arguments.
    """
    launch(source, args)


def _register_components_from_file(filepath: str) -> None:
    """
    Dynamically load a Python file containing custom component definitions and register it
    under specific module names in sys.modules to ensure that these classes can be properly
    resolved during hydration of the manifest yaml file.

    This is a somewhat hacky replacement for the file structure manipulation we do when building
    connector images to ensure the custom components can be imported.
    """
    import importlib.util
    import sys
    from pathlib import Path

    components_path = Path(filepath)
    if not components_path.exists():
        raise FileNotFoundError(f"Components file not found: {components_path}")

    module_name = "components"
    sdm_module_name = "source_declarative_manifest.components"

    spec = importlib.util.spec_from_file_location(module_name, components_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {components_path}")

    # Create module and execute code
    module = importlib.util.module_from_spec(spec)

    # Register then execute the module
    # we dual-register the module to mirror what is done elsewhere in the CDK
    sys.modules[module_name] = module
    sys.modules[sdm_module_name] = module

    spec.loader.exec_module(module)


if __name__ == "__main__":
    args = sys.argv[1:]
    parsed_args = AirbyteEntrypoint.parse_args(args)

    manifest_path = getattr(parsed_args, "manifest_path", None) or "resources/manifest.yaml"
    components_path = getattr(parsed_args, "components_path", None)
    if components_path:
        _register_components_from_file(components_path)
    catalog_path = AirbyteEntrypoint.extract_catalog(args)
    config_path = AirbyteEntrypoint.extract_config(args)
    state_path = AirbyteEntrypoint.extract_state(args)

    debug_manifest(
        YamlDeclarativeSource(
            path_to_yaml=manifest_path,
            catalog=YamlDeclarativeSource.read_catalog(catalog_path) if catalog_path else None,
            config=YamlDeclarativeSource.read_config(config_path) if config_path else None,
            state=YamlDeclarativeSource.read_state(state_path) if state_path else None,
        ),
        args,
    )
