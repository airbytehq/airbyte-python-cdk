# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Defines the `source-declarative-manifest` connector, which installs alongside CDK.

This file was originally imported from the dedicated connector directory, under the
`airbyte` monorepo.

Usage:

```
pipx install airbyte-cdk
source-declarative-manifest --help
source-declarative-manifest spec
...
```
"""

from __future__ import annotations

import json
import pkgutil
import sys
import traceback
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any, cast

import orjson
import yaml

from airbyte_cdk.entrypoint import AirbyteEntrypoint, launch
from airbyte_cdk.models import (
    AirbyteErrorTraceMessage,
    AirbyteMessage,
    AirbyteMessageSerializer,
    AirbyteStateMessage,
    AirbyteTraceMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecificationSerializer,
    TraceType,
    Type,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource
from airbyte_cdk.sources.source import TState
from airbyte_cdk.utils.datetime_helpers import ab_datetime_now


class SourceLocalYaml(YamlDeclarativeSource):
    """
    Declarative source defined by a yaml file in the local filesystem
    """

    def __init__(
        self,
        catalog: ConfiguredAirbyteCatalog | None,
        config: MutableMapping[str, Any] | None,
        state: TState,
        **kwargs: Any,
    ) -> None:
        """
        HACK!
            Problem: YamlDeclarativeSource relies on the calling module name/path to find the yaml file.
            Implication: If you call YamlDeclarativeSource directly it will look for the yaml file in the wrong place. (e.g. the airbyte-cdk package)
            Solution: Subclass YamlDeclarativeSource from the same location as the manifest to load.

            When can we remove this?
                When the airbyte-cdk is updated to not rely on the calling module name/path to find the yaml file.
                When all manifest connectors are updated to use the new airbyte-cdk.
                When all manifest connectors are updated to use the source-declarative-manifest as the base image.
        """
        super().__init__(
            catalog=catalog,
            config=config,
            state=state,  # type: ignore [arg-type]
            path_to_yaml="manifest.yaml",
        )


def _is_local_manifest_command(args: list[str]) -> bool:
    # Check for a local manifest.yaml file
    return Path("/airbyte/integration_code/source_declarative_manifest/manifest.yaml").exists()


def handle_command(args: list[str]) -> None:
    if _is_local_manifest_command(args):
        handle_local_manifest_command(args)
    else:
        handle_remote_manifest_command(args)


def _get_local_yaml_source(args: list[str]) -> SourceLocalYaml:
    try:
        config, catalog, state = _parse_inputs_into_config_catalog_state(args)
        return SourceLocalYaml(config=config, catalog=catalog, state=state)
    except Exception as error:
        print(
            orjson.dumps(
                AirbyteMessageSerializer.dump(
                    AirbyteMessage(
                        type=Type.TRACE,
                        trace=AirbyteTraceMessage(
                            type=TraceType.ERROR,
                            emitted_at=ab_datetime_now().to_epoch_millis(),
                            error=AirbyteErrorTraceMessage(
                                message=f"Error starting the sync. This could be due to an invalid configuration or catalog. Please contact Support for assistance. Error: {error}",
                                stack_trace=traceback.format_exc(),
                            ),
                        ),
                    )
                )
            ).decode()
        )
        raise error


def handle_local_manifest_command(args: list[str]) -> None:
    source = _get_local_yaml_source(args)
    launch(
        source=source,
        args=args,
    )


def handle_remote_manifest_command(args: list[str]) -> None:
    """Overrides the spec command to return the generalized spec for the declarative manifest source.

    This is different from a typical low-code, but built and published separately source built as a ManifestDeclarativeSource,
    because that will have a spec method that returns the spec for that specific source. Other than spec,
    the generalized connector behaves the same as any other, since the manifest is provided in the config.
    """
    if args[0] == "spec":
        json_spec = pkgutil.get_data(
            "airbyte_cdk.cli.source_declarative_manifest",
            "spec.json",
        )
        if json_spec is None:
            raise FileNotFoundError(
                "Could not find `spec.json` file for source-declarative-manifest"
            )

        spec_obj = json.loads(json_spec)
        spec = ConnectorSpecificationSerializer.load(spec_obj)

        message = AirbyteMessage(type=Type.SPEC, spec=spec)
        print(AirbyteEntrypoint.airbyte_message_to_string(message))
    else:
        source = create_declarative_source(args)
        launch(
            source=source,
            args=args,
        )


def create_declarative_source(
    args: list[str],
) -> ConcurrentDeclarativeSource:  # type: ignore [type-arg]
    """Creates the source with the injected config.

    This essentially does what other low-code sources do at build time, but at runtime,
    with a user-provided manifest in the config. This better reflects what happens in the
    connector builder.
    """
    try:
        config: MutableMapping[str, Any] | None
        catalog: ConfiguredAirbyteCatalog | None
        state: list[AirbyteStateMessage]
        config, catalog, state = _parse_inputs_into_config_catalog_state(args)

        if config is None:
            raise ValueError(
                "Invalid config: `__injected_declarative_manifest` should be provided at the root "
                "of the config or using the --manifest-path argument."
            )

        # If a manifest_path is provided in the args, inject it into the config
        injected_manifest = _parse_manifest_from_args(args)
        if injected_manifest:
            config["__injected_declarative_manifest"] = injected_manifest

        if "__injected_declarative_manifest" not in config:
            raise ValueError(
                "Invalid config: `__injected_declarative_manifest` should be provided at the root "
                "of the config or using the --manifest-path argument. "
                f"Config only has keys: {list(config.keys() if config else [])}"
            )
        if not isinstance(config["__injected_declarative_manifest"], dict):
            raise ValueError(
                "Invalid config: `__injected_declarative_manifest` should be a dictionary, "
                f"but got type: {type(config['__injected_declarative_manifest'])}"
            )

        # Load custom components if provided - this will register them in sys.modules
        _parse_components_from_args(args)

        return ConcurrentDeclarativeSource(
            config=config,
            catalog=catalog,
            state=state,
            source_config=cast(dict[str, Any], config["__injected_declarative_manifest"]),
        )
    except Exception as error:
        print(
            orjson.dumps(
                AirbyteMessageSerializer.dump(
                    AirbyteMessage(
                        type=Type.TRACE,
                        trace=AirbyteTraceMessage(
                            type=TraceType.ERROR,
                            emitted_at=ab_datetime_now().to_epoch_millis(),
                            error=AirbyteErrorTraceMessage(
                                message=f"Error starting the sync. This could be due to an invalid configuration or catalog. Please contact Support for assistance. Error: {error}",
                                stack_trace=traceback.format_exc(),
                            ),
                        ),
                    )
                )
            ).decode()
        )
        raise error


def _parse_inputs_into_config_catalog_state(
    args: list[str],
) -> tuple[
    MutableMapping[str, Any] | None,
    ConfiguredAirbyteCatalog | None,
    list[AirbyteStateMessage],
]:
    parsed_args = AirbyteEntrypoint.parse_args(args)
    config = (
        ConcurrentDeclarativeSource.read_config(parsed_args.config)
        if hasattr(parsed_args, "config")
        else None
    )
    catalog = (
        ConcurrentDeclarativeSource.read_catalog(parsed_args.catalog)
        if hasattr(parsed_args, "catalog")
        else None
    )
    state = (
        ConcurrentDeclarativeSource.read_state(parsed_args.state)
        if hasattr(parsed_args, "state")
        else []
    )

    return config, catalog, state


def _parse_manifest_from_args(args: list[str]) -> dict[str, Any] | None:
    """Extracts and parse the manifest file if specified in the args."""
    parsed_args = AirbyteEntrypoint.parse_args(args)

    # Safely check if manifest_path is provided in the args
    if hasattr(parsed_args, "manifest_path") and parsed_args.manifest_path:
        try:
            # Read the manifest file
            with open(parsed_args.manifest_path, "r") as manifest_file:
                manifest_content = yaml.safe_load(manifest_file)
                if not isinstance(manifest_content, dict):
                    raise ValueError(f"Manifest must be a dictionary, got {type(manifest_content)}")
                return manifest_content
        except Exception as error:
            raise ValueError(
                f"Failed to load manifest file from {parsed_args.manifest_path}: {error}"
            )

    return None


def _register_components_from_file(filepath: str) -> None:
    """Load and register components from a Python file for CLI usage.

    This is a special case for CLI usage that bypasses the checksum validation
    since the user is explicitly providing the file to execute.
    """
    import importlib.util
    import sys

    # Use Python's import mechanism to properly load the module
    components_path = Path(filepath)

    # Standard module names that the rest of the system expects
    module_name = "components"
    sdm_module_name = "source_declarative_manifest.components"

    # Create module spec
    spec = importlib.util.spec_from_file_location(module_name, components_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {components_path}")

    # Create module and execute code
    module = importlib.util.module_from_spec(spec)

    # Register the module BEFORE executing its code
    # This is critical for features like dataclasses that look up the module
    sys.modules[module_name] = module
    sys.modules[sdm_module_name] = module

    # Now execute the module code
    spec.loader.exec_module(module)


def _parse_components_from_args(args: list[str]) -> bool:
    """Loads and registers the custom components.py module if it exists.

    This function imports the components module from a provided path
    and registers it in sys.modules so it can be found by the source.

    Returns True if components were registered, False otherwise.
    """
    parsed_args = AirbyteEntrypoint.parse_args(args)

    # Safely check if components_path is provided in the args
    if hasattr(parsed_args, "components_path") and parsed_args.components_path:
        try:
            # Use our CLI-specific function that bypasses checksum validation
            _register_components_from_file(parsed_args.components_path)
            return True
        except Exception as error:
            raise ValueError(
                f"Failed to load components from {parsed_args.components_path}: {error}"
            )

    return False


def run() -> None:
    args: list[str] = sys.argv[1:]
    handle_command(args)
