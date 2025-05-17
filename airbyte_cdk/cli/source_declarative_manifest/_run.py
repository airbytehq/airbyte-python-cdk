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
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import orjson

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
        config: Mapping[str, Any] | None,
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
        config: Mapping[str, Any] | None
        catalog: ConfiguredAirbyteCatalog | None
        state: list[AirbyteStateMessage]
        config, catalog, state = _parse_inputs_into_config_catalog_state(args)
        if config is None or "__injected_declarative_manifest" not in config:
            raise ValueError(
                "Invalid config: `__injected_declarative_manifest` should be provided at the root "
                f"of the config but config only has keys: {list(config.keys() if config else [])}"
            )
        if not isinstance(config["__injected_declarative_manifest"], dict):
            raise ValueError(
                "Invalid config: `__injected_declarative_manifest` should be a dictionary, "
                f"but got type: {type(config['__injected_declarative_manifest'])}"
            )

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
    Mapping[str, Any] | None,
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


def run() -> None:
    args: list[str] = sys.argv[1:]
    handle_command(args)
