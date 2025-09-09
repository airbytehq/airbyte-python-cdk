#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import sys
from typing import Any, Mapping

from airbyte_cdk.entrypoint import AirbyteEntrypoint, launch
from airbyte_cdk.sources.declarative.yaml_declarative_source import (
    YamlDeclarativeSource,
)

configuration: Mapping[str, Any] = {
    "path_to_yaml": "resources/manifest.yaml",
}


def debug_manifest(source: YamlDeclarativeSource, args: list[str]) -> None:
    """
    Run the debug manifest with the given source and arguments.
    """
    launch(source, args)


if __name__ == "__main__":
    args = sys.argv[1:]
    parsed_args = AirbyteEntrypoint.parse_args(args)
    manifest_path = parsed_args.manifest_path or "resources/manifest.yaml"
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
