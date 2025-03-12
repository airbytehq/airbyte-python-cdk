#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import sys
from typing import Any, Mapping

from airbyte_cdk.entrypoint import launch
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
    debug_manifest(
        YamlDeclarativeSource(**configuration),
        sys.argv[1:],
    )
