#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
from tempfile import NamedTemporaryFile
from typing import Any

from airbyte_cdk.cli.source_declarative_manifest._run import (
    create_declarative_source,
)
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource


def test_given_injected_declarative_manifest_and_py_components_then_return_declarative_manifest(
    py_components_config_dict: dict[str, Any],
):
    with NamedTemporaryFile(delete=False, suffix=".json") as temp_config_file:
        json.dump(py_components_config_dict, temp_config_file)
        temp_config_file.flush()
        source = create_declarative_source(
            ["check", "--config", temp_config_file.name],
        )
    assert isinstance(source, ManifestDeclarativeSource)
