# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Utility and factory functions for testing Airbyte connectors."""

from pathlib import Path
from typing import Any, Literal

import yaml

from airbyte_cdk.test.standard_tests.connector_base import ConnectorTestSuiteBase
from airbyte_cdk.test.standard_tests.declarative_sources import (
    DeclarativeSourceTestSuite,
)
from airbyte_cdk.test.standard_tests.destination_base import DestinationTestSuiteBase
from airbyte_cdk.test.standard_tests.source_base import SourceTestSuiteBase
from airbyte_cdk.test.standard_tests.test_resources import (
    METADATA_YAML,
    find_connector_root_from_name,
)

TEST_CLASS_MAPPING: dict[
    Literal["python", "manifest-only", "declarative"], type[ConnectorTestSuiteBase]
] = {
    "python": SourceTestSuiteBase,
    "manifest-only": DeclarativeSourceTestSuite,
    # "declarative": DeclarativeSourceTestSuite,
}


def create_connector_test_suite(
    *,
    connector_name: str | None = None,
    connector_directory: Path | None = None,
) -> type[ConnectorTestSuiteBase]:
    """Get the test class for the specified connector name or path."""
    if connector_name and connector_directory:
        raise ValueError("Specify either `connector_name` or `connector_directory`, not both.")
    if not connector_name and not connector_directory:
        raise ValueError("Specify either `connector_name` or `connector_directory`.")

    if connector_name:
        connector_directory = find_connector_root_from_name(
            connector_name,
        )
    else:
        # By here, we know that connector_directory is not None
        # but connector_name is None. Set the connector_name.
        assert connector_directory is not None, "connector_directory should not be None here."
        connector_name = connector_directory.name

    metadata_yaml_path = connector_directory / METADATA_YAML
    if not metadata_yaml_path.exists():
        raise FileNotFoundError(
            f"Could not find metadata YAML file '{metadata_yaml_path}' relative to the connector directory."
        )
    metadata_dict: dict[str, Any] = yaml.safe_load(metadata_yaml_path.read_text())
    metadata_tags = metadata_dict["data"].get("tags", [])
    for language_option in TEST_CLASS_MAPPING:
        if f"language:{language_option}" in metadata_tags:
            language = language_option
            test_suite_class = TEST_CLASS_MAPPING[language]
            break
    else:
        raise ValueError(
            f"Unsupported connector type. "
            f"Supported language values are: {', '.join(TEST_CLASS_MAPPING.keys())}. "
            f"Found tags: {', '.join(metadata_tags)}"
        )

    subclass_overrides: dict[str, Any] = {
        "get_connector_root_dir": lambda: connector_directory,
    }

    TestSuiteAuto = type(
        "TestSuiteAuto",
        (test_suite_class,),
        subclass_overrides,
    )

    return TestSuiteAuto
