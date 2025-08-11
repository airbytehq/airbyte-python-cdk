"""
Unit tests for validating manifest.yaml files from the connector registry against the CDK schema.

This test suite fetches all manifest-only connectors from the Airbyte connector registry,
downloads their manifest.yaml files from public endpoints, and validates them against
the current declarative component schema defined in the CDK.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import patch

import pytest
import requests
import yaml

from airbyte_cdk.sources.declarative.validators.validate_adheres_to_schema import (
    ValidateAdheresToSchema,
)

logger = logging.getLogger(__name__)

EXCLUDED_CONNECTORS: List[Tuple[str, str]] = []

CONNECTOR_REGISTRY_URL = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"
MANIFEST_URL_TEMPLATE = (
    "https://connectors.airbyte.com/files/metadata/airbyte/{connector_name}/latest/manifest.yaml"
)

VALIDATION_SUCCESSES: List[Tuple[str, str]] = []
VALIDATION_FAILURES: List[Tuple[str, str, str]] = []
DOWNLOAD_FAILURES: List[Tuple[str, str]] = []


def load_declarative_component_schema() -> Dict[str, Any]:
    """Load the declarative component schema from the CDK."""
    schema_path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / "airbyte_cdk/sources/declarative/declarative_component_schema.yaml"
    )
    with open(schema_path, "r") as file:
        schema = yaml.safe_load(file)
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a dictionary")
        return schema


def get_manifest_only_connectors() -> List[Tuple[str, str]]:
    """
    Fetch manifest-only connectors from the registry.

    Returns:
        List of tuples (connector_name, cdk_version) where cdk_version will be
        determined from the manifest.yaml file itself.
    """
    try:
        response = requests.get(CONNECTOR_REGISTRY_URL, timeout=30)
        response.raise_for_status()
        registry = response.json()

        manifest_connectors: List[Tuple[str, str]] = []
        for source in registry.get("sources", []):
            if source.get("language") == "manifest-only":
                connector_name = source.get("dockerRepository", "").replace("airbyte/", "")
                if connector_name:
                    manifest_connectors.append((connector_name, "unknown"))

        return manifest_connectors
    except Exception as e:
        pytest.fail(f"Failed to fetch connector registry: {e}")


def download_manifest(connector_name: str) -> Tuple[str, str]:
    """
    Download manifest.yaml for a connector.

    Returns:
        Tuple of (manifest_content, cdk_version) where cdk_version is extracted
        from the manifest's version field.
    """
    url = MANIFEST_URL_TEMPLATE.format(connector_name=connector_name)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        manifest_content = response.text

        manifest_dict = yaml.safe_load(manifest_content)
        cdk_version = manifest_dict.get("version", "unknown")

        return manifest_content, cdk_version
    except Exception as e:
        DOWNLOAD_FAILURES.append((connector_name, str(e)))
        raise


def get_manifest_only_connector_names() -> List[str]:
    """
    Get all manifest-only connector names from the registry.

    Returns:
        List of connector names (e.g., "source-hubspot")
    """
    connectors = get_manifest_only_connectors()
    return [connector_name for connector_name, _ in connectors]


@pytest.mark.parametrize("connector_name", get_manifest_only_connector_names())
def test_manifest_validates_against_schema(connector_name: str) -> None:
    """
    Test that manifest.yaml files from the registry validate against the CDK schema.

    Args:
        connector_name: Name of the connector (e.g., "source-hubspot")
    """
    # Download manifest first to get CDK version
    try:
        manifest_content, cdk_version = download_manifest(connector_name)
    except Exception as e:
        pytest.fail(f"Failed to download manifest for {connector_name}: {e}")

    if (connector_name, cdk_version) in EXCLUDED_CONNECTORS:
        pytest.skip(
            f"Skipping {connector_name} - connector declares it is compatible with "
            f"CDK version {cdk_version} but is known to fail validation"
        )

    try:
        manifest_dict = yaml.safe_load(manifest_content)
    except yaml.YAMLError as e:
        error_msg = f"Invalid YAML in manifest for {connector_name}: {e}"
        VALIDATION_FAILURES.append((connector_name, cdk_version, error_msg))
        pytest.fail(error_msg)

    schema = load_declarative_component_schema()
    validator = ValidateAdheresToSchema(schema=schema)

    try:
        validator.validate(manifest_dict)
        VALIDATION_SUCCESSES.append((connector_name, cdk_version))
        logger.info(f"✓ {connector_name} (CDK {cdk_version}) - validation passed")
    except ValueError as e:
        error_msg = (
            f"Manifest validation failed for {connector_name} "
            f"(connector declares it is compatible with CDK version {cdk_version}): {e}"
        )
        VALIDATION_FAILURES.append((connector_name, cdk_version, str(e)))
        logger.error(f"✗ {connector_name} (CDK {cdk_version}) - validation failed: {e}")
        pytest.fail(error_msg)


def test_schema_loads_successfully() -> None:
    """Test that the declarative component schema loads without errors."""
    schema = load_declarative_component_schema()
    assert isinstance(schema, dict)
    assert "type" in schema
    assert schema["type"] == "object"


def test_connector_registry_accessible() -> None:
    """Test that the connector registry is accessible."""
    response = requests.get(CONNECTOR_REGISTRY_URL, timeout=30)
    assert response.status_code == 200
    registry = response.json()
    assert "sources" in registry
    assert isinstance(registry["sources"], list)


def test_manifest_only_connectors_found() -> None:
    """Test that we can find manifest-only connectors in the registry."""
    connectors = get_manifest_only_connectors()
    assert len(connectors) > 0, "No manifest-only connectors found in registry"

    for connector_name, _ in connectors:
        assert isinstance(connector_name, str)
        assert len(connector_name) > 0
        assert connector_name.startswith("source-") or connector_name.startswith("destination-")


def test_sample_manifest_download() -> None:
    """Test that we can download a sample manifest file."""
    connectors = get_manifest_only_connectors()
    if not connectors:
        pytest.skip("No manifest-only connectors available for testing")

    connector_name, _ = connectors[0]
    try:
        manifest_content, cdk_version = download_manifest(connector_name)
    except Exception as e:
        pytest.skip(f"Could not download sample manifest from {connector_name}: {e}")

    assert isinstance(manifest_content, str)
    assert len(manifest_content) > 0
    assert isinstance(cdk_version, str)
    assert len(cdk_version) > 0

    manifest_dict = yaml.safe_load(manifest_content)
    assert isinstance(manifest_dict, dict)
    assert "version" in manifest_dict
    assert manifest_dict["version"] == cdk_version


def log_test_results() -> None:
    """Log comprehensive test results for analysis."""
    print("\n" + "=" * 80)
    print("MANIFEST VALIDATION TEST RESULTS SUMMARY")
    print("=" * 80)

    print(f"\n✓ SUCCESSFUL VALIDATIONS ({len(VALIDATION_SUCCESSES)}):")
    for connector_name, cdk_version in VALIDATION_SUCCESSES:
        print(f"  - {connector_name} (CDK {cdk_version})")

    print(f"\n✗ VALIDATION FAILURES ({len(VALIDATION_FAILURES)}):")
    for connector_name, cdk_version, error in VALIDATION_FAILURES:
        print(f"  - {connector_name} (CDK {cdk_version}): {error}")

    print(f"\n⚠ DOWNLOAD FAILURES ({len(DOWNLOAD_FAILURES)}):")
    for connector_name, error in DOWNLOAD_FAILURES:
        print(f"  - {connector_name}: {error}")

    print("\n" + "=" * 80)
    print(
        f"TOTAL: {len(VALIDATION_SUCCESSES)} passed, {len(VALIDATION_FAILURES)} failed, {len(DOWNLOAD_FAILURES)} download errors"
    )
    print("=" * 80)


def pytest_sessionfinish(session: Any, exitstatus: Any) -> None:
    """Called after whole test run finished, right before returning the exit status to the system."""
    log_test_results()
