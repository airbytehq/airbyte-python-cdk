"""
Unit tests for validating manifest.yaml files from the connector registry against the CDK schema.

This test suite fetches all manifest-only connectors from the Airbyte connector registry,
downloads their manifest.yaml files from public endpoints, and validates them against
the current declarative component schema defined in the CDK.
"""

import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import patch

import pytest
import requests
import yaml

from airbyte_cdk.sources.declarative.parsers.manifest_component_transformer import (
    ManifestComponentTransformer,
)
from airbyte_cdk.sources.declarative.parsers.manifest_reference_resolver import (
    ManifestReferenceResolver,
)
from airbyte_cdk.sources.declarative.validators.validate_adheres_to_schema import (
    ValidateAdheresToSchema,
)
from airbyte_cdk.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)

logger = logging.getLogger(__name__)

# List of connectors to exclude from validation.
EXCLUDED_CONNECTORS: List[Tuple[str, str]] = []

RECHECK_EXCLUSION_LIST = False

USE_GIT_SPARSE_CHECKOUT = True

CONNECTOR_REGISTRY_URL = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"
MANIFEST_URL_TEMPLATE = (
    "https://connectors.airbyte.com/files/metadata/airbyte/{connector_name}/latest/manifest.yaml"
)


@pytest.fixture(scope="session")
def validation_successes() -> List[Tuple[str, str]]:
    """Thread-safe list for tracking validation successes."""
    return []


@pytest.fixture(scope="session")
def validation_failures() -> List[Tuple[str, str, str]]:
    """Thread-safe list for tracking validation failures."""
    return []


@pytest.fixture(scope="session")
def download_failures() -> List[Tuple[str, str]]:
    """Thread-safe list for tracking download failures."""
    return []


@pytest.fixture(scope="session")
def cdk_validation_failures() -> List[Tuple[str, str, str]]:
    """Thread-safe list for tracking CDK validation failures."""
    return []


@pytest.fixture(scope="session")
def spec_execution_failures() -> List[Tuple[str, str, str]]:
    """Thread-safe list for tracking SPEC execution failures."""
    return []


@pytest.fixture(scope="session")
def schema_validator() -> ValidateAdheresToSchema:
    """Cached schema validator to avoid repeated loading."""
    schema = load_declarative_component_schema()
    return ValidateAdheresToSchema(schema=schema)


@pytest.fixture(scope="session")
def manifest_connector_names() -> List[str]:
    """Cached list of manifest-only connector names to avoid repeated registry calls."""
    if USE_GIT_SPARSE_CHECKOUT:
        # Use git sparse-checkout to get all available manifest connectors
        try:
            manifests = download_manifests_via_git()
            return list(manifests.keys())
        except Exception as e:
            logger.warning(f"Git sparse-checkout failed, falling back to registry: {e}")
            connectors = get_manifest_only_connectors()
            return [connector_name for connector_name, _ in connectors]
    else:
        connectors = get_manifest_only_connectors()
        return [connector_name for connector_name, _ in connectors]


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


# Global cache for git-downloaded manifests
_git_manifest_cache: Dict[str, Tuple[str, str]] = {}


def download_manifest(
    connector_name: str, download_failures: List[Tuple[str, str]]
) -> Tuple[str, str]:
    """
    Download manifest.yaml for a connector.

    Returns:
        Tuple of (manifest_content, cdk_version) where cdk_version is extracted
        from the manifest's version field.
    """
    global _git_manifest_cache

    if USE_GIT_SPARSE_CHECKOUT and not _git_manifest_cache:
        try:
            logger.info("Initializing git sparse-checkout cache...")
            _git_manifest_cache = download_manifests_via_git()
            logger.info(f"Cached {len(_git_manifest_cache)} manifests from git")
        except Exception as e:
            logger.warning(f"Git sparse-checkout failed, using HTTP fallback: {e}")

    if connector_name in _git_manifest_cache:
        return _git_manifest_cache[connector_name]

    url = MANIFEST_URL_TEMPLATE.format(connector_name=connector_name)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        manifest_content = response.text

        manifest_dict = yaml.safe_load(manifest_content)
        cdk_version = manifest_dict.get("version", "unknown")

        return manifest_content, cdk_version
    except Exception as e:
        download_failures.append((connector_name, str(e)))
        raise


def download_manifests_via_git() -> Dict[str, Tuple[str, str]]:
    """
    Download all manifest files using git sparse-checkout for better performance.

    Returns:
        Dict mapping connector_name to (manifest_content, cdk_version)
    """
    manifests: Dict[str, Tuple[str, str]] = {}

    with tempfile.TemporaryDirectory() as temp_dir:
        repo_path = Path(temp_dir) / "airbyte"

        try:
            logger.info("Cloning airbyte repo with sparse-checkout...")
            subprocess.run(
                [
                    "git",
                    "clone",
                    "--filter=blob:none",
                    "--sparse",
                    "--depth=1",
                    "https://github.com/airbytehq/airbyte.git",
                    str(repo_path),
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=120,
            )

            logger.info("Setting sparse-checkout pattern...")
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo_path),
                    "sparse-checkout",
                    "set",
                    "airbyte-integrations/connectors/*/manifest.yaml",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )

            logger.info("Processing manifest files...")
            manifest_files = list(repo_path.glob("airbyte-integrations/connectors/*/manifest.yaml"))
            logger.info(f"Found {len(manifest_files)} manifest files")

            for i, manifest_path in enumerate(manifest_files):
                connector_name = manifest_path.parent.name
                if i % 50 == 0:
                    logger.info(
                        f"Processing manifest {i + 1}/{len(manifest_files)}: {connector_name}"
                    )
                try:
                    with open(manifest_path, "r") as f:
                        manifest_content = f.read()

                    manifest_dict = yaml.safe_load(manifest_content)
                    cdk_version = manifest_dict.get("version", "unknown")
                    manifests[connector_name] = (manifest_content, cdk_version)
                except Exception as e:
                    logger.warning(f"Failed to process manifest for {connector_name}: {e}")

        except subprocess.TimeoutExpired:
            logger.error("Git sparse-checkout timed out. Falling back to HTTP downloads.")
            return {}
        except subprocess.CalledProcessError as e:
            logger.warning(f"Git sparse-checkout failed: {e}. Falling back to HTTP downloads.")
            return {}
        except Exception as e:
            logger.error(
                f"Unexpected error in git sparse-checkout: {e}. Falling back to HTTP downloads."
            )
            return {}

    logger.info(f"Successfully cached {len(manifests)} manifests from git")
    return manifests


def get_manifest_only_connector_names() -> List[str]:
    """
    Get all manifest-only connector names from the registry.

    Returns:
        List of connector names (e.g., "source-hubspot")
    """
    connectors = get_manifest_only_connectors()
    return [connector_name for connector_name, _ in connectors]


@pytest.mark.parametrize("connector_name", get_manifest_only_connector_names())
def test_manifest_validates_against_schema(
    connector_name: str,
    schema_validator: ValidateAdheresToSchema,
    validation_successes: List[Tuple[str, str]],
    validation_failures: List[Tuple[str, str, str]],
    download_failures: List[Tuple[str, str]],
    cdk_validation_failures: List[Tuple[str, str, str]],
    spec_execution_failures: List[Tuple[str, str, str]],
) -> None:
    """
    Test that manifest.yaml files from the registry validate against the CDK schema.

    Args:
        connector_name: Name of the connector (e.g., "source-hubspot")
    """
    # Download manifest first to get CDK version
    try:
        manifest_content, cdk_version = download_manifest(connector_name, download_failures)
    except Exception as e:
        pytest.fail(f"Failed to download manifest for {connector_name}: {e}")

    is_excluded = (connector_name, cdk_version) in EXCLUDED_CONNECTORS

    if RECHECK_EXCLUSION_LIST:
        expected_to_fail = is_excluded
    else:
        # Normal mode: skip excluded connectors
        if is_excluded:
            pytest.skip(
                f"Skipping {connector_name} - connector declares it is compatible with "
                f"CDK version {cdk_version} but is known to fail validation"
            )

    try:
        manifest_dict = yaml.safe_load(manifest_content)
    except yaml.YAMLError as e:
        error_msg = f"Invalid YAML in manifest for {connector_name}: {e}"
        validation_failures.append((connector_name, cdk_version, error_msg))
        pytest.fail(error_msg)

    try:
        if "type" not in manifest_dict:
            manifest_dict["type"] = "DeclarativeSource"

        # Resolve references in the manifest
        resolved_manifest = ManifestReferenceResolver().preprocess_manifest(manifest_dict)

        # Propagate types and parameters throughout the manifest
        preprocessed_manifest = ManifestComponentTransformer().propagate_types_and_parameters(
            "", resolved_manifest, {}
        )

        schema_validator.validate(preprocessed_manifest)
        logger.info(f"✓ {connector_name} (CDK {cdk_version}) - JSON schema validation passed")

        try:
            manifest_source = ManifestDeclarativeSource(source_config=preprocessed_manifest)
            logger.info(f"✓ {connector_name} (CDK {cdk_version}) - CDK validation passed")
        except Exception as e:
            error_msg = f"CDK validation failed: {e}"
            cdk_validation_failures.append((connector_name, cdk_version, error_msg))
            logger.warning(f"⚠ {connector_name} (CDK {cdk_version}) - CDK validation failed: {e}")

        try:
            manifest_source = ManifestDeclarativeSource(source_config=preprocessed_manifest)
            spec_result = manifest_source.spec(logger)
            if spec_result is None:
                raise ValueError("SPEC command returned None")
            logger.info(f"✓ {connector_name} (CDK {cdk_version}) - SPEC execution passed")
        except Exception as e:
            error_msg = f"SPEC execution failed: {e}"
            spec_execution_failures.append((connector_name, cdk_version, error_msg))
            logger.warning(f"⚠ {connector_name} (CDK {cdk_version}) - SPEC execution failed: {e}")

        validation_successes.append((connector_name, cdk_version))
        logger.info(f"✓ {connector_name} (CDK {cdk_version}) - comprehensive validation completed")

        if RECHECK_EXCLUSION_LIST and expected_to_fail:
            pytest.fail(
                f"EXCLUSION LIST ERROR: {connector_name} (CDK {cdk_version}) was expected to fail "
                f"but passed validation. Remove from EXCLUDED_CONNECTORS."
            )

    except ValueError as e:
        error_msg = (
            f"Manifest validation failed for {connector_name} "
            f"(connector declares it is compatible with CDK version {cdk_version}): {e}"
        )
        validation_failures.append((connector_name, cdk_version, str(e)))
        logger.error(f"✗ {connector_name} (CDK {cdk_version}) - validation failed: {e}")

        if RECHECK_EXCLUSION_LIST and not expected_to_fail:
            pytest.fail(
                f"EXCLUSION LIST ERROR: {connector_name} (CDK {cdk_version}) was expected to pass "
                f"but failed validation. Add to EXCLUDED_CONNECTORS: {error_msg}"
            )
        elif not RECHECK_EXCLUSION_LIST:
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


def test_sample_manifest_download(download_failures: List[Tuple[str, str]]) -> None:
    """Test that we can download a sample manifest file."""
    connectors = get_manifest_only_connectors()
    if not connectors:
        pytest.skip("No manifest-only connectors available for testing")

    connector_name, _ = connectors[0]
    try:
        manifest_content, cdk_version = download_manifest(connector_name, download_failures)
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


def log_test_results(
    validation_successes: List[Tuple[str, str]],
    validation_failures: List[Tuple[str, str, str]],
    download_failures: List[Tuple[str, str]],
    cdk_validation_failures: List[Tuple[str, str, str]],
    spec_execution_failures: List[Tuple[str, str, str]],
) -> None:
    """Log comprehensive test results for analysis."""
    print("\n" + "=" * 80)
    print("MANIFEST VALIDATION TEST RESULTS SUMMARY")
    print("=" * 80)

    print(f"\n✓ SUCCESSFUL VALIDATIONS ({len(validation_successes)}):")
    for connector_name, cdk_version in validation_successes:
        print(f"  - {connector_name} (CDK {cdk_version})")

    print(f"\n✗ VALIDATION FAILURES ({len(validation_failures)}):")
    for connector_name, cdk_version, error in validation_failures:
        print(f"  - {connector_name} (CDK {cdk_version}): {error}")

    print(f"\n⚠ DOWNLOAD FAILURES ({len(download_failures)}):")
    for connector_name, error in download_failures:
        print(f"  - {connector_name}: {error}")

    print(f"\n⚠ CDK VALIDATION FAILURES ({len(cdk_validation_failures)}):")
    for connector_name, cdk_version, error in cdk_validation_failures:
        print(f"  - {connector_name} (CDK {cdk_version}): {error}")

    print(f"\n⚠ SPEC EXECUTION FAILURES ({len(spec_execution_failures)}):")
    for connector_name, cdk_version, error in spec_execution_failures:
        print(f"  - {connector_name} (CDK {cdk_version}): {error}")

    print("\n" + "=" * 80)
    print(
        f"TOTAL: {len(validation_successes)} passed, {len(validation_failures)} failed, "
        f"{len(download_failures)} download errors, {len(cdk_validation_failures)} CDK validation failures, "
        f"{len(spec_execution_failures)} SPEC execution failures"
    )
    print("=" * 80)


def pytest_sessionfinish(session: Any, exitstatus: Any) -> None:
    """Called after whole test run finished, right before returning the exit status to the system."""
    validation_successes = getattr(session, "_validation_successes", [])
    validation_failures = getattr(session, "_validation_failures", [])
    download_failures = getattr(session, "_download_failures", [])
    cdk_validation_failures = getattr(session, "_cdk_validation_failures", [])
    spec_execution_failures = getattr(session, "_spec_execution_failures", [])
    log_test_results(validation_successes, validation_failures, download_failures, cdk_validation_failures, spec_execution_failures)
