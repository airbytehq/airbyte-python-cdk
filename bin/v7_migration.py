#!/usr/bin/env python3

# How to use:
# * Have a venv with yaml and toml
# * Run the script
# * Create the PR
# * Find and replace `[TBD](https://github.com/airbytehq/airbyte/pull/TBD)`


import argparse
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import toml
import yaml


class Connector:
    """Represents an Airbyte connector with its metadata and capabilities."""

    def __init__(self, path: Path):
        """
        Initialize a connector object.

        Args:
            path: Path to the connector directory
        """
        self.path = path
        self.name = path.name
        self.metadata = self._load_metadata()
        self._manifest_content = None  # Cache for manifest content

    def _load_metadata(self) -> Optional[Dict]:
        """Load metadata.yaml from the connector directory."""
        metadata_file = self.path / "metadata.yaml"

        if not metadata_file.exists():
            return None

        try:
            with open(metadata_file, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading metadata for {self.name}: {e}")
            raise e

    @property
    def manifest_content(self) -> Optional[str]:
        """
        Lazy-load and cache the manifest.yaml content.

        For Python connectors (with pyproject.toml), checks in the Python package directory.
        For manifest-only connectors, checks in the root directory.

        Returns:
            Manifest content as string, or None if file doesn't exist or can't be read
        """
        if self._manifest_content is not None:
            return self._manifest_content

        # Determine possible manifest.yaml locations
        manifest_locations = []

        # Location 1: Root directory (for manifest-only connectors)
        manifest_locations.append(self.path / "manifest.yaml")

        # Location 2: Python package directory (for Python connectors with pyproject.toml)
        if self.has_pyproject_toml():
            # Convert connector name to Python package name
            # e.g., source-zoom -> source_zoom
            package_name = self.name.replace("-", "_")
            python_package_manifest = self.path / package_name / "manifest.yaml"
            manifest_locations.append(python_package_manifest)

        # Try each location until we find a manifest file
        for manifest_file in manifest_locations:
            if manifest_file.exists():
                try:
                    with open(manifest_file, "r") as f:
                        self._manifest_content = f.read()
                    return self._manifest_content
                except Exception as e:
                    print(f"Error reading manifest from {manifest_file} for {self.name}: {e}")
                    continue

        # No manifest found in any location
        self._manifest_content = ""
        return self._manifest_content

    def is_source(self) -> bool:
        """
        Check if this connector is a source connector.

        Returns:
            True if the connector is a source, False otherwise
        """
        if not self.metadata:
            return False

        data = self.metadata.get("data", {})
        return data.get("connectorType") == "source"

    def is_using_unbounded_cdk_version_6(self) -> bool:
        """
        Check if this connector is using unbounded CDK version 6.

        This can be either:
        1. Using source-declarative-manifest with major version 6 (manifest-only connectors)
        2. Having pyproject.toml with airbyte_cdk dependency that uses caret (^) version specification (Python connectors)

        For pyproject.toml files, only dependencies with caret signs are considered "unbounded".

        Returns:
            True if the connector uses unbounded CDK version 6, False otherwise
        """
        # Check if it's a source connector first
        if not self.is_source():
            return False

        # Method 1: Check for source-declarative-manifest v6 base image
        build_options = self.metadata.get("data", {}).get("connectorBuildOptions", {})
        base_image = build_options.get("baseImage", "")

        if "source-declarative-manifest" in base_image:
            # Parse the version from the base image
            # Format: docker.io/airbyte/source-declarative-manifest:6.60.16@sha256:...
            # or: airbyte/source-declarative-manifest:6.60.16
            version_pattern = r"source-declarative-manifest:(\d+)\.[\d\.]+(?:@|$)"
            match = re.search(version_pattern, base_image)

            if match:
                major_version = int(match.group(1))
                if major_version == 6:
                    return True

        # Method 2: Check for pyproject.toml with unbounded airbyte_cdk dependency (with caret sign)
        pyproject_file = self.path / "pyproject.toml"
        if pyproject_file.exists():
            try:
                with open(pyproject_file, "r") as f:
                    pyproject_data = toml.load(f)

                # Check dependencies in different sections
                dependencies_sections = [
                    pyproject_data.get("tool", {}).get("poetry", {}).get("dependencies", {}),
                    pyproject_data.get("project", {}).get("dependencies", []),
                ]

                # Check poetry dependencies (dict format)
                poetry_deps = dependencies_sections[0]
                if isinstance(poetry_deps, dict) and "airbyte-cdk" in poetry_deps:
                    cdk_version = poetry_deps["airbyte-cdk"]
                    # Check if the version specification contains a caret sign
                    if isinstance(cdk_version, str) and "^6" in cdk_version:
                        return True

                # Check PEP 621 dependencies (list format)
                project_deps = dependencies_sections[1]
                if isinstance(project_deps, list):
                    for dep in project_deps:
                        if isinstance(dep, str) and dep.startswith("airbyte-cdk"):
                            # Check if the dependency specification contains a caret sign
                            if "^6" in dep:
                                return True

            except Exception as e:
                print(f"Error reading pyproject.toml for {self.name}: {e}")

        return False

    def has_components_file(self) -> bool:
        """
        Check if the connector has a components.py file.

        For Python connectors (with pyproject.toml), checks in the Python package directory.
        For manifest-only connectors, checks in the root directory.

        Returns:
            True if components.py exists in the connector directory, False otherwise
        """
        # Location 1: Root directory (for manifest-only connectors)
        components_file = self.path / "components.py"
        if components_file.exists():
            return True

        # Location 2: Python package directory (for Python connectors with pyproject.toml)
        if self.has_pyproject_toml():
            # Convert connector name to Python package name
            # e.g., source-zoom -> source_zoom
            package_name = self.name.replace("-", "_")
            python_package_components = self.path / package_name / "components.py"
            if python_package_components.exists():
                return True

        return False

    def has_pyproject_toml(self) -> bool:
        """
        Check if the connector has a pyproject.toml file.

        Returns:
            True if pyproject.toml exists in the connector directory, False otherwise
        """
        pyproject_file = self.path / "pyproject.toml"
        return pyproject_file.exists()

    def has_custom_incremental_sync(self) -> bool:
        """
        Check if the manifest file contains CustomIncrementalSync.

        Returns:
            True if CustomIncrementalSync is found in the manifest, False otherwise
        """
        content = self.manifest_content
        if not content:
            return False

        # Check for CustomIncrementalSync in the manifest content
        # This could appear in different ways:
        # - class_name: source_declarative_manifest.components.CustomIncrementalSync
        # - type: CustomIncrementalSync
        # - CustomIncrementalSync (as a direct reference)
        return "CustomIncrementalSync" in content

    def has_custom_retriever(self) -> bool:
        """
        Check if the manifest file contains CustomRetriever.

        Returns:
            True if CustomRetriever is found in the manifest, False otherwise
        """
        content = self.manifest_content
        if not content:
            return False  # No manifest or empty content means no CustomRetriever

        # Check for CustomRetriever in the manifest content
        # This could appear in different ways:
        # - class_name: source_declarative_manifest.components.CustomRetriever
        # - type: CustomRetriever
        # - CustomRetriever (as a direct reference)
        return "CustomRetriever" in content

    def has_custom_partition_router(self) -> bool:
        """
        Check if the manifest file contains CustomPartitionRouter.

        Returns:
            True if CustomPartitionRouter is found in the manifest, False otherwise
        """
        content = self.manifest_content
        if not content:
            return False  # No manifest or empty content means no CustomPartitionRouter

        # Check for CustomPartitionRouter in the manifest content
        # This could appear in different ways:
        # - class_name: source_declarative_manifest.components.CustomPartitionRouter
        # - type: CustomPartitionRouter
        # - CustomPartitionRouter (as a direct reference)
        return "CustomPartitionRouter" in content

    def imports_deprecated_class(self) -> bool:
        """
        Check if the connector imports any deprecated classes that need migration.

        Deprecated classes:
        - ManifestDeclarativeSource
        - DeclarativeStream
        - DeclarativeSource
        - DatetimeBasedCursor
        - DeclarativeCursor
        - GlobalSubstreamCursor
        - PerPartitionCursor
        - PerPartitionWithGlobalCursor
        - ResumableFullRefreshCursor

        Returns:
            True if any deprecated class is imported, False otherwise
        """
        deprecated_classes = {
            "ManifestDeclarativeSource",
            "DeclarativeStream",
            "DeclarativeSource",
            "DatetimeBasedCursor",
            "DeclarativeCursor",
            "GlobalSubstreamCursor",
            "PerPartitionCursor",
            "PerPartitionWithGlobalCursor",
            "ResumableFullRefreshCursor",
        }

        # Determine search directories
        search_paths = []

        # Always check root directory
        search_paths.append(self.path)

        # For Python connectors, also check the Python package directory
        if self.has_pyproject_toml():
            package_name = self.name.replace("-", "_")
            python_package_dir = self.path / package_name
            if python_package_dir.exists():
                search_paths.append(python_package_dir)

        # Search for Python files and check imports
        for search_path in search_paths:
            try:
                # Find all Python files recursively
                python_files = list(search_path.rglob("*.py"))

                for python_file in python_files:
                    try:
                        with open(python_file, "r", encoding="utf-8") as f:
                            content = f.read()

                        # Check for any deprecated class in the file content
                        for deprecated_class in deprecated_classes:
                            if deprecated_class in content:
                                # More precise check: look for actual import statements
                                lines = content.split("\n")
                                for line in lines:
                                    line = line.strip()
                                    if (
                                        (line.startswith("from ") or line.startswith("import "))
                                        and deprecated_class in line
                                        and "DeclarativeStreamModel" not in line
                                        and "YamlDeclarativeSource" not in line
                                    ):
                                        return True

                    except Exception as e:
                        print(f"Error reading Python file {python_file} for {self.name}: {e}")
                        continue

            except Exception as e:
                print(f"Error searching for Python files in {search_path} for {self.name}: {e}")
                continue

        return False

    def is_eligible_for_migration(self) -> bool:
        return (
            not self.has_custom_incremental_sync()
            and not self.has_custom_retriever()
            and not self.has_custom_partition_router()
            and not self.imports_deprecated_class()
            and not self.has_pyproject_toml()
        )

    def migrate_to_cdk_v7(self, sha256_hash: str = "af8807056f8218ecf0d4ec6b6ee717cdf20251fee5d2c1c77b5723771363b9b0") -> bool:
        """
        Migrate the connector to CDK version 7.

        This handles two cases:
        1. If the connector uses source-declarative-manifest, updates the metadata.yaml
           to use baseImage: docker.io/airbyte/source-declarative-manifest:7.0.0@sha256:<TBD>
        2. If the connector is Python, sets the version in pyproject.toml to ^7
        
        For both cases, it also increments the dockerImageTag in metadata.yaml.

        Args:
            sha256_hash: The SHA256 hash for the v7.0.0 base image (default: "<TBD>")

        Returns:
            True if migration was successful, False otherwise
        """
        print(f"Migrating {self.name} to CDK 7...")
        success = False

        # Case 1: Handle source-declarative-manifest connectors
        if self._uses_declarative_manifest():
            success = self._migrate_declarative_manifest_to_v7(sha256_hash)

        # Case 2: Handle Python connectors
        if self.has_pyproject_toml():
            success = self._migrate_python_connector_to_v7() or success

        # Case 3: Update changelog for all successful migrations
        if success:
            changelog_success = self._update_changelog()
            if not changelog_success:
                print(
                    f"Warning: changelog update failed for {self.name}, but migration was successful"
                )

        return success

    def _uses_declarative_manifest(self) -> bool:
        """Check if this connector uses source-declarative-manifest."""
        if not self.metadata:
            return False

        build_options = self.metadata.get("data", {}).get("connectorBuildOptions", {})
        base_image = build_options.get("baseImage", "")
        return "source-declarative-manifest" in base_image

    def _migrate_declarative_manifest_to_v7(self, sha256_hash: str) -> bool:
        """
        Migrate a declarative manifest connector to use CDK v7 base image.

        Args:
            sha256_hash: The SHA256 hash for the v7.0.0 base image

        Returns:
            True if migration was successful, False otherwise
        """
        try:
            metadata_file = self.path / "metadata.yaml"

            if not metadata_file.exists():
                print(f"No metadata.yaml found for {self.name}")
                return False

            # Load the current metadata
            with open(metadata_file, "r") as f:
                metadata_content = f.read()

            # Update the baseImage to version 7
            new_base_image = (
                f"docker.io/airbyte/source-declarative-manifest:7.0.0@sha256:{sha256_hash}"
            )

            # Replace the base image using regex to preserve formatting
            base_image_pattern = r"(baseImage:\s*)[^\n\r]+"
            replacement = rf"\g<1>{new_base_image}"

            updated_content = re.sub(base_image_pattern, replacement, metadata_content)

            # Write back the updated metadata with the new base image
            with open(metadata_file, "w") as f:
                f.write(updated_content)

            # Update dockerImageTag in metadata.yaml
            metadata_updated = self._update_docker_image_tag()
            if not metadata_updated:
                print(
                    f"Warning: dockerImageTag update failed for {self.name}, but baseImage was updated successfully"
                )

            print(f"Successfully migrated {self.name} to use CDK v7 declarative manifest")
            return True

        except Exception as e:
            print(f"Error migrating declarative manifest for {self.name}: {e}")
            return False

    def _migrate_python_connector_to_v7(self) -> bool:
        """
        Migrate a Python connector to use CDK v7.

        Updates the airbyte-cdk dependency in pyproject.toml to use ^7.

        Returns:
            True if migration was successful, False otherwise
        """
        try:
            pyproject_file = self.path / "pyproject.toml"

            if not pyproject_file.exists():
                print(f"No pyproject.toml found for {self.name}")
                return False

            # Load the current pyproject.toml
            with open(pyproject_file, "r") as f:
                pyproject_data = toml.load(f)

            updated = False

            # Check poetry dependencies (dict format)
            poetry_deps = pyproject_data.get("tool", {}).get("poetry", {}).get("dependencies", {})
            if isinstance(poetry_deps, dict) and "airbyte-cdk" in poetry_deps:
                poetry_deps["airbyte-cdk"] = "^7"
                updated = True

            # Check PEP 621 dependencies (list format)
            project_deps = pyproject_data.get("project", {}).get("dependencies", [])
            if isinstance(project_deps, list):
                for i, dep in enumerate(project_deps):
                    if isinstance(dep, str) and dep.startswith("airbyte-cdk"):
                        project_deps[i] = "airbyte-cdk>=7.0.0,<8.0.0"
                        updated = True

            if updated:
                with open(pyproject_file, "w") as f:
                    # WARNING: using toml to rewrite stuff can lead to changing the order/the formatting on some things
                    toml.dump(pyproject_data, f)

                poetry_lock_success = self._run_poetry_lock()
                if not poetry_lock_success:
                    print(
                        f"Warning: poetry lock failed for {self.name}, but pyproject.toml was updated successfully"
                    )

                # Update dockerImageTag in metadata.yaml
                metadata_updated = self._update_docker_image_tag()
                if not metadata_updated:
                    print(
                        f"Warning: dockerImageTag update failed for {self.name}, but pyproject.toml was updated successfully"
                    )

                print(f"Successfully migrated {self.name} to use CDK v7 in pyproject.toml")
                return True
            else:
                print(f"No airbyte-cdk dependency found in {self.name} pyproject.toml")
                return False

        except Exception as e:
            print(f"Error migrating Python connector {self.name}: {e}")
            return False

    def _update_docker_image_tag(self) -> bool:
        """
        Update the dockerImageTag field in metadata.yaml by incrementing the version.

        Returns:
            True if update was successful, False otherwise
        """
        try:
            metadata_file = self.path / "metadata.yaml"

            if not metadata_file.exists():
                print(f"No metadata.yaml found for {self.name}")
                return False

            # Load the current metadata
            with open(metadata_file, "r") as f:
                metadata_content = f.read()

            # Update the dockerImageTag field by incrementing the version
            docker_tag_pattern = r"(dockerImageTag:\s*)([^\n\r]+)"
            docker_tag_match = re.search(docker_tag_pattern, metadata_content)
            
            if docker_tag_match:
                current_tag = docker_tag_match.group(2).strip()
                new_tag = self._increment_version(current_tag)
                
                updated_content = re.sub(
                    docker_tag_pattern, 
                    rf"\g<1>{new_tag}", 
                    metadata_content
                )
                
                # Write back the updated metadata
                with open(metadata_file, "w") as f:
                    f.write(updated_content)
                
                print(f"Updated dockerImageTag from {current_tag} to {new_tag} for {self.name}")
                return True
            else:
                print(f"Warning: dockerImageTag not found in metadata.yaml for {self.name}")
                return False

        except Exception as e:
            print(f"Error updating dockerImageTag for {self.name}: {e}")
            return False

    def _run_poetry_lock(self) -> bool:
        """
        Run poetry lock to update the lock file after dependency changes.

        Returns:
            True if poetry lock was successful, False otherwise
        """
        try:
            print(f"Running poetry lock for {self.name}...")
            result = subprocess.run(
                ["poetry", "lock"],
                cwd=self.path,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
            )

            if result.returncode == 0:
                print(f"Successfully ran poetry lock for {self.name}")
                return True
            else:
                print(f"poetry lock failed for {self.name}:")
                print(f"stdout: {result.stdout}")
                print(f"stderr: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print(f"poetry lock timed out for {self.name}")
            return False
        except FileNotFoundError:
            print(f"poetry command not found. Please ensure Poetry is installed.")
            return False
        except Exception as e:
            print(f"Error running poetry lock for {self.name}: {e}")
            return False

    def _update_changelog(self) -> bool:
        """
        Update the changelog in docs/integrations/sources/<name>.md for CDK v7 migration.

        Returns:
            True if changelog update was successful, False otherwise
        """
        try:
            # Construct the changelog file path
            # Convert source-zoom -> zoom, source-hubspot -> hubspot, etc.
            if self.name.startswith("source-"):
                doc_name = self.name[7:]  # Remove 'source-' prefix
            else:
                doc_name = self.name

            changelog_file = (
                self.path.parent.parent.parent
                / "docs"
                / "integrations"
                / "sources"
                / f"{doc_name}.md"
            )

            if not changelog_file.exists():
                print(f"Changelog file not found: {changelog_file}")
                return False

            # Read the current changelog content
            with open(changelog_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Find the changelog table and get the current version
            current_version = self._extract_current_version(content)
            if not current_version:
                print(f"Could not determine current version for {self.name}")
                return False

            # Generate next version (increment patch version)
            next_version = self._increment_version(current_version)

            # Generate the new changelog entry
            current_date = datetime.now().strftime("%Y-%m-%d")
            new_entry = f"| {next_version} | {current_date} | [TBD](https://github.com/airbytehq/airbyte/pull/TBD) | Update to CDK v7.0.0 |"

            # Find the position to insert the new entry (after the table header)
            table_header_pattern = r"(\| Version \| Date.*?\n\| :--.*?\n)"
            match = re.search(table_header_pattern, content)

            if not match:
                print(f"Could not find changelog table header in {changelog_file}")
                return False

            # Insert the new entry right after the table header
            insert_position = match.end()
            updated_content = (
                content[:insert_position] + new_entry + "\n" + content[insert_position:]
            )

            # Write back the updated content
            with open(changelog_file, "w", encoding="utf-8") as f:
                f.write(updated_content)

            print(f"Successfully updated changelog for {self.name} with version {next_version}")
            return True

        except Exception as e:
            print(f"Error updating changelog for {self.name}: {e}")
            return False

    def _extract_current_version(self, content: str) -> Optional[str]:
        """
        Extract the current version from the changelog content.

        Args:
            content: The changelog file content

        Returns:
            Current version string or None if not found
        """
        # Look for the first version entry in the changelog table
        # Pattern: | 1.2.29 | 2025-08-23 | ...
        version_pattern = r"\| (\d+\.\d+\.\d+) \|"
        match = re.search(version_pattern, content)

        if match:
            return match.group(1)
        return None

    def _increment_version(self, version: str) -> str:
        """
        Increment the patch version number.

        Args:
            version: Version string like "1.2.29"

        Returns:
            Incremented version string like "1.2.30"
        """
        try:
            parts = version.split(".")
            if len(parts) == 3:
                major, minor, patch = parts
                new_patch = int(patch) + 1
                return f"{major}.{minor}.{new_patch}"
            else:
                print(f"Warning: unexpected version format: {version}")
                return f"{version}.1"  # Fallback
        except Exception as e:
            print(f"Error incrementing version {version}: {e}")
            return f"{version}.1"  # Fallback

    def __str__(self) -> str:
        return self.name


def get_connector_directories(connectors_path: Path) -> List[Path]:
    """
    Get all connector directories (source-* and destination-*).

    Args:
        connectors_path: Path to the connectors directory

    Returns:
        List of connector directory paths
    """
    connector_dirs = []

    if not connectors_path.exists():
        print(f"Connectors path does not exist: {connectors_path}")
        return connector_dirs

    for item in connectors_path.iterdir():
        if item.is_dir() and item.name.startswith("source-"):
            connector_dirs.append(item)

    return connector_dirs


def main():
    parser = argparse.ArgumentParser(
        description="Migrate Airbyte connectors to CDK version 7",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate 10 connectors (default count)
  python v7_migration.py --connectors-path /path/to/connectors
  
  # Migrate 5 connectors
  python v7_migration.py --connectors-path /path/to/connectors --count 5
  
  # Migrate all eligible connectors (use a large number)
  python v7_migration.py --connectors-path /path/to/connectors --count 1000
  
  # Use relative path
  python v7_migration.py --connectors-path ../connectors --count 3
        """,
    )

    parser.add_argument(
        "--airbyte-repo-path",
        type=str,
        default=None,
        help="Path to the connectors directory (required)",
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of eligible connectors to migrate (default: 10)",
    )

    args = parser.parse_args()

    airbyte_repo_path = Path(args.airbyte_repo_path).resolve()
    if not airbyte_repo_path.exists():
        print(f"Error: airbyte_repo_path path does not exist: {airbyte_repo_path}")
        return 1

    print(f"Using connectors path: {airbyte_repo_path}")

    connectors = [
        Connector(connector_path)
        for connector_path in get_connector_directories(
            airbyte_repo_path / "airbyte-integrations" / "connectors"
        )
    ]

    v6_declarative_sources = [c for c in connectors if c.is_using_unbounded_cdk_version_6()]
    eligible_for_migration = [c for c in v6_declarative_sources if c.is_eligible_for_migration()]
    non_eligible_for_migration = [
        c for c in v6_declarative_sources if not c.is_eligible_for_migration()
    ]

    print(
        f"{len(eligible_for_migration)} sources eligible for automatic migration out of {len(v6_declarative_sources)} sources using CDK version 6"
    )
    print(f"sources non-eligible are: {[c.name for c in non_eligible_for_migration]}")

    count_to_migrate = min(args.count, len(eligible_for_migration))
    print(f"Migrating {count_to_migrate} connectors")
    for i in range(count_to_migrate):
        eligible_for_migration[i].migrate_to_cdk_v7()

    print("DONE")


if __name__ == "__main__":
    main()
