#!/usr/bin/env python3
"""Generate documentation using MkDocs."""
import os
import subprocess
import sys
from pathlib import Path


def run():
    """Generate documentation using MkDocs build command."""
    docs_dir = Path(__file__).parent
    project_root = docs_dir.parent

    # Ensure we're in the project root
    os.chdir(project_root)

    # Create docs directory structure if it doesn't exist
    api_docs_dir = docs_dir / "api"
    api_docs_dir.mkdir(exist_ok=True)

    # Create index.md if it doesn't exist
    index_path = docs_dir / "index.md"
    if not index_path.exists():
        with open(index_path, "w") as f:
            f.write("""# Airbyte Python CDK

A framework for writing Airbyte Connectors.

## Overview

The Airbyte Python CDK provides a framework for building source and destination connectors for Airbyte.
It handles the complexity of implementing the Airbyte protocol, allowing you to focus on the connector-specific logic.

## Quick Links

- [GitHub Repository](https://github.com/airbytehq/airbyte-python-cdk)
- [API Reference](api/index.md)
- [Contributing Guide](CONTRIBUTING.md)
""")

    # Create API documentation index
    with open(api_docs_dir / "index.md", "w") as f:
        f.write("""# API Reference

This section contains the API reference for the Airbyte Python CDK.

## Package Structure

- [Core](core.md) - Core CDK functionality
- [Models](models.md) - Data models and protocol types
- [Sources](sources.md) - Base classes and utilities for source connectors
- [Destinations](destinations.md) - Base classes and utilities for destination connectors
""")

    # Run mkdocs build
    try:
        subprocess.run(["mkdocs", "build"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error generating documentation: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run()
