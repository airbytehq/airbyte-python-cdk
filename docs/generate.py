#!/usr/bin/env python3
"""Generate documentation using MkDocs and copy to Airbyte docs repository."""

import os
import shutil
import subprocess
import sys
from pathlib import Path


def run():
    """Generate documentation using MkDocs build command and copy to Airbyte docs if available."""
    project_root = Path(__file__).parent.parent

    # Ensure we're in the project root
    os.chdir(project_root)

    # Run mkdocs build
    try:
        subprocess.run(["mkdocs", "build"], check=True)

        # Copy the built site to the main Airbyte docs if we're in the right environment
        airbyte_docs_dir = (
            project_root.parent
            / "airbyte"
            / "docs"
            / "connector-development"
            / "cdk-python"
            / "api-reference"
        )
        if airbyte_docs_dir.parent.exists():
            airbyte_docs_dir.mkdir(exist_ok=True)
            shutil.copytree(project_root / "site", airbyte_docs_dir / "site", dirs_exist_ok=True)
            print(f"Copied API reference to {airbyte_docs_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Error generating documentation: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Warning: Could not copy docs to Airbyte repo: {e}", file=sys.stderr)


if __name__ == "__main__":
    run()
