import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.integration
def test_plugin_auto_discovery():
    """Test that the plugin can auto-discover and run tests."""
    sample_connector_path = Path(__file__).parent.parent.parent / "resources" / "sample_connector"

    result = subprocess.run(
        [
            "pytest",
            "-xvs",
            "--auto-discover",
            f"--connector-dir={sample_connector_path}",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"Pytest failed: {result.stderr}"

    assert "Running test_connection[default]" in result.stdout
    assert "Running test_discover[default]" in result.stdout
    assert "2 passed" in result.stdout
