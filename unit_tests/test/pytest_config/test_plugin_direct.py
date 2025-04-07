import os
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from airbyte_cdk.test.pytest_config.plugin import (
    _create_dynamic_source_test_suite,
    _determine_connector_type,
    _is_connector_directory,
)


@pytest.mark.integration
def test_plugin_functionality():
    """Test that the plugin can auto-discover connector functionality."""
    sample_connector_path = str(Path(__file__).parent.parent.parent / "resources" / "sample_connector")
    
    with MonkeyPatch().context() as mp:
        mp.setenv("AUTO_DISCOVER", "true")
        assert _is_connector_directory(sample_connector_path) is True
    
    assert _determine_connector_type(sample_connector_path) == "source"
    
    test_suite_class = _create_dynamic_source_test_suite(sample_connector_path)
    assert test_suite_class is not None
    
    scenarios = test_suite_class.get_scenarios()
    assert len(scenarios) == 1
    assert scenarios[0].id == "default"
    
    connector = test_suite_class.create_connector(scenarios[0])
    assert connector is not None
