import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airbyte_cdk.test.pytest_config.plugin import (
    _create_dynamic_source_test_suite,
    _determine_connector_type,
    _is_connector_directory,
    pytest_generate_tests,
)


class TestPytestPlugin:
    def test_is_connector_directory(self, tmpdir):
        connector_dir = tmpdir / "connector"
        connector_dir.mkdir()
        (connector_dir / "metadata.yaml").write_text("sourceDefinitionId: 123", encoding="utf-8")

        with patch.dict(os.environ, {"AUTO_DISCOVER": "true"}):
            assert _is_connector_directory(str(connector_dir)) is True

        with patch.dict(os.environ, {"AUTO_DISCOVER": "false"}):
            assert _is_connector_directory(str(connector_dir)) is False

    def test_determine_connector_type(self, tmpdir):
        source_dir = tmpdir / "source"
        source_dir.mkdir()
        (source_dir / "source.py").write_text("class SourceTest: pass", encoding="utf-8")

        dest_dir = tmpdir / "destination"
        dest_dir.mkdir()
        (dest_dir / "destination.py").write_text("class DestinationTest: pass", encoding="utf-8")

        metadata_dir = tmpdir / "metadata"
        metadata_dir.mkdir()
        (metadata_dir / "metadata.yaml").write_text("sourceDefinitionId: 123", encoding="utf-8")

        assert _determine_connector_type(str(source_dir)) == "source"
        assert _determine_connector_type(str(dest_dir)) == "destination"
        assert _determine_connector_type(str(metadata_dir)) == "source"

    def test_create_dynamic_source_test_suite(self, tmpdir):
        source_dir = tmpdir / "source"
        source_dir.mkdir()
        source_file = source_dir / "source.py"
        source_file.write_text(
            """
            from airbyte_cdk.sources import AbstractSource
            
            class SourceTest(AbstractSource):
                def check_connection(self, logger, config):
                    return True, None
                
                def streams(self, config):
                    return []
            """,
            encoding="utf-8",
        )

        config_file = source_dir / "connector-acceptance-tests.yml"
        config_file.write_text(
            """
            test_read:
              config_path: config.json
            """,
            encoding="utf-8",
        )

        (source_dir / "config.json").write_text('{"api_key": "test"}', encoding="utf-8")

        with (
            patch("importlib.util.spec_from_file_location"),
            patch("importlib.util.module_from_spec"),
            patch("inspect.getmembers", return_value=[("SourceTest", type("SourceTest", (), {}))]),
        ):
            test_suite_class = _create_dynamic_source_test_suite(str(source_dir))

            assert test_suite_class is not None
            assert hasattr(test_suite_class, "working_dir")
            assert test_suite_class.working_dir == source_dir
            assert hasattr(test_suite_class, "acceptance_test_config_path")

    def test_pytest_generate_tests(self):
        metafunc = MagicMock()
        metafunc.fixturenames = ["instance"]

        scenarios = [MagicMock(), MagicMock()]
        scenarios[0].id = "scenario1"
        scenarios[1].id = "scenario2"

        mock_class = MagicMock()
        mock_class.get_scenarios.return_value = scenarios
        metafunc.cls = mock_class

        pytest_generate_tests(metafunc)

        metafunc.parametrize.assert_called_once()
        args, kwargs = metafunc.parametrize.call_args
        assert args[0] == "instance"
        assert args[1] == scenarios
        assert kwargs["ids"] == ["scenario1", "scenario2"]
