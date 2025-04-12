import os
from hashlib import md5
from pathlib import Path
from typing import Any, cast

import yaml
from boltons.typeutils import classproperty

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.declarative.models import ConnectorTestScenario
from airbyte_cdk.test.declarative.test_suites.connector_base import MANIFEST_YAML
from airbyte_cdk.test.declarative.test_suites.source_base import (
    SourceTestSuiteBase,
)
from airbyte_cdk.test.declarative.utils.job_runner import IConnector


def md5_checksum(file_path: Path) -> str:
    with open(file_path, "rb") as file:
        return md5(file.read()).hexdigest()


class DeclarativeSourceTestSuite(SourceTestSuiteBase):
    @classproperty
    def manifest_yaml_path(cls) -> Path:
        """Get the path to the manifest.yaml file."""
        result = cls.get_connector_root_dir() / MANIFEST_YAML
        if result.exists():
            return result

        raise FileNotFoundError(
            f"Manifest YAML file not found at {result}. "
            "Please ensure that the test suite is run in the correct directory.",
        )

    @classproperty
    def components_py_path(cls) -> Path | None:
        """Get the path to the components.py file."""
        result = cls.get_connector_root_dir() / "components.py"
        if result.exists():
            return result

        return None

    @classmethod
    def create_connector(
        cls,
        scenario: ConnectorTestScenario,
    ) -> IConnector:
        """Create a connector instance for the test suite."""
        config: dict[str, Any] = scenario.get_config_dict()
        # catalog = scenario.get_catalog()
        # state = scenario.get_state()
        # source_config = scenario.get_source_config()

        manifest_dict = yaml.safe_load(cls.manifest_yaml_path.read_text())
        if cls.components_py_path and cls.components_py_path.exists():
            os.environ["AIRBYTE_ENABLE_UNSAFE_CODE"] = "true"
            config["__injected_components_py"] = cls.components_py_path.read_text()
            config["__injected_components_py_checksums"] = {
                "md5": md5_checksum(cls.components_py_path),
            }

        return cast(
            IConnector,
            ConcurrentDeclarativeSource(
                config=config,
                catalog=None,
                state=None,
                source_config=manifest_dict,
            ),
        )
