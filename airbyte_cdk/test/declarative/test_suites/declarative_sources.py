import os
from hashlib import md5
from pathlib import Path
from typing import Any, cast

import yaml

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.declarative.models import ConnectorTestScenario
from airbyte_cdk.test.declarative.test_suites.source_base import (
    SourceTestSuiteBase,
)
from airbyte_cdk.test.declarative.utils.job_runner import IConnector


def md5_checksum(file_path: Path) -> str:
    with open(file_path, "rb") as file:
        return md5(file.read()).hexdigest()


class DeclarativeSourceTestSuite(SourceTestSuiteBase):
    manifest_path = Path("manifest.yaml")
    components_py_path: Path | None = None

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

        manifest_dict = yaml.safe_load(cls.manifest_path.read_text())
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
