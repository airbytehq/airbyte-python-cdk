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


def md5_checksum(file_path: Path) -> str:
    with open(file_path, "rb") as file:
        return md5(file.read()).hexdigest()


class DeclarativeSourceTestSuite(SourceTestSuiteBase):
    manifest_path = Path("manifest.yaml")
    components_py_path: Path | None = None

    def create_connector(
        self, connector_test: ConnectorTestScenario
    ) -> ConcurrentDeclarativeSource:
        config = connector_test.get_config_dict()
        # catalog = connector_test.get_catalog()
        # state = connector_test.get_state()
        # source_config = connector_test.get_source_config()

        manifest_dict = yaml.safe_load(self.manifest_path.read_text())
        if self.components_py_path and self.components_py_path.exists():
            os.environ["AIRBYTE_ENABLE_UNSAFE_CODE"] = "true"
            config["__injected_components_py"] = self.components_py_path.read_text()
            config["__injected_components_py_checksums"] = {
                "md5": md5_checksum(self.components_py_path),
            }

        return ConcurrentDeclarativeSource(
            config=config,
            catalog=None,
            state=None,
            source_config=manifest_dict,
        )
