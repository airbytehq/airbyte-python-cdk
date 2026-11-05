import logging
import os
from collections.abc import Iterator, Mapping
from hashlib import md5
from pathlib import Path
from typing import Any, cast

import yaml
from boltons.typeutils import classproperty

from airbyte_cdk.models import AirbyteMessage, AirbyteStateMessage, ConfiguredAirbyteCatalog
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.models import ConnectorTestScenario
from airbyte_cdk.test.standard_tests._job_runner import IConnector
from airbyte_cdk.test.standard_tests.source_base import SourceTestSuiteBase
from airbyte_cdk.utils.connector_paths import MANIFEST_YAML


def md5_checksum(file_path: Path) -> str:
    """Calculate the MD5 checksum of the file text, as injected by `create_connector`.

    This is used to calculate the checksum of the `components.py` file, if it exists.
    """
    return md5(file_path.read_text().encode()).hexdigest()


class DeclarativeSourceTestSuite(SourceTestSuiteBase):
    """Declarative source test suite.

    This inherits from the Python-based source test suite and implements the
    `create_connector` method to create a declarative source object instead of
    requiring a custom Python source object.

    The class also automatically locates the `manifest.yaml` file and the
    `components.py` file (if it exists) in the connector's directory.
    """

    connector: type[IConnector] | None = None

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
        """Get the path to the `components.py` file, if one exists.

        If not `components.py` file exists, return None.
        """
        result = cls.get_connector_root_dir() / "components.py"
        if result.exists():
            return result

        return None

    @classmethod
    def _injected_config(
        cls,
        scenario: ConnectorTestScenario | None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """The scenario's config with the manifest (and `components.py`) injected, and the manifest."""
        scenario = scenario or ConnectorTestScenario()  # Use default (empty) scenario if None
        manifest_dict = yaml.safe_load(cls.manifest_yaml_path.read_text())
        config = {
            "__injected_manifest": manifest_dict,
        }
        config.update(
            scenario.get_config_dict(
                empty_if_missing=True,
                connector_root=cls.get_connector_root_dir(),
            ),
        )

        if cls.components_py_path and cls.components_py_path.exists():
            os.environ["AIRBYTE_ENABLE_UNSAFE_CODE"] = "true"
            config["__injected_components_py"] = cls.components_py_path.read_text()
            config["__injected_components_py_checksums"] = {
                "md5": md5_checksum(cls.components_py_path),
            }
        return config, manifest_dict

    @classmethod
    def create_connector(
        cls,
        scenario: ConnectorTestScenario | None,
    ) -> IConnector:
        """Create a connector scenario for the test suite.

        This overrides `create_connector` from the create a declarative source object
        instead of requiring a custom python source object.

        Subclasses should not need to override this method.

        `ConcurrentDeclarativeSource` reads only the state it was built with, not the state its
        `read` is given. A `read` with input state therefore reads from a source built with that
        state and the read's catalog, as `source-declarative-manifest` builds it for the platform.
        """
        config, manifest_dict = cls._injected_config(scenario)
        source = ConcurrentDeclarativeSource(
            config=config,
            catalog=None,
            state=None,
            source_config=manifest_dict,
        )
        read_without_state = source.read
        injected_config = config

        def read(
            logger: logging.Logger,
            config: Mapping[str, Any],
            catalog: ConfiguredAirbyteCatalog,
            state: list[AirbyteStateMessage] | None = None,
        ) -> Iterator[AirbyteMessage]:
            if not state:
                return read_without_state(logger, config, catalog, state)
            # Built from the config the first source was built with, as
            # `source-declarative-manifest` builds it from the config file. `config` is that
            # source's config after its migrations.
            return ConcurrentDeclarativeSource(
                config=injected_config,
                catalog=catalog,
                state=state,
                source_config=manifest_dict,
            ).read(logger, config, catalog, state)

        source.read = read  # type: ignore[method-assign]
        return cast(IConnector, source)

    def _create_connector_with_state(
        self,
        scenario: ConnectorTestScenario,
        *,
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage],
    ) -> IConnector:
        """The source a `read` with `catalog` and `state` reads from (see `create_connector`).

        A `StateDelegatingStream` is its incremental stream only in a source built with state.
        """
        config, manifest_dict = self._injected_config(scenario)
        return cast(
            IConnector,
            ConcurrentDeclarativeSource(
                config=config, catalog=catalog, state=state, source_config=manifest_dict
            ),
        )
