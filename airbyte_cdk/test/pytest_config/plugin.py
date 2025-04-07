import importlib.util
import inspect
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Type, TypeVar, Union, cast

import pytest
import yaml
from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.nodes import Item
from _pytest.python import Metafunc, Module

from airbyte_cdk.sources import AbstractSource, Source
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.declarative.models import ConnectorTestScenario
from airbyte_cdk.test.declarative.test_suites.connector_base import ConnectorTestSuiteBase
from airbyte_cdk.test.declarative.test_suites.destination_base import DestinationTestSuiteBase
from airbyte_cdk.test.declarative.test_suites.source_base import SourceTestSuiteBase


def pytest_collect_file(parent: pytest.Collector, path: Any) -> Optional[Module]:
    """Handle file collection for pytest.

    Args:
        parent: The parent collector
        path: The path to the file being collected
    """
    path_str = str(path)
    path_name = os.path.basename(path_str)

    if path_name == "__init__.py":
        return None

    if path_name == "test_connector.py":
        return pytest.Module.from_parent(parent, path=path)  # type: ignore

    connector_dir = os.environ.get("CONNECTOR_DIR") or os.getcwd()
    path_parent = os.path.dirname(path_str)
    if path_parent == connector_dir and _is_connector_directory(connector_dir):
        return ConnectorTestNode.from_parent(parent, path=path)  # type: ignore

    return None


def pytest_configure(config: Config) -> None:
    """Configure pytest."""
    config.addinivalue_line("markers", "connector: mark test as a connector test")
    config.addinivalue_line("markers", "source: mark test as a source connector test")
    config.addinivalue_line("markers", "destination: mark test as a destination connector test")
    config.addinivalue_line("markers", "auto_discover: mark test as auto-discovered")


def pytest_addoption(parser: Parser) -> None:
    """Add custom CLI options to pytest."""
    parser.addoption(
        "--run-connector",
        action="store_true",
        default=False,
        help="run connector tests",
    )
    parser.addoption(
        "--connector-dir",
        action="store",
        default=None,
        help="directory containing the connector to test",
    )
    parser.addoption(
        "--auto-discover",
        action="store_true",
        default=False,
        help="enable automatic discovery of connector tests",
    )


class ConnectorTestNode(pytest.File):
    """Custom pytest collector for auto-discovered connector tests."""

    def collect(self) -> Generator[pytest.Item, None, None]:
        """Collect test items from a connector directory."""
        connector_dir = os.environ.get("CONNECTOR_DIR") or os.getcwd()
        connector_type = _determine_connector_type(connector_dir)

        if connector_type == "source":
            test_class = _create_dynamic_source_test_suite(connector_dir)
            if test_class:
                for name, method in inspect.getmembers(test_class, inspect.isfunction):
                    if name.startswith("test_"):
                        yield AutoDiscoveredTestItem.from_parent(
                            self, name=name, test_class=test_class, test_method=method
                        )
        elif connector_type == "destination":
            test_class = _create_dynamic_destination_test_suite(connector_dir)
            if test_class:
                for name, method in inspect.getmembers(test_class, inspect.isfunction):
                    if name.startswith("test_"):
                        yield AutoDiscoveredTestItem.from_parent(
                            self, name=name, test_class=test_class, test_method=method
                        )


T = TypeVar("T")


class AutoDiscoveredTestItem(pytest.Item):
    """Custom pytest item for auto-discovered tests."""

    def __init__(
        self,
        name: str,
        parent: pytest.Collector,
        test_class: Type[T],
        test_method: Callable[..., Any],
    ) -> None:
        super().__init__(name, parent)
        self.test_class = test_class
        self.test_method = test_method
        self.add_marker(pytest.mark.auto_discover)

    def runtest(self) -> None:
        """Run the test."""
        instance = self.test_class()

        get_scenarios = getattr(self.test_class, "get_scenarios", None)
        scenarios = (
            get_scenarios() if callable(get_scenarios) else [ConnectorTestScenario(id="default")]
        )

        for scenario in scenarios:
            instance_name = f"{self.name}[{scenario.id or 'default'}]"
            print(f"Running {instance_name}")
            self.test_method(instance, scenario)

    def reportinfo(self) -> Tuple[Path, Optional[int], str]:
        """Return test location information."""
        return self.fspath, None, f"{self.name}"


def _is_connector_directory(directory_path: str) -> bool:
    """Check if a directory is a connector directory."""
    auto_discover = os.environ.get("AUTO_DISCOVER") == "true"

    try:
        config = getattr(pytest, "config", None)
        if config and config.getoption("--auto-discover", False):
            auto_discover = True
    except (AttributeError, ValueError):
        pass

    if not auto_discover:
        return False

    path = Path(directory_path)

    indicator_files = [
        path / "metadata.yaml",
        path / "source.py",
        path / "destination.py",
        path / "manifest.yaml",
    ]

    return any(file.exists() for file in indicator_files)


def _determine_connector_type(directory_path: str) -> str:
    """Determine if a directory contains a source or destination connector."""
    path = Path(directory_path)

    if (path / "source.py").exists():
        return "source"
    if (path / "destination.py").exists():
        return "destination"

    metadata_path = path / "metadata.yaml"
    if metadata_path.exists():
        with open(metadata_path, "r") as f:
            content = f.read()
            if "sourceDefinitionId" in content:
                return "source"
            elif "destinationDefinitionId" in content:
                return "destination"

    return "source"


def _create_dynamic_source_test_suite(connector_dir: str) -> Optional[Type[Any]]:
    """Create a dynamic source test suite class for a discovered connector."""
    connector_path = Path(connector_dir)

    source_file = None
    for file in connector_path.glob("**/*.py"):
        if file.name == "source.py":
            source_file = file
            break

    if not source_file:
        return None

    try:
        module_name = f"discovered_source_{connector_path.name.replace('-', '_')}"
        spec = importlib.util.spec_from_file_location(module_name, source_file)
        if spec is None:
            return None

        module = importlib.util.module_from_spec(spec)
        if spec.loader is None:
            return None

        spec.loader.exec_module(module)

        source_class = None
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and name.startswith("Source"):
                source_class = obj
                break

        if not source_class:
            return None

        class DiscoveredSourceTestSuite(SourceTestSuiteBase):
            connector = source_class
            working_dir = connector_path

            acceptance_test_config_path = next(
                (
                    path
                    for path in [
                        connector_path / "connector-acceptance-tests.yml",
                        connector_path / "acceptance-test-config.yml",
                    ]
                    if path.exists()
                ),
                None,  # type: ignore
            )

            @classmethod
            def create_connector(cls, scenario: ConnectorTestScenario) -> Any:
                return cls.connector() if callable(cls.connector) else None

            @classmethod
            def get_scenarios(cls) -> List[ConnectorTestScenario]:
                """Get test scenarios from acceptance test config if it exists."""
                if cls.acceptance_test_config_path and cls.acceptance_test_config_path.exists():
                    with open(cls.acceptance_test_config_path, "r") as f:
                        config = yaml.safe_load(f)

                    scenarios = []
                    if "test_read" in config and "config_path" in config["test_read"]:
                        config_path = Path(connector_path) / config["test_read"]["config_path"]
                        if config_path.exists():
                            with open(config_path, "r") as f:
                                config_dict = yaml.safe_load(f)
                            scenarios.append(
                                ConnectorTestScenario(
                                    id="default",
                                    config_dict=config_dict,
                                )
                            )

                    if scenarios:
                        return scenarios

                return [ConnectorTestScenario(id="default")]

        return DiscoveredSourceTestSuite

    except Exception as e:
        print(f"Error creating dynamic test suite: {e}")
        return None


def _create_dynamic_destination_test_suite(connector_dir: str) -> Optional[Type[Any]]:
    """Create a dynamic destination test suite class for a discovered connector."""
    connector_path = Path(connector_dir)

    destination_file = None
    for file in connector_path.glob("**/*.py"):
        if file.name == "destination.py":
            destination_file = file
            break

    if not destination_file:
        return None

    try:
        module_name = f"discovered_destination_{connector_path.name.replace('-', '_')}"
        spec = importlib.util.spec_from_file_location(module_name, destination_file)
        if spec is None:
            return None

        module = importlib.util.module_from_spec(spec)
        if spec.loader is None:
            return None

        spec.loader.exec_module(module)

        destination_class = None
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and name.startswith("Destination"):
                destination_class = obj
                break

        if not destination_class:
            return None

        class DiscoveredDestinationTestSuite(DestinationTestSuiteBase):
            connector = destination_class
            working_dir = connector_path

            acceptance_test_config_path = next(
                (
                    path
                    for path in [
                        connector_path / "connector-acceptance-tests.yml",
                        connector_path / "acceptance-test-config.yml",
                    ]
                    if path.exists()
                ),
                None,  # type: ignore
            )

            @classmethod
            def create_connector(cls, scenario: ConnectorTestScenario) -> Any:
                return cls.connector() if callable(cls.connector) else None

            @classmethod
            def get_scenarios(cls) -> List[ConnectorTestScenario]:
                """Get test scenarios from acceptance test config if it exists."""
                if cls.acceptance_test_config_path and cls.acceptance_test_config_path.exists():
                    with open(cls.acceptance_test_config_path, "r") as f:
                        config = yaml.safe_load(f)

                    scenarios = []
                    if "test_read" in config and "config_path" in config["test_read"]:
                        config_path = Path(connector_path) / config["test_read"]["config_path"]
                        if config_path.exists():
                            with open(config_path, "r") as f:
                                config_dict = yaml.safe_load(f)
                            scenarios.append(
                                ConnectorTestScenario(
                                    id="default",
                                    config_dict=config_dict,
                                )
                            )

                    if scenarios:
                        return scenarios

                return [ConnectorTestScenario(id="default")]

        return DiscoveredDestinationTestSuite

    except Exception as e:
        print(f"Error creating dynamic test suite: {e}")
        return None


def pytest_collection_modifyitems(config: Config, items: List[Item]) -> None:
    """Modify collected items based on CLI options."""
    if not config.getoption("--run-connector") and not config.getoption("--auto-discover"):
        skip_connector = pytest.mark.skip(
            reason="need --run-connector or --auto-discover option to run"
        )
        for item in items:
            if "connector" in item.keywords or "auto_discover" in item.keywords:
                item.add_marker(skip_connector)

    if config.getoption("--auto-discover"):
        os.environ["AUTO_DISCOVER"] = "true"

    connector_dir = config.getoption("--connector-dir")
    if connector_dir:
        os.environ["CONNECTOR_DIR"] = connector_dir


def pytest_generate_tests(metafunc: Metafunc) -> None:
    """Generate tests from scenarios.

    This hook allows for the automatic parametrization of test methods
    with scenarios from test classes, without requiring explicit calls
    to generate_tests in test files.
    """
    if "instance" in metafunc.fixturenames:
        test_class = metafunc.cls
        if test_class is None:
            return

        get_scenarios = getattr(test_class, "get_scenarios", None)
        if callable(get_scenarios):
            scenarios = test_class.get_scenarios()
            if scenarios:
                ids = [scenario.id or f"scenario_{i}" for i, scenario in enumerate(scenarios)]
                metafunc.parametrize("instance", scenarios, ids=ids)


def pytest_runtest_setup(item: Item) -> None:
    """This hook is called before each test function is executed."""
    print(f"Setting up test: {item.name}")


def pytest_runtest_teardown(item: Item, nextitem: Optional[Item]) -> None:
    """This hook is called after each test function is executed."""
    print(f"Tearing down test: {item.name}")
