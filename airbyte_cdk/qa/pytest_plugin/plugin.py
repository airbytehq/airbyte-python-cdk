"""Pytest plugin for running QA checks on connectors."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.main import Session
from _pytest.python import Function, Module

from airbyte_cdk.qa.connector import Connector


def pytest_addoption(parser: Parser) -> None:
    """Add command-line options for the QA checks plugin.

    Args:
        parser: The pytest command-line parser
    """
    group = parser.getgroup("airbyte-qa")
    group.addoption(
        "--connector-name",
        action="store",
        dest="connector_name",
        help="Name of the connector to check",
    )
    group.addoption(
        "--connector-directory",
        action="store",
        dest="connector_directory",
        type=Path,
        help="Path to the connector directory",
    )
    group.addoption(
        "--report-path",
        action="store",
        dest="report_path",
        type=Path,
        help="Path to write the report to",
    )


@pytest.fixture(scope="session")
def connector(request: pytest.FixtureRequest) -> Connector:
    """Fixture for the connector under test.

    Args:
        request: The pytest request object

    Returns:
        Connector: The connector under test
    """
    connector_name = request.config.getoption("connector_name")
    connector_directory = request.config.getoption("connector_directory")
    
    if not connector_name and not connector_directory:
        cwd = Path.cwd()
        if cwd.name.startswith("source-") or cwd.name.startswith("destination-"):
            connector_name = cwd.name
            connector_directory = cwd
        else:
            for parent in cwd.parents:
                if parent.name.startswith("source-") or parent.name.startswith("destination-"):
                    connector_name = parent.name
                    connector_directory = parent
                    break
    
    if not connector_name and not connector_directory:
        pytest.fail("Could not determine connector name or directory. Please provide --connector-name or --connector-directory.")
    
    return Connector(connector_name, connector_directory)


class QACheckFunction(Function):
    """A pytest Function that represents a QA check."""

    def __init__(self, *args, check_name: str, check_description: str, **kwargs):
        """Initialize a QACheckFunction.

        Args:
            check_name: The name of the check
            check_description: The description of the check
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments
        """
        super().__init__(*args, **kwargs)
        self.check_name = check_name
        self.check_description = check_description
        
    def _getdocstring(self) -> Optional[str]:
        """Get the docstring for the check.

        Returns:
            Optional[str]: The docstring for the check
        """
        return self.check_description


def pytest_configure(config: Config) -> None:
    """Configure pytest for the QA checks plugin.

    Args:
        config: The pytest config object
    """
    config.addinivalue_line("markers", "qa_check: mark a test as a QA check")
    config.addinivalue_line("markers", "check_category(category): mark a test with a check category")
    config.addinivalue_line("markers", "connector_language(languages): mark a test as applicable to specific connector languages")
    config.addinivalue_line("markers", "connector_type(types): mark a test as applicable to specific connector types")
    config.addinivalue_line("markers", "connector_support_level(levels): mark a test as applicable to specific connector support levels")
    config.addinivalue_line("markers", "connector_cloud_usage(usages): mark a test as applicable to specific connector cloud usages")
    config.addinivalue_line("markers", "connector_ab_internal_sl(sl): mark a test as applicable to connectors with a specific ab_internal_sl")
    config.addinivalue_line("markers", "requires_metadata: mark a test as requiring metadata")
    config.addinivalue_line("markers", "runs_on_released_connectors: mark a test as running on released connectors")


def pytest_collection_modifyitems(config: Config, items: List[pytest.Item]) -> None:
    """Modify the collected test items to filter based on connector properties.

    Args:
        config: The pytest config object
        items: The collected test items
    """
    connector_name = config.getoption("connector_name")
    connector_directory = config.getoption("connector_directory")
    
    if not connector_name and not connector_directory:
        return
    
    connector_obj = Connector(connector_name, connector_directory)
    
    for item in items:
        if not item.get_closest_marker("qa_check"):
            continue
        
        if item.get_closest_marker("requires_metadata") and not connector_obj.metadata:
            item.add_marker(pytest.mark.skip(reason="This check requires metadata file to run"))
            continue
            
        if item.get_closest_marker("runs_on_released_connectors") is None and connector_obj.is_released:
            item.add_marker(pytest.mark.skip(reason="Check does not apply to released connectors"))
            continue
            
        if not connector_obj.language:
            item.add_marker(pytest.mark.skip(reason="Connector language could not be inferred"))
            continue
            
        languages_marker = item.get_closest_marker("connector_language")
        if languages_marker and connector_obj.language.value not in languages_marker.args[0]:
            item.add_marker(pytest.mark.skip(reason=f"Check does not apply to {connector_obj.language.value} connectors"))
            continue
            
        types_marker = item.get_closest_marker("connector_type")
        if types_marker and connector_obj.connector_type not in types_marker.args[0]:
            item.add_marker(pytest.mark.skip(reason=f"Check does not apply to {connector_obj.connector_type} connectors"))
            continue
            
        support_levels_marker = item.get_closest_marker("connector_support_level")
        if support_levels_marker and connector_obj.support_level not in support_levels_marker.args[0]:
            item.add_marker(pytest.mark.skip(reason=f"Check does not apply to {connector_obj.support_level} connectors"))
            continue
            
        cloud_usage_marker = item.get_closest_marker("connector_cloud_usage")
        if cloud_usage_marker and connector_obj.cloud_usage not in cloud_usage_marker.args[0]:
            item.add_marker(pytest.mark.skip(reason=f"Check does not apply to {connector_obj.cloud_usage} connectors"))
            continue
            
        ab_internal_sl_marker = item.get_closest_marker("connector_ab_internal_sl")
        if ab_internal_sl_marker and connector_obj.ab_internal_sl < ab_internal_sl_marker.args[0]:
            item.add_marker(pytest.mark.skip(reason=f"Check does not apply to connectors with sl < {ab_internal_sl_marker.args[0]}"))
            continue


def pytest_sessionfinish(session: Session, exitstatus: int) -> None:
    """Generate a report at the end of the test session.

    Args:
        session: The pytest session
        exitstatus: The exit status of the session
    """
    report_path = session.config.getoption("report_path")
    if not report_path:
        return
    
    report_path = Path(report_path)
    report_path.write_text("{}")
