"""Tests for the QA models."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airbyte_cdk.qa.connector import Connector, ConnectorLanguage
from airbyte_cdk.qa.models import Check, CheckCategory, CheckResult, CheckStatus, Report


class TestCheck(Check):
    """Test implementation of the Check abstract class."""

    name = "Test Check"
    description = "Test check description"
    category = CheckCategory.TESTING

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        return self.pass_(connector, "Test passed")


class TestCheckResult:
    """Tests for the CheckResult class."""

    def test_repr(self):
        """Test the __repr__ method."""
        connector = MagicMock()
        connector.__repr__.return_value = "test-connector"
        check = TestCheck()
        result = CheckResult(check=check, connector=connector, status=CheckStatus.PASSED, message="Test passed")
        assert repr(result) == "test-connector - ✅ Passed - Test Check: Test passed."


class TestReport:
    """Tests for the Report class."""

    def test_to_json(self):
        """Test the to_json method."""
        connector = MagicMock()
        connector.technical_name = "test-connector"
        check = TestCheck()
        result = CheckResult(check=check, connector=connector, status=CheckStatus.PASSED, message="Test passed")
        report = Report(check_results=[result])
        json_report = report.to_json()
        data = json.loads(json_report)
        assert "generation_timestamp" in data
        assert "connectors" in data
        assert "test-connector" in data["connectors"]
        assert data["connectors"]["test-connector"]["successful_checks_count"] == 1
        assert data["connectors"]["test-connector"]["total_checks_count"] == 1
        assert data["connectors"]["test-connector"]["failed_checks_count"] == 0
        assert data["connectors"]["test-connector"]["skipped_checks_count"] == 0
        assert len(data["connectors"]["test-connector"]["passed_checks"]) == 1
        assert data["connectors"]["test-connector"]["passed_checks"][0]["check"] == "Test Check"
        assert data["connectors"]["test-connector"]["passed_checks"][0]["message"] == "Test passed"

    def test_write(self, tmp_path):
        """Test the write method."""
        connector = MagicMock()
        connector.technical_name = "test-connector"
        check = TestCheck()
        result = CheckResult(check=check, connector=connector, status=CheckStatus.PASSED, message="Test passed")
        report = Report(check_results=[result])
        report_path = tmp_path / "report.json"
        report.write(report_path)
        assert report_path.exists()
        data = json.loads(report_path.read_text())
        assert "generation_timestamp" in data
        assert "connectors" in data
        assert "test-connector" in data["connectors"]
