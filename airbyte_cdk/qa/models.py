
from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from airbyte_cdk.qa.connector import Connector, ConnectorLanguage

ALL_LANGUAGES = [
    ConnectorLanguage.JAVA,
    ConnectorLanguage.LOW_CODE,
    ConnectorLanguage.PYTHON,
    ConnectorLanguage.MANIFEST_ONLY,
]

ALL_TYPES = ["source", "destination"]


class CheckCategory(Enum):
    """The category of a QA check."""

    PACKAGING = "📦 Packaging"
    DOCUMENTATION = "📄 Documentation"
    ASSETS = "💼 Assets"
    SECURITY = "🔒 Security"
    METADATA = "📝 Metadata"
    TESTING = "🧪 Testing"


class CheckStatus(Enum):
    """The status of a QA check."""

    PASSED = "✅ Passed"
    FAILED = "❌ Failed"
    SKIPPED = "🔶 Skipped"


@dataclass
class CheckResult:
    """The result of a QA check.

    Attributes:
        check: The QA check that was run
        connector: The connector that was checked
        status: The status of the check
        message: A message explaining the result of the check
    """

    check: Check
    connector: Connector
    status: CheckStatus
    message: str

    def __repr__(self) -> str:
        return f"{self.connector} - {self.status.value} - {self.check.name}: {self.message}."


class Check(ABC):
    """Base class for all QA checks.

    This abstract class defines the interface for all QA checks. Subclasses must implement
    the name, description, category, and _run methods.
    """

    requires_metadata: bool = True
    runs_on_released_connectors: bool = True

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the QA check.

        Raises:
            NotImplementedError: Subclasses must implement name property/attribute

        Returns:
            str: The name of the QA check
        """
        raise NotImplementedError("Subclasses must implement name property/attribute")

    @property
    def required(self) -> bool:
        """Whether the QA check is required.

        Returns:
            bool: Whether the QA check is required
        """
        return True

    @property
    @abstractmethod
    def description(self) -> str:
        """A full description of the QA check. Used for documentation purposes.

        It can use markdown syntax.

        Raises:
            NotImplementedError: Subclasses must implement description property/attribute

        Returns:
            str: The description of the QA check
        """
        raise NotImplementedError("Subclasses must implement description property/attribute")

    @property
    def applies_to_connector_languages(self) -> List[ConnectorLanguage]:
        """The connector languages that the QA check applies to.

        Returns:
            List[ConnectorLanguage]: The connector languages that the QA check applies to
        """
        return ALL_LANGUAGES

    @property
    def applies_to_connector_types(self) -> List[str]:
        """The connector types that the QA check applies to.

        Returns:
            List[str]: The connector types that the QA check applies to
        """
        return ALL_TYPES

    @property
    def applies_to_connector_ab_internal_sl(self) -> int:
        """The connector ab_internal_s that the QA check applies to.

        Returns:
            int: integer value for connector ab_internal_sl level
        """
        return 0

    @property
    @abstractmethod
    def category(self) -> CheckCategory:
        """The category of the QA check.

        Raises:
            NotImplementedError: Subclasses must implement category property/attribute

        Returns:
            CheckCategory: The category of the QA check
        """
        raise NotImplementedError("Subclasses must implement category property/attribute")

    @property
    def applies_to_connector_support_levels(self) -> Optional[List[str]]:
        """The connector's support levels that the QA check applies to.

        Returns:
            List[str]: None if connector's support levels that the QA check applies to is not specified
        """
        return None

    @property
    def applies_to_connector_cloud_usage(self) -> Optional[List[str]]:
        """The connector's cloud usage level that the QA check applies to.

        Returns:
            List[str]: None if connector's cloud usage levels that the QA check applies to is not specified
        """
        return None

    def run(self, connector: Connector) -> CheckResult:
        """Run the QA check on the given connector.

        Args:
            connector: The connector to run the check on

        Returns:
            CheckResult: The result of the check
        """
        if not self.runs_on_released_connectors and connector.is_released:
            return self.skip(
                connector,
                "Check does not apply to released connectors",
            )
        if not connector.metadata and self.requires_metadata:
            return self.fail(
                connector,
                "This checks requires metadata file to run. Please add metadata.yaml file to the connector code directory.",
            )
        if not connector.language:
            return self.fail(connector, "Connector language could not be inferred")
        if connector.language not in self.applies_to_connector_languages:
            return self.skip(
                connector,
                f"Check does not apply to {connector.language.value} connectors",
            )
        if connector.connector_type not in self.applies_to_connector_types:
            return self.skip(
                connector,
                f"Check does not apply to {connector.connector_type} connectors",
            )
        if self.applies_to_connector_support_levels and connector.support_level not in self.applies_to_connector_support_levels:
            return self.skip(
                connector,
                f"Check does not apply to {connector.support_level} connectors",
            )
        if self.applies_to_connector_cloud_usage and connector.cloud_usage not in self.applies_to_connector_cloud_usage:
            return self.skip(
                connector,
                f"Check does not apply to {connector.cloud_usage} connectors",
            )
        if connector.ab_internal_sl < self.applies_to_connector_ab_internal_sl:
            return self.skip(
                connector,
                f"Check does not apply to connectors with sl < {self.applies_to_connector_ab_internal_sl}",
            )
        return self._run(connector)

    def _run(self, connector: Connector) -> CheckResult:
        """Run the actual check logic.

        This method must be implemented by subclasses.

        Args:
            connector: The connector to run the check on

        Raises:
            NotImplementedError: Subclasses must implement _run method

        Returns:
            CheckResult: The result of the check
        """
        raise NotImplementedError("Subclasses must implement _run method")

    def pass_(self, connector: Connector, message: str) -> CheckResult:
        """Create a passing check result.

        Args:
            connector: The connector that was checked
            message: A message explaining why the check passed

        Returns:
            CheckResult: A passing check result
        """
        return CheckResult(connector=connector, check=self, status=CheckStatus.PASSED, message=message)

    def fail(self, connector: Connector, message: str) -> CheckResult:
        """Create a failing check result.

        Args:
            connector: The connector that was checked
            message: A message explaining why the check failed

        Returns:
            CheckResult: A failing check result
        """
        return CheckResult(connector=connector, check=self, status=CheckStatus.FAILED, message=message)

    def skip(self, connector: Connector, reason: str) -> CheckResult:
        """Create a skipped check result.

        Args:
            connector: The connector that was checked
            reason: A reason explaining why the check was skipped

        Returns:
            CheckResult: A skipped check result
        """
        return CheckResult(connector=connector, check=self, status=CheckStatus.SKIPPED, message=reason)

    def create_check_result(self, connector: Connector, passed: bool, message: str) -> CheckResult:
        """Create a check result based on whether the check passed or failed.

        Args:
            connector: The connector that was checked
            passed: Whether the check passed
            message: A message explaining the result of the check

        Returns:
            CheckResult: The check result
        """
        status = CheckStatus.PASSED if passed else CheckStatus.FAILED
        return CheckResult(check=self, connector=connector, status=status, message=message)


@dataclass
class Report:
    """The report of a QA run.

    Attributes:
        check_results: The results of the QA checks
    """

    badge_name = "Connector QA Checks"
    check_results: list[CheckResult]
    image_shield_root_url = "https://img.shields.io/badge"

    def write(self, output_file: Path) -> None:
        """Write the report to a file.

        Args:
            output_file: The file to write the report to
        """
        output_file.write_text(self.to_json())

    def to_json(self) -> str:
        """Convert the report to a JSON-serializable dictionary.

        Returns:
            str: The report as a JSON string
        """
        connectors_report: Dict[str, Dict] = {}
        for check_result in self.check_results:
            connector = check_result.connector
            connectors_report.setdefault(
                connector.technical_name,
                {
                    "failed_checks": [],
                    "skipped_checks": [],
                    "passed_checks": [],
                    "failed_checks_count": 0,
                    "skipped_checks_count": 0,
                    "successful_checks_count": 0,
                    "total_checks_count": 0,
                },
            )
            check_name_and_message = {
                "check": check_result.check.name,
                "message": check_result.message,
            }
            if check_result.status == CheckStatus.PASSED:
                connectors_report[connector.technical_name]["passed_checks"].append(check_name_and_message)
                connectors_report[connector.technical_name]["successful_checks_count"] += 1
                connectors_report[connector.technical_name]["total_checks_count"] += 1

            elif check_result.status == CheckStatus.FAILED:
                connectors_report[connector.technical_name]["failed_checks"].append(check_name_and_message)
                connectors_report[connector.technical_name]["failed_checks_count"] += 1
                connectors_report[connector.technical_name]["total_checks_count"] += 1

            elif check_result.status == CheckStatus.SKIPPED:
                connectors_report[connector.technical_name]["skipped_checks"].append(check_name_and_message)
                connectors_report[connector.technical_name]["skipped_checks_count"] += 1
            else:
                raise ValueError(f"Invalid check status {check_result.status}")
        for connector_technical_name in connectors_report.keys():
            connectors_report[connector_technical_name]["badge_color"] = (
                "red" if connectors_report[connector_technical_name]["failed_checks_count"] > 0 else "green"
            )
            badge_name = self.badge_name.replace(" ", "_")
            badge_text = f"{connectors_report[connector_technical_name]['successful_checks_count']}/{connectors_report[connector_technical_name]['total_checks_count']}".replace(
                " ", "_"
            )
            connectors_report[connector_technical_name]["badge_text"] = badge_text
            connectors_report[connector_technical_name]["badge_url"] = (
                f"{self.image_shield_root_url}/{badge_name}-{badge_text}-{connectors_report[connector_technical_name]['badge_color']}"
            )
        return json.dumps(
            {
                "generation_timestamp": datetime.utcnow().isoformat(),
                "connectors": connectors_report,
            }
        )
