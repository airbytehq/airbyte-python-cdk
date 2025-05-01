"""Testing checks for Airbyte connectors."""

from airbyte_cdk.qa.connector import Connector, ConnectorLanguage
from airbyte_cdk.qa.models import Check, CheckCategory, CheckResult


class TestingCheck(Check):
    """Base class for testing checks."""

    category = CheckCategory.TESTING


class CheckConnectorHasAcceptanceTests(TestingCheck):
    """Check that connectors have acceptance tests."""

    name = "Connectors must have acceptance tests"
    description = (
        "Connectors must have acceptance tests to ensure that they meet the Airbyte specification."
    )

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        acceptance_test_dir = connector.code_directory / "acceptance-test-config"
        if not acceptance_test_dir.exists():
            return self.create_check_result(
                connector=connector,
                passed=False,
                message="Acceptance test directory does not exist",
            )
        return self.create_check_result(
            connector=connector,
            passed=True,
            message="Acceptance test directory exists",
        )


ENABLED_CHECKS = [
    CheckConnectorHasAcceptanceTests(),
]
