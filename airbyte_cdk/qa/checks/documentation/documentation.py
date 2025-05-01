"""Documentation checks for Airbyte connectors."""

from pathlib import Path

from airbyte_cdk.qa.connector import Connector
from airbyte_cdk.qa.models import Check, CheckCategory, CheckResult


class DocumentationCheck(Check):
    """Base class for documentation checks."""

    category = CheckCategory.DOCUMENTATION


class CheckDocumentationExists(DocumentationCheck):
    """Check that connectors have documentation."""

    name = "Connectors must have documentation"
    description = (
        "Connectors must have documentation to ensure that users can understand how to use them."
    )

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        docs_dir = Path("/home/ubuntu/repos/airbyte/docs/integrations")
        connector_type_dir = docs_dir / (connector.connector_type + "s")

        doc_file = connector_type_dir / (
            connector.technical_name.replace("source-", "").replace("destination-", "") + ".md"
        )

        if not doc_file.exists():
            return self.create_check_result(
                connector=connector,
                passed=False,
                message=f"Documentation file {doc_file} does not exist",
            )
        return self.create_check_result(
            connector=connector,
            passed=True,
            message=f"Documentation file {doc_file} exists",
        )


ENABLED_CHECKS = [
    CheckDocumentationExists(),
]
