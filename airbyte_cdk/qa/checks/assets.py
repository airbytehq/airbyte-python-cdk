"""Asset checks for Airbyte connectors."""

from airbyte_cdk.qa import consts
from airbyte_cdk.qa.connector import Connector
from airbyte_cdk.qa.models import Check, CheckCategory, CheckResult


class AssetCheck(Check):
    """Base class for asset checks."""

    category = CheckCategory.ASSETS


class CheckConnectorHasIcon(AssetCheck):
    """Check that connectors have an icon."""

    name = "Connectors must have an icon"
    description = f"Connectors must have an icon file named `{consts.ICON_FILE_NAME}` in their code directory. This is to ensure that all connectors have a visual representation in the UI."

    def _run(self, connector: Connector) -> CheckResult:
        """Run the check.

        Args:
            connector: The connector to check

        Returns:
            CheckResult: The result of the check
        """
        icon_path = connector.code_directory / consts.ICON_FILE_NAME
        if not icon_path.exists():
            return self.create_check_result(
                connector=connector,
                passed=False,
                message=f"Icon file {consts.ICON_FILE_NAME} does not exist",
            )
        return self.create_check_result(
            connector=connector,
            passed=True,
            message=f"Icon file {consts.ICON_FILE_NAME} exists",
        )


ENABLED_CHECKS = [
    CheckConnectorHasIcon(),
]
