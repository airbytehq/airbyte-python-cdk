"""QA checks for Airbyte connectors."""

from airbyte_cdk.qa.checks.assets import ENABLED_CHECKS as ASSETS_CHECKS
from airbyte_cdk.qa.checks.documentation import ENABLED_CHECKS as DOCUMENTATION_CHECKS
from airbyte_cdk.qa.checks.metadata import ENABLED_CHECKS as METADATA_CORRECTNESS_CHECKS
from airbyte_cdk.qa.checks.packaging import ENABLED_CHECKS as PACKAGING_CHECKS
from airbyte_cdk.qa.checks.security import ENABLED_CHECKS as SECURITY_CHECKS
from airbyte_cdk.qa.checks.testing import ENABLED_CHECKS as TESTING_CHECKS

ENABLED_CHECKS = (
    DOCUMENTATION_CHECKS
    + METADATA_CORRECTNESS_CHECKS
    + PACKAGING_CHECKS
    + ASSETS_CHECKS
    + SECURITY_CHECKS
    + TESTING_CHECKS
)
