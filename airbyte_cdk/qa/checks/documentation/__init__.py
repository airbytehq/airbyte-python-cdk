"""Documentation checks for Airbyte connectors."""

from airbyte_cdk.qa.checks.documentation.documentation import (
    CheckDocumentationExists,
)

ENABLED_CHECKS = [
    CheckDocumentationExists(),
]
