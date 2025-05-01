"""The `airbyte_cdk.qa` module provides quality assurance checks for Airbyte connectors.

This module includes a framework for running pre-release checks on connectors to ensure
they meet Airbyte's quality standards. The checks are organized into categories and can
be run individually or as a group.


The QA module includes the following check categories:

- **Packaging**: Checks related to connector packaging, including dependency management,
  versioning, and licensing.
- **Metadata**: Checks related to connector metadata, including language tags, CDK tags,
  and breaking changes deadlines.
- **Security**: Checks related to connector security, including HTTPS usage and base image
  requirements.
- **Assets**: Checks related to connector assets, including icons and other visual elements.
- **Documentation**: Checks related to connector documentation, ensuring it exists and is
  properly formatted.
- **Testing**: Checks related to connector testing, ensuring acceptance tests are present.


Checks can be configured based on various connector attributes:

- **Connector Language**: Checks can be configured to run only on connectors of specific
  languages (Python, Java, Low-Code, Manifest-Only).
- **Connector Type**: Checks can be configured to run only on specific connector types
  (source, destination).
- **Support Level**: Checks can be configured to run only on connectors with specific
  support levels (certified, community, etc.).
- **Cloud Usage**: Checks can be configured to run only on connectors with specific
  cloud usage settings (enabled, disabled, etc.).
- **Internal SL**: Checks can be configured to run only on connectors with specific
  internal service level requirements.


Checks can be run using the `airbyte-cdk connector pre-release-check` command:

```bash
airbyte-cdk connector pre-release-check --connector-name source-example

airbyte-cdk connector pre-release-check --connector-name source-example --check CheckConnectorUsesPoetry --check CheckVersionBump

airbyte-cdk connector pre-release-check --connector-directory /path/to/connector

airbyte-cdk connector pre-release-check --connector-name source-example --report-path report.json
```


The QA module is designed to be extensible. New checks can be added by creating a new
class that inherits from the `Check` base class and implementing the required methods.

Example:

```python
from airbyte_cdk.qa.models import Check, CheckCategory, CheckResult
from airbyte_cdk.qa.connector import Connector

class MyCustomCheck(Check):
    name = "My custom check"
    description = "Description of what my check verifies"
    category = CheckCategory.TESTING

    def _run(self, connector: Connector) -> CheckResult:
        if some_condition:
            return self.pass_(connector, "Check passed message")
        else:
            return self.fail(connector, "Check failed message")
```
"""
