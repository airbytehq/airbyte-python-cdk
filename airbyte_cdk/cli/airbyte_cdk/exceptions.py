# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Exceptions for the Airbyte CDK CLI."""

from dataclasses import dataclass
from typing import List

from airbyte_cdk.sql.exceptions import AirbyteConnectorError


@dataclass(kw_only=True)
class ConnectorSecretWithNoValidVersionsError(AirbyteConnectorError):
    """Error when a connector secret has no valid versions."""

    connector_name: str
    secret_names: List[str]
    gcp_project_id: str

    def __str__(self) -> str:
        """Return a string representation of the exception."""
        from airbyte_cdk.cli.airbyte_cdk._secrets import _get_secret_url
        
        urls = [_get_secret_url(secret_name, self.gcp_project_id) for secret_name in self.secret_names]
        urls_str = "\n".join([f"- {url}" for url in urls])
        secrets_str = ", ".join(self.secret_names)
        return (
            f"No valid versions found for the following secrets in connector '{self.connector_name}': {secrets_str}. "
            f"Please check the following URLs for more information:\n{urls_str}"
        )
