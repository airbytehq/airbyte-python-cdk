# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""HTTP proxy configuration models."""

from typing import Optional

from pydantic.v1 import BaseModel, Field


class HttpProxyConfig(BaseModel):
    """Configuration model for HTTP proxy settings."""

    proxy_url: str = Field(
        ...,
        title="Proxy URL",
        description="The URL of the HTTP proxy server to use for requests",
        examples=["http://proxy.example.com:8080", "https://proxy.example.com:8080"],
    )
    proxy_ca_certificate: Optional[str] = Field(
        None,
        title="Proxy CA Certificate",
        description="Custom CA certificate for the proxy server in PEM format",
        airbyte_secret=True,
    )

    class Config:
        title = "HTTP Proxy Configuration"
        description = "Configuration for routing HTTP requests through a proxy server"
