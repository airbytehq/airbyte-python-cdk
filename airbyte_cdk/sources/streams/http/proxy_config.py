#
#

import base64
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

import requests
from requests.auth import HTTPProxyAuth


@dataclass
class ProxyConfig:
    """Configuration for HTTP/HTTPS proxy settings."""

    enabled: bool = False
    http_proxy: Optional[str] = None
    https_proxy: Optional[str] = None
    no_proxy: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None
    verify_ssl: bool = True
    ca_certificate: Optional[str] = None
    client_certificate: Optional[str] = None
    client_key: Optional[str] = None

    def get_proxies(self) -> Dict[str, str]:
        """Get proxy configuration for requests library."""
        if not self.enabled:
            return {}

        proxies = {}
        if self.http_proxy:
            proxies["http"] = self.http_proxy
        if self.https_proxy:
            proxies["https"] = self.https_proxy

        return proxies

    def get_proxy_auth(self) -> Optional[HTTPProxyAuth]:
        """Get proxy authentication if configured."""
        if self.proxy_username and self.proxy_password:
            return HTTPProxyAuth(self.proxy_username, self.proxy_password)
        return None

    def configure_session(self, session: requests.Session) -> requests.Session:
        """Configure a requests session with proxy settings."""
        if not self.enabled:
            return session

        proxies = self.get_proxies()
        if proxies:
            session.proxies.update(proxies)

        proxy_auth = self.get_proxy_auth()
        if proxy_auth:
            session.auth = proxy_auth

        if not self.verify_ssl:
            session.verify = False
        elif self.ca_certificate:
            ca_cert_path = self._write_temp_cert(self.ca_certificate, "ca_cert")
            session.verify = ca_cert_path

        if self.client_certificate and self.client_key:
            client_cert_path = self._write_temp_cert(self.client_certificate, "client_cert")
            client_key_path = self._write_temp_cert(self.client_key, "client_key")
            session.cert = (client_cert_path, client_key_path)

        return session

    def _write_temp_cert(self, cert_content: str, cert_type: str) -> str:
        """Write certificate content to a secure temporary file."""
        try:
            cert_bytes = base64.b64decode(cert_content)
        except Exception as e:
            raise ValueError(f"Invalid base64 certificate content for {cert_type}: {e}")

        fd, temp_path = tempfile.mkstemp(suffix=f"_{cert_type}.pem", prefix="airbyte_proxy_")
        try:
            os.fchmod(fd, 0o600)  # Restrict to owner only
            os.write(fd, cert_bytes)
        finally:
            os.close(fd)

        return temp_path

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> Optional["ProxyConfig"]:
        """Create ProxyConfig from connector configuration."""
        proxy_section = config.get("proxy", {})

        if not proxy_section.get("enabled", False):
            http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
            https_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
            no_proxy = os.getenv("NO_PROXY") or os.getenv("no_proxy")

            if http_proxy or https_proxy:
                return cls(
                    enabled=True,
                    http_proxy=http_proxy,
                    https_proxy=https_proxy,
                    no_proxy=no_proxy,
                )
            return None

        return cls(
            enabled=proxy_section.get("enabled", False),
            http_proxy=proxy_section.get("http_proxy"),
            https_proxy=proxy_section.get("https_proxy"),
            no_proxy=proxy_section.get("no_proxy"),
            proxy_username=proxy_section.get("proxy_username"),
            proxy_password=proxy_section.get("proxy_password"),
            verify_ssl=proxy_section.get("verify_ssl", True),
            ca_certificate=proxy_section.get("ca_certificate"),
            client_certificate=proxy_section.get("client_certificate"),
            client_key=proxy_section.get("client_key"),
        )
