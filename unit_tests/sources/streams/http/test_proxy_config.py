#
#

import base64
import os
import tempfile
from unittest.mock import Mock, patch

import pytest
import requests
from requests.auth import HTTPProxyAuth

from airbyte_cdk.sources.streams.http.proxy_config import ProxyConfig


class TestProxyConfig:
    def test_proxy_config_disabled_by_default(self):
        """Test that proxy configuration is disabled by default."""
        config = ProxyConfig()
        assert not config.enabled
        assert config.get_proxies() == {}
        assert config.get_proxy_auth() is None

    def test_proxy_config_with_basic_settings(self):
        """Test proxy configuration with basic HTTP/HTTPS proxy settings."""
        config = ProxyConfig(
            enabled=True,
            http_proxy="http://proxy.example.com:8080",
            https_proxy="https://proxy.example.com:8443",
        )

        assert config.enabled
        proxies = config.get_proxies()
        assert proxies["http"] == "http://proxy.example.com:8080"
        assert proxies["https"] == "https://proxy.example.com:8443"

    def test_proxy_config_with_authentication(self):
        """Test proxy configuration with username/password authentication."""
        config = ProxyConfig(
            enabled=True,
            http_proxy="http://proxy.example.com:8080",
            proxy_username="testuser",
            proxy_password="testpass",
        )

        auth = config.get_proxy_auth()
        assert isinstance(auth, HTTPProxyAuth)
        assert auth.username == "testuser"
        assert auth.password == "testpass"

    def test_proxy_config_without_authentication(self):
        """Test proxy configuration without authentication."""
        config = ProxyConfig(enabled=True, http_proxy="http://proxy.example.com:8080")

        assert config.get_proxy_auth() is None

    def test_configure_session_disabled(self):
        """Test that session configuration does nothing when proxy is disabled."""
        config = ProxyConfig(enabled=False)
        session = requests.Session()
        original_proxies = session.proxies.copy()

        configured_session = config.configure_session(session)

        assert configured_session is session
        assert session.proxies == original_proxies

    def test_configure_session_with_proxies(self):
        """Test session configuration with proxy settings."""
        config = ProxyConfig(
            enabled=True,
            http_proxy="http://proxy.example.com:8080",
            https_proxy="https://proxy.example.com:8443",
            proxy_username="testuser",
            proxy_password="testpass",
        )

        session = requests.Session()
        configured_session = config.configure_session(session)

        assert configured_session is session
        assert session.proxies["http"] == "http://proxy.example.com:8080"
        assert session.proxies["https"] == "https://proxy.example.com:8443"
        assert isinstance(session.auth, HTTPProxyAuth)

    def test_configure_session_ssl_verification_disabled(self):
        """Test session configuration with SSL verification disabled."""
        config = ProxyConfig(
            enabled=True, http_proxy="http://proxy.example.com:8080", verify_ssl=False
        )

        session = requests.Session()
        configured_session = config.configure_session(session)

        assert configured_session is session
        assert session.verify is False

    @patch("tempfile.mkstemp")
    @patch("os.fchmod")
    @patch("os.write")
    @patch("os.close")
    def test_configure_session_with_ca_certificate(
        self, mock_close, mock_write, mock_fchmod, mock_mkstemp
    ):
        """Test session configuration with custom CA certificate."""
        cert_content = base64.b64encode(b"fake-ca-certificate").decode()
        mock_mkstemp.return_value = (123, "/tmp/test_ca_cert.pem")

        config = ProxyConfig(
            enabled=True, http_proxy="http://proxy.example.com:8080", ca_certificate=cert_content
        )

        session = requests.Session()
        configured_session = config.configure_session(session)

        assert configured_session is session
        assert session.verify == "/tmp/test_ca_cert.pem"
        mock_mkstemp.assert_called_once()
        mock_fchmod.assert_called_once_with(123, 0o600)
        mock_write.assert_called_once_with(123, b"fake-ca-certificate")
        mock_close.assert_called_once_with(123)

    @patch("tempfile.mkstemp")
    @patch("os.fchmod")
    @patch("os.write")
    @patch("os.close")
    def test_configure_session_with_client_certificates(
        self, mock_close, mock_write, mock_fchmod, mock_mkstemp
    ):
        """Test session configuration with client certificates for mutual TLS."""
        client_cert = base64.b64encode(b"fake-client-certificate").decode()
        client_key = base64.b64encode(b"fake-client-key").decode()

        mock_mkstemp.side_effect = [
            (123, "/tmp/test_client_cert.pem"),
            (124, "/tmp/test_client_key.pem"),
        ]

        config = ProxyConfig(
            enabled=True,
            http_proxy="http://proxy.example.com:8080",
            client_certificate=client_cert,
            client_key=client_key,
        )

        session = requests.Session()
        configured_session = config.configure_session(session)

        assert configured_session is session
        assert session.cert == ("/tmp/test_client_cert.pem", "/tmp/test_client_key.pem")
        assert mock_mkstemp.call_count == 2
        assert mock_fchmod.call_count == 2
        assert mock_write.call_count == 2

    def test_write_temp_cert_invalid_base64(self):
        """Test that invalid base64 certificate content raises ValueError."""
        config = ProxyConfig()

        with pytest.raises(ValueError, match="Invalid base64 certificate content"):
            config._write_temp_cert("invalid-base64!", "test_cert")

    def test_from_config_disabled(self):
        """Test creating ProxyConfig from configuration when disabled."""
        config_dict = {"proxy": {"enabled": False}}
        proxy_config = ProxyConfig.from_config(config_dict)

        assert proxy_config is None

    def test_from_config_enabled(self):
        """Test creating ProxyConfig from configuration when enabled."""
        config_dict = {
            "proxy": {
                "enabled": True,
                "http_proxy": "http://proxy.example.com:8080",
                "https_proxy": "https://proxy.example.com:8443",
                "proxy_username": "testuser",
                "proxy_password": "testpass",
                "verify_ssl": False,
            }
        }

        proxy_config = ProxyConfig.from_config(config_dict)

        assert proxy_config is not None
        assert proxy_config.enabled
        assert proxy_config.http_proxy == "http://proxy.example.com:8080"
        assert proxy_config.https_proxy == "https://proxy.example.com:8443"
        assert proxy_config.proxy_username == "testuser"
        assert proxy_config.proxy_password == "testpass"
        assert not proxy_config.verify_ssl

    @patch.dict(
        os.environ,
        {
            "HTTP_PROXY": "http://env-proxy.example.com:8080",
            "HTTPS_PROXY": "https://env-proxy.example.com:8443",
            "NO_PROXY": "localhost,127.0.0.1",
        },
    )
    def test_from_config_environment_variables(self):
        """Test creating ProxyConfig from environment variables when not explicitly configured."""
        config_dict = {}
        proxy_config = ProxyConfig.from_config(config_dict)

        assert proxy_config is not None
        assert proxy_config.enabled
        assert proxy_config.http_proxy == "http://env-proxy.example.com:8080"
        assert proxy_config.https_proxy == "https://env-proxy.example.com:8443"
        assert proxy_config.no_proxy == "localhost,127.0.0.1"

    def test_from_config_no_proxy_section(self):
        """Test creating ProxyConfig when no proxy section exists and no env vars."""
        config_dict = {"other_config": "value"}
        proxy_config = ProxyConfig.from_config(config_dict)

        assert proxy_config is None
