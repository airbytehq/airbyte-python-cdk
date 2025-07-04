# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import os
import tempfile
from logging import Logger
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airbyte_cdk.utils.http_proxy import (
    AIRBYTE_NO_PROXY_ENTRIES,
    PROXY_CA_CERTIFICATE_CONFIG_KEY,
    PROXY_URL_CONFIG_KEY,
    _get_no_proxy_entries_from_env_var,
    _get_no_proxy_string,
    _install_ca_certificate,
    configure_custom_http_proxy,
)


class TestGetNoProxyEntriesFromEnvVar:
    def test_no_proxy_env_var_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            result = _get_no_proxy_entries_from_env_var()
            assert result == []

    def test_no_proxy_env_var_set_with_values(self):
        with patch.dict(os.environ, {"NO_PROXY": "example.com,test.org,*.local"}, clear=True):
            result = _get_no_proxy_entries_from_env_var()
            assert result == ["example.com", "test.org", "*.local"]

    def test_no_proxy_env_var_set_with_empty_values(self):
        with patch.dict(os.environ, {"NO_PROXY": "example.com,,test.org"}, clear=True):
            result = _get_no_proxy_entries_from_env_var()
            assert result == ["example.com", "test.org"]

    def test_no_proxy_env_var_set_with_whitespace(self):
        with patch.dict(os.environ, {"NO_PROXY": " example.com , test.org , *.local "}, clear=True):
            result = _get_no_proxy_entries_from_env_var()
            assert result == ["example.com", "test.org", "*.local"]


class TestGetNoProxyString:
    def test_no_existing_no_proxy_env_var(self):
        with patch.dict(os.environ, {}, clear=True):
            result = _get_no_proxy_string()
            expected_entries = set(AIRBYTE_NO_PROXY_ENTRIES)
            result_entries = set(result.split(","))
            assert result_entries == expected_entries

    def test_with_existing_no_proxy_env_var(self):
        with patch.dict(os.environ, {"NO_PROXY": "custom.example.com,test.org"}, clear=True):
            result = _get_no_proxy_string()
            result_entries = set(result.split(","))
            expected_entries = set(AIRBYTE_NO_PROXY_ENTRIES + ["custom.example.com", "test.org"])
            assert result_entries == expected_entries

    def test_deduplication(self):
        with patch.dict(
            os.environ, {"NO_PROXY": "localhost,127.0.0.1,custom.example.com"}, clear=True
        ):
            result = _get_no_proxy_string()
            result_entries = result.split(",")
            assert len(result_entries) == len(set(result_entries))


class TestInstallCaCertificate:
    def test_install_ca_certificate(self):
        test_cert = (
            "-----BEGIN CERTIFICATE-----\ntest certificate content\n-----END CERTIFICATE-----"
        )

        with patch.dict(os.environ, {}, clear=True):
            cert_path = _install_ca_certificate(test_cert)

            assert cert_path.exists()
            assert cert_path.suffix == ".pem"
            assert "airbyte-custom-ca-cert-" in cert_path.name

            with open(cert_path, "r") as f:
                content = f.read()
            assert content == test_cert

            assert os.environ["REQUESTS_CA_BUNDLE"] == str(cert_path)
            assert os.environ["CURL_CA_BUNDLE"] == str(cert_path)
            assert os.environ["SSL_CERT_FILE"] == str(cert_path)

            cert_path.unlink()


class TestConfigureCustomHttpProxy:
    def setup_method(self):
        self.logger = MagicMock(spec=Logger)

    def test_no_proxy_url_provided(self):
        with patch.dict(os.environ, {}, clear=True):
            configure_custom_http_proxy({}, logger=self.logger)

            assert "HTTP_PROXY" not in os.environ
            assert "HTTPS_PROXY" not in os.environ
            assert "NO_PROXY" not in os.environ
            self.logger.info.assert_not_called()

    def test_proxy_url_from_config(self):
        config = {PROXY_URL_CONFIG_KEY: "http://proxy.example.com:8080"}

        with patch.dict(os.environ, {}, clear=True):
            configure_custom_http_proxy(config, logger=self.logger)

            assert os.environ["HTTP_PROXY"] == "http://proxy.example.com:8080"
            assert os.environ["HTTPS_PROXY"] == "http://proxy.example.com:8080"
            assert "NO_PROXY" in os.environ
            self.logger.info.assert_called_once_with(
                "Using custom proxy URL: http://proxy.example.com:8080"
            )

    def test_proxy_url_from_parameter_overrides_config(self):
        config = {PROXY_URL_CONFIG_KEY: "http://config.proxy.com:8080"}

        with patch.dict(os.environ, {}, clear=True):
            configure_custom_http_proxy(
                config, logger=self.logger, proxy_url="http://param.proxy.com:8080"
            )

            assert os.environ["HTTP_PROXY"] == "http://param.proxy.com:8080"
            assert os.environ["HTTPS_PROXY"] == "http://param.proxy.com:8080"
            self.logger.info.assert_called_once_with(
                "Using custom proxy URL: http://param.proxy.com:8080"
            )

    def test_ca_certificate_from_config(self):
        test_cert = "-----BEGIN CERTIFICATE-----\ntest cert\n-----END CERTIFICATE-----"
        config = {
            PROXY_URL_CONFIG_KEY: "http://proxy.example.com:8080",
            PROXY_CA_CERTIFICATE_CONFIG_KEY: test_cert,
        }

        with patch.dict(os.environ, {}, clear=True):
            configure_custom_http_proxy(config, logger=self.logger)

            assert os.environ["HTTP_PROXY"] == "http://proxy.example.com:8080"
            assert "REQUESTS_CA_BUNDLE" in os.environ
            assert "CURL_CA_BUNDLE" in os.environ
            assert "SSL_CERT_FILE" in os.environ

            cert_path = Path(os.environ["REQUESTS_CA_BUNDLE"])
            assert cert_path.exists()

            with open(cert_path, "r") as f:
                content = f.read()
            assert content == test_cert

            assert self.logger.info.call_count == 2
            self.logger.info.assert_any_call(
                "Using custom proxy URL: http://proxy.example.com:8080"
            )

            cert_path.unlink()

    def test_ca_certificate_from_parameter_overrides_config(self):
        config_cert = "-----BEGIN CERTIFICATE-----\nconfig cert\n-----END CERTIFICATE-----"
        param_cert = "-----BEGIN CERTIFICATE-----\nparam cert\n-----END CERTIFICATE-----"
        config = {
            PROXY_URL_CONFIG_KEY: "http://proxy.example.com:8080",
            PROXY_CA_CERTIFICATE_CONFIG_KEY: config_cert,
        }

        with patch.dict(os.environ, {}, clear=True):
            configure_custom_http_proxy(config, logger=self.logger, ca_cert_file_text=param_cert)

            cert_path = Path(os.environ["REQUESTS_CA_BUNDLE"])
            with open(cert_path, "r") as f:
                content = f.read()
            assert content == param_cert

            cert_path.unlink()
