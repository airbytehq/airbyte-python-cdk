# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import pytest
from pydantic.v1 import ValidationError

from airbyte_cdk.models.http_proxy_config import HttpProxyConfig


class TestHttpProxyConfig:
    def test_valid_config_with_required_fields_only(self):
        config = HttpProxyConfig(proxy_url="http://proxy.example.com:8080")

        assert config.proxy_url == "http://proxy.example.com:8080"
        assert config.proxy_ca_certificate is None

    def test_valid_config_with_all_fields(self):
        test_cert = "-----BEGIN CERTIFICATE-----\ntest certificate\n-----END CERTIFICATE-----"
        config = HttpProxyConfig(
            proxy_url="https://proxy.example.com:8080", proxy_ca_certificate=test_cert
        )

        assert config.proxy_url == "https://proxy.example.com:8080"
        assert config.proxy_ca_certificate == test_cert

    def test_missing_required_proxy_url(self):
        with pytest.raises(ValidationError) as exc_info:
            HttpProxyConfig()
        
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("proxy_url",)
        assert errors[0]["type"] == "value_error.missing"

    def test_empty_proxy_url(self):
        config = HttpProxyConfig(proxy_url="")
        assert config.proxy_url == ""

    def test_serialization(self):
        test_cert = "-----BEGIN CERTIFICATE-----\ntest certificate\n-----END CERTIFICATE-----"
        config = HttpProxyConfig(
            proxy_url="https://proxy.example.com:8080", proxy_ca_certificate=test_cert
        )

        serialized = config.dict()
        expected = {
            "proxy_url": "https://proxy.example.com:8080",
            "proxy_ca_certificate": test_cert,
        }
        assert serialized == expected

    def test_serialization_exclude_none(self):
        config = HttpProxyConfig(proxy_url="http://proxy.example.com:8080")

        serialized = config.dict(exclude_none=True)
        expected = {"proxy_url": "http://proxy.example.com:8080"}
        assert serialized == expected

    def test_json_serialization(self):
        config = HttpProxyConfig(proxy_url="http://proxy.example.com:8080")

        json_str = config.json()
        assert '"proxy_url": "http://proxy.example.com:8080"' in json_str
        assert '"proxy_ca_certificate": null' in json_str

    def test_from_dict(self):
        data = {"proxy_url": "https://proxy.example.com:8080", "proxy_ca_certificate": "test-cert"}

        config = HttpProxyConfig(**data)
        assert config.proxy_url == "https://proxy.example.com:8080"
        assert config.proxy_ca_certificate == "test-cert"

    def test_schema_generation(self):
        schema = HttpProxyConfig.schema()

        assert schema["type"] == "object"
        assert "proxy_url" in schema["properties"]
        assert "proxy_ca_certificate" in schema["properties"]

        proxy_url_prop = schema["properties"]["proxy_url"]
        assert proxy_url_prop["type"] == "string"
        assert proxy_url_prop["title"] == "Proxy URL"

        ca_cert_prop = schema["properties"]["proxy_ca_certificate"]
        assert ca_cert_prop["type"] == "string"
        assert ca_cert_prop["title"] == "Proxy CA Certificate"
        assert ca_cert_prop.get("airbyte_secret") is True

    def test_config_class_attributes(self):
        config_class = HttpProxyConfig.Config
        assert config_class.title == "HTTP Proxy Configuration"
        assert (
            config_class.description
            == "Configuration for routing HTTP requests through a proxy server"
        )
