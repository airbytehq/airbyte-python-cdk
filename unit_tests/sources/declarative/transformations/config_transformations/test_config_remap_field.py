from copy import deepcopy
from unittest import TestCase

import pytest

from airbyte_cdk.sources.declarative.transformations.config_transformations import (
    ConfigRemapField as RemapField,
)


class TestRemapField(TestCase):
    def test_given_valid_inputs_when_transform_then_field_is_remapped(self):
        config = {
            "authorization": {
                "auth_type": "client_credentials",
                "client_id": "12345",
                "client_secret": "secret",
            }
        }
        remap_transform = RemapField(
            field_path=["authorization", "auth_type"],
            map={"client_credentials": "oauth2", "api_key": "key_auth"},
            config=config,
        )

        original_config = deepcopy(config)

        remap_transform.transform(config)

        assert config["authorization"]["auth_type"] == "oauth2"
        assert original_config["authorization"]["auth_type"] == "client_credentials"
        assert config["authorization"]["client_id"] == original_config["authorization"]["client_id"]
        assert (
            config["authorization"]["client_secret"]
            == original_config["authorization"]["client_secret"]
        )

    def test_given_value_not_in_map_when_transform_then_field_unchanged(self):
        remap_transform = RemapField(
            field_path=["authorization", "auth_type"],
            map={"client_credentials": "oauth2", "api_key": "key_auth"},
        )

        config = {
            "authorization": {"auth_type": "basic_auth", "username": "user", "password": "pass"}
        }
        original_config = deepcopy(config)
        remap_transform.transform(config)

        assert config["authorization"]["auth_type"] == "basic_auth"
        assert config == original_config

    def test_given_field_path_not_in_config_when_transform_then_config_unchanged(self):
        remap_transform = RemapField(
            field_path=["authentication", "type"],  # Not in config
            map={"client_credentials": "oauth2"},
        )
        config = {
            "authorization": {  # Different key
                "auth_type": "client_credentials"
            }
        }
        original_config = deepcopy(config)

        remap_transform.transform(config)

        assert config == original_config

    def test_given_interpolated_path_when_transform_then_field_is_remapped(self):
        remap_transform = RemapField(
            field_path=["auth_data", "{{ config['auth_field_name'] }}"],
            map={"basic": "basic_auth", "token": "bearer"},
        )

        config = {
            "auth_field_name": "type",
            "auth_data": {"type": "token", "token_value": "abc123"},
        }

        remap_transform.transform(config)

        assert config["auth_data"]["type"] == "bearer"

    def test_given_empty_map_when_transform_then_config_unchanged(self):
        remap_transform = RemapField(field_path=["authorization", "auth_type"], map={})

        config = {"authorization": {"auth_type": "client_credentials", "client_id": "12345"}}
        original_config = deepcopy(config)

        remap_transform.transform(config)

        assert config == original_config

    def test_given_empty_field_path_when_transform_then_raises_exception(self):
        with pytest.raises(Exception):
            RemapField(field_path=[], map={"old_value": "new_value"})

    def test_multiple_transformations_applied_in_sequence(self):
        auth_type_transform = RemapField(
            field_path=["auth", "type"], map={"api_key": "key_auth", "oauth": "oauth2"}
        )

        env_transform = RemapField(
            field_path=["environment"], map={"dev": "development", "prod": "production"}
        )

        config = {"auth": {"type": "oauth", "credentials": "secret"}, "environment": "dev"}

        auth_type_transform.transform(config)
        env_transform.transform(config)

        assert config["auth"]["type"] == "oauth2"
        assert config["environment"] == "development"

    def test_amazon_seller_partner_marketplace_remap_with_interpolated_mapping(self):
        mapping = {
            "endpoint": {
                "ES": "{{ 'https://sellingpartnerapi' if config.environment == 'production' else 'https://sandbox.sellingpartnerapi' }}-eu.amazon.com",
            }
        }
        sandbox_config = {"environment": "sandbox", "marketplace": "ES"}
        production_config = {"environment": "production", "marketplace": "ES"}
        RemapField(
            field_path=["marketplace"], map=mapping["endpoint"], config=sandbox_config
        ).transform(sandbox_config)
        RemapField(
            field_path=["marketplace"], map=mapping["endpoint"], config=production_config
        ).transform(production_config)

        assert sandbox_config["marketplace"] == "https://sandbox.sellingpartnerapi-eu.amazon.com"
        assert production_config["marketplace"] == "https://sellingpartnerapi-eu.amazon.com"
