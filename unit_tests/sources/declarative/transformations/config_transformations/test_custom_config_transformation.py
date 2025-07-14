#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Dict, MutableMapping, Optional

from airbyte_cdk.sources.declarative.transformations.config_transformations.config_transformation import (
    ConfigTransformation,
)


class MockCustomConfigTransformation(ConfigTransformation):
    """
    A mock custom config transformation for testing purposes.
    This simulates what a real custom transformation would look like.
    """

    def __init__(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.parameters = parameters or {}

    def transform(self, config: MutableMapping[str, Any]) -> None:
        """
        Transform the config by adding a test field.
        This simulates the behavior of a real custom transformation.
        """
        # Only modify user config keys, avoid framework-injected keys
        # Check if there are any user keys (not starting with __)
        has_user_keys = any(not key.startswith('__') for key in config.keys())
        if has_user_keys:
            config['transformed_field'] = 'transformed_value'
            if self.parameters.get('additional_field'):
                config['additional_field'] = self.parameters['additional_field']


def test_given_valid_config_when_transform_then_config_is_transformed():
    """Test that a custom config transformation properly transforms the config."""
    transformation = MockCustomConfigTransformation()
    config = {"original_field": "original_value"}

    transformation.transform(config)

    assert config["original_field"] == "original_value"
    assert config["transformed_field"] == "transformed_value"


def test_given_config_with_parameters_when_transform_then_parameters_are_applied():
    """Test that custom config transformation respects parameters."""
    parameters = {"additional_field": "parameter_value"}
    transformation = MockCustomConfigTransformation(parameters=parameters)
    config = {"original_field": "original_value"}

    transformation.transform(config)

    assert config["original_field"] == "original_value"
    assert config["transformed_field"] == "transformed_value"
    assert config["additional_field"] == "parameter_value"
