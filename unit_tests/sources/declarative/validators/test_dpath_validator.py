# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from unittest import TestCase

import pytest

from airbyte_cdk.sources.declarative.validators.dpath_validator import DpathValidator
from airbyte_cdk.sources.declarative.validators.validation_strategy import ValidationStrategy


class MockValidationStrategy(ValidationStrategy):
    def __init__(self, should_fail=False, error_message="Validation failed"):
        self.should_fail = should_fail
        self.error_message = error_message
        self.validate_called = False
        self.validated_value = None

    def validate(self, value):
        self.validate_called = True
        self.validated_value = value
        if self.should_fail:
            raise ValueError(self.error_message)


class TestDpathValidator(TestCase):
    def test_given_valid_path_and_input_validate_is_successful(self):
        strategy = MockValidationStrategy()
        validator = DpathValidator(field_path=["user", "profile", "email"], strategy=strategy)

        test_data = {"user": {"profile": {"email": "test@example.com", "name": "Test User"}}}

        validator.validate(test_data)

        assert strategy.validate_called
        assert strategy.validated_value


class TestDpathValidator(TestCase):
    def test_given_valid_top_level_path_and_input_validate_is_successful(self):
        strategy = MockValidationStrategy()
        validator = DpathValidator(field_path=["user"], strategy=strategy)

        test_data = {"user": {"profile": {"email": "test@example.com", "name": "Test User"}}}

        validator.validate(test_data)

        assert strategy.validate_called
        assert strategy.validated_value

    def test_given_strategy_fails_when_validate_then_raise_value_error(self):
        error_message = "Invalid email format"
        strategy = MockValidationStrategy(should_fail=True, error_message=error_message)
        validator = DpathValidator(field_path=["user", "email"], strategy=strategy)

        test_data = {"user": {"email": "invalid-email"}}

        with pytest.raises(ValueError):
            validator.validate(test_data)

        assert strategy.validate_called
        assert strategy.validated_value == "invalid-email"

    def test_given_empty_path_list_when_validate_then_validate_raises_exception(self):
        strategy = MockValidationStrategy()
        validator = DpathValidator(field_path=[], strategy=strategy)
        test_data = {"key": "value"}

        with pytest.raises(ValueError):
            validator.validate(test_data)

    def test_path_with_wildcard_when_validate_then_validate_is_successful(self):
        strategy = MockValidationStrategy()
        validator = DpathValidator(field_path=["users", "*", "email"], strategy=strategy)

        test_data = {
            "users": {
                "user1": {"email": "user1@example.com", "name": "User One"},
                "user2": {"email": "user2@example.com", "name": "User Two"},
            }
        }

        validator.validate(test_data)

        assert strategy.validate_called
        assert strategy.validated_value in ["user1@example.com", "user2@example.com"]
        self.assertIn(strategy.validated_value, ["user1@example.com", "user2@example.com"])

    def test_given_no_values_when_validate_then_validate_is_not_called(self):
        strategy = MockValidationStrategy()
        validator = DpathValidator(field_path=["users", "*", "email"], strategy=strategy)

        validator.validate({"users": {}})
        validator.validate(None)
        validator.validate({"users": [{"name": "Octavia"}]})

        assert not strategy.validate_called
