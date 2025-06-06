# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from unittest import TestCase

import pytest

from airbyte_cdk.sources.declarative.validators.predicate_validator import PredicateValidator
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


class TestPredicateValidator(TestCase):
    def test_given_valid_input_validate_is_successful(self):
        strategy = MockValidationStrategy()
        test_value = "test@example.com"
        validator = PredicateValidator(value=test_value, strategy=strategy, config={}, condition="")

        validator.validate()

        assert strategy.validate_called
        assert strategy.validated_value == test_value

    def test_given_invalid_input_when_validate_then_raise_value_error(self):
        error_message = "Invalid email format"
        strategy = MockValidationStrategy(should_fail=True, error_message=error_message)
        test_value = "invalid-email"
        validator = PredicateValidator(value=test_value, strategy=strategy, config={}, condition="")

        with pytest.raises(ValueError) as context:
            validator.validate()

        assert error_message in str(context.value)
        assert strategy.validate_called
        assert strategy.validated_value == test_value

    def test_given_complex_object_when_validate_then_successful(self):
        strategy = MockValidationStrategy()
        test_value = {"user": {"email": "test@example.com", "name": "Test User"}}
        validator = PredicateValidator(value=test_value, strategy=strategy, config={}, condition="")

        validator.validate()

        assert strategy.validate_called
        assert strategy.validated_value == test_value

    def test_given_condition_is_false_when_validate_then_validate_is_not_called(self):
        strategy = MockValidationStrategy()
        validator = PredicateValidator(
            value="test",
            strategy=strategy,
            config={"test": "test"},
            condition="{{ not config.get('test') }}",
        )

        validator.validate()

        assert not strategy.validate_called
