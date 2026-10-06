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
        validator = PredicateValidator(value=test_value, strategy=strategy)

        validator.validate()

        assert strategy.validate_called
        assert strategy.validated_value == test_value

    def test_given_invalid_input_when_validate_then_raise_value_error(self):
        error_message = "Invalid email format"
        strategy = MockValidationStrategy(should_fail=True, error_message=error_message)
        test_value = "invalid-email"
        validator = PredicateValidator(value=test_value, strategy=strategy)

        with pytest.raises(ValueError) as context:
            validator.validate()

        assert error_message in str(context.value)
        assert strategy.validate_called
        assert strategy.validated_value == test_value

    def test_given_complex_object_when_validate_then_successful(self):
        strategy = MockValidationStrategy()
        test_value = {"user": {"email": "test@example.com", "name": "Test User"}}
        validator = PredicateValidator(value=test_value, strategy=strategy)

        validator.validate()

        assert strategy.validate_called
        assert strategy.validated_value == test_value

    def test_given_interpolated_value_when_validate_then_value_is_evaluated_against_config(self):
        strategy = MockValidationStrategy()
        validator = PredicateValidator(value="{{ config['domain'] }}", strategy=strategy)

        validator.validate({"domain": "example.atlassian.net"})

        assert strategy.validated_value == "example.atlassian.net"

    def test_given_interpolated_list_expression_when_validate_then_strategy_receives_list(self):
        strategy = MockValidationStrategy()
        validator = PredicateValidator(
            value="{{ config['report_options_list'] | map(attribute='stream_name') | list }}",
            strategy=strategy,
        )

        validator.validate({"report_options_list": [{"stream_name": "a"}, {"stream_name": "b"}]})

        assert strategy.validated_value == ["a", "b"]

    def test_given_literal_non_string_values_when_validate_then_values_are_preserved(self):
        for literal in [123, 1.5, True, None, ["a", 1], {"key": [1, 2]}]:
            strategy = MockValidationStrategy()
            PredicateValidator(value=literal, strategy=strategy).validate({})
            assert strategy.validated_value == literal

    def test_given_nested_interpolated_values_when_validate_then_nested_strings_are_evaluated(self):
        strategy = MockValidationStrategy()
        validator = PredicateValidator(
            value={"name": "{{ config['name'] }}", "ids": ["{{ config['id'] }}", 2]},
            strategy=strategy,
        )

        validator.validate({"name": "test", "id": 1})

        assert strategy.validated_value == {"name": "test", "ids": [1, 2]}
