#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.validators.validate_in_line_condition import (
    ValidateInLineCondition,
)


class TestValidateInLineCondition:
    def test_given_valid_condition_validates_true(self):
        config = {"test_key": "test_value"}
        validation_strategy = ValidateInLineCondition(config=config)

        validation_strategy.validate("{{ config['test_key'] == 'test_value' }}")

    def test_given_invalid_condition_raises_error(self):
        config = {"test_key": "test_value"}
        validation_strategy = ValidateInLineCondition(config=config)

        with pytest.raises(ValueError) as exc_info:
            validation_strategy.validate("{{ config['test_key'] == 'wrong_value' }}")

        assert "Condition evaluated to False" in str(exc_info.value)
        assert "evaluated to False" in str(exc_info.value)

    def test_given_non_string_condition_raises_error(self):
        config = {}
        validation_strategy = ValidateInLineCondition(config=config)

        for value in [None, 123, True, False, {"key": "value"}, ["item"]]:
            with pytest.raises(ValueError) as exc_info:
                validation_strategy.validate(value)

            assert f"Invalid condition argument: {value}. Should be a string." in str(exc_info.value)

    @pytest.mark.parametrize(
        "config, condition, expected_evaluation",
        [
            (
                {"parent": {"nested": {"value": "target"}}, "array": [1, 2, 3], "enabled": True},
                "{{ config['parent']['nested']['value'] == 'target' and config['enabled'] and 2 in config['array'] }}",
                True,
            ),
            (
                {"parent": {"nested": {"value": "target"}}, "array": [1, 2, 3], "enabled": True},
                "{{ config['parent']['nested']['value'] == 'target' and 5 in config['array'] }}",
                False,
            ),
        ],
    )
    def test_given_complex_config_condition_validates_correctly(
        self, config, condition, expected_evaluation
    ):
        """Test validation with complex config and condition"""
        validation_strategy = ValidateInLineCondition(config=config)
        if expected_evaluation:
            validation_strategy.validate(condition)
        else:
            with pytest.raises(ValueError) as exc_info:
                validation_strategy.validate(condition)

            assert "Condition evaluated to False" in str(exc_info.value)
            assert "evaluated to False" in str(exc_info.value)

    def test_given_empty_string_condition_raises_error(self):
        config = {}
        validation_strategy = ValidateInLineCondition(config=config)

        with pytest.raises(ValueError) as exc_info:
            validation_strategy.validate("")

        assert "Condition evaluated to False" in str(exc_info.value)

    def test_given_invalid_jinja_syntax_raises_error(self):
        config = {}
        validation_strategy = ValidateInLineCondition(config=config)

        with pytest.raises(ValueError) as exc_info:
            validation_strategy.validate("{{ this is invalid jinja }}")

        assert "Invalid jinja expression" in str(exc_info.value)

    def test_given_condition_with_missing_config_key_raises_error(self):
        config = {"existing_key": "value"}
        validation_strategy = ValidateInLineCondition(config=config)

        with pytest.raises(ValueError) as exc_info:
            validation_strategy.validate("{{ config['non_existent_key'] }}")

        assert "Condition evaluated to False" in str(exc_info.value)
