# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the secret masking functionality in the Airbyte CDK CLI."""

from unittest.mock import patch

import pytest

from airbyte_cdk.cli.airbyte_cdk import _secrets


@pytest.mark.parametrize(
    "config,expected_calls",
    [
        # Test masking in a flat dict
        ({"password": "secret123", "regular": "value"}, ["secret123"]),
        # Test masking in a nested dict
        ({"outer": {"api_key": "keyval"}}, ["keyval"]),
        # Test masking in a list of dicts
        ([{"token": "tok1"}, {"name": "v"}], ["tok1"]),
        # Test masking in a dict with a list value
        ({"passwords": ["a", "b"]}, ["a", "b"]),
        # Test masking of multi-line secrets
        ({"password": "multi\nline\nsecret"}, ["multi", "line", "secret"]),
        # Test masking in a deeply nested structure
        ({"a": [{"b": {"secret": "deep"}}]}, ["deep"]),
        # Test masking with no secrets
        ({"foo": "bar"}, []),
        # Additional edge case: mixed types
        ({"password": ["a", 123, {"nested": "val"}]}, ["a", "123", "val"]),
        ([{"password": "foo"}], ["foo"]),
    ],
)
def test_print_ci_secrets_masks_for_config(
    config: dict,
    expected_calls: list,
) -> None:
    with patch(
        "airbyte_cdk.cli.airbyte_cdk._secrets._print_ci_secret_mask_for_string",
    ) as mask_mock:
        _secrets._print_ci_secrets_masks_for_config(config)
        actual_calls = [str(call.args[0]) for call in mask_mock.call_args_list]
        assert sorted(actual_calls) == sorted(expected_calls)
