#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.transformations.keys_to_snake_transformation import (
    KeysToSnakeCaseTransformation,
)

_ANY_VALUE = -1


@pytest.mark.parametrize(
    "input_keys, expected_keys",
    [
        (
            {"FirstName": _ANY_VALUE, "lastName": _ANY_VALUE},
            {"first_name": _ANY_VALUE, "last_name": _ANY_VALUE},
        ),
        (
            {"123Number": _ANY_VALUE, "456Another123": _ANY_VALUE},
            {"_123_number": _ANY_VALUE, "_456_another_123": _ANY_VALUE},
        ),
        (
            {
                "NestedRecord": {"FirstName": _ANY_VALUE, "lastName": _ANY_VALUE},
                "456Another123": _ANY_VALUE,
            },
            {
                "nested_record": {"first_name": _ANY_VALUE, "last_name": _ANY_VALUE},
                "_456_another_123": _ANY_VALUE,
            },
        ),
        (
            {"hello@world": _ANY_VALUE, "test#case": _ANY_VALUE},
            {"hello_world": _ANY_VALUE, "test_case": _ANY_VALUE},
        ),
        (
            {"MixedUPCase123": _ANY_VALUE, "lowercaseAnd123": _ANY_VALUE},
            {"mixed_upcase_123": _ANY_VALUE, "lowercase_and_123": _ANY_VALUE},
        ),
        ({"Café": _ANY_VALUE, "Naïve": _ANY_VALUE}, {"cafe": _ANY_VALUE, "naive": _ANY_VALUE}),
        (
            {
                "This is a full sentence": _ANY_VALUE,
                "Another full sentence with more words": _ANY_VALUE,
            },
            {
                "this_is_a_full_sentence": _ANY_VALUE,
                "another_full_sentence_with_more_words": _ANY_VALUE,
            },
        ),
    ],
)
def test_keys_transformation(input_keys, expected_keys):
    KeysToSnakeCaseTransformation().transform(input_keys)
    assert input_keys == expected_keys
