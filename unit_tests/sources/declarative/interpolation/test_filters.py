#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import base64
import hashlib
import hmac as hmac_lib

import pytest

from airbyte_cdk.sources.declarative.interpolation.jinja import JinjaInterpolation

interpolation = JinjaInterpolation()


def test_hash_md5_no_salt() -> None:
    input_string = "abcd"
    s = "{{ '%s' | hash('md5') }}" % input_string
    filter_hash = interpolation.eval(s, config={})

    # compute expected hash calling hashlib directly
    hash_obj = hashlib.md5()
    hash_obj.update(str(input_string).encode("utf-8"))
    hashlib_computed_hash = hash_obj.hexdigest()

    assert filter_hash == hashlib_computed_hash


def test_hash_md5_on_numeric_value() -> None:
    input_value = 123.456
    s = "{{ %f | hash('md5') }}" % input_value
    filter_hash = interpolation.eval(s, config={})

    # compute expected hash calling hashlib directly
    hash_obj = hashlib.md5()
    hash_obj.update(str(input_value).encode("utf-8"))
    hashlib_computed_hash = hash_obj.hexdigest()

    assert filter_hash == hashlib_computed_hash


def test_hash_md5_with_salt() -> None:
    input_string = "test_input_string"
    input_salt = "test_input_salt"

    s = "{{ '%s' | hash('md5', '%s' ) }}" % (input_string, input_salt)
    filter_hash = interpolation.eval(s, config={})

    # compute expected value calling hashlib directly
    hash_obj = hashlib.md5()
    hash_obj.update(str(input_string + input_salt).encode("utf-8"))
    hashlib_computed_hash = hash_obj.hexdigest()

    assert filter_hash == hashlib_computed_hash


@pytest.mark.parametrize(
    "input_string",
    ["test_input_client_id", "some_client_secret_1", "12345", "775.78"],
)
def test_base64encode(input_string: str) -> None:
    s = "{{ '%s' | base64encode }}" % input_string
    filter_base64encode = interpolation.eval(s, config={})

    # compute expected base64encode calling base64 library directly
    base64_obj = base64.b64encode(input_string.encode("utf-8")).decode()

    assert filter_base64encode == base64_obj


@pytest.mark.parametrize(
    "input_string, expected_string",
    [
        ("aW5wdXRfc3RyaW5n", "input_string"),
        ("YWlyYnl0ZQ==", "airbyte"),
        ("cGFzc3dvcmQ=", "password"),
    ],
)
def test_base64decode(input_string: str, expected_string: str) -> None:
    s = "{{ '%s' | base64decode }}" % input_string
    filter_base64decode = interpolation.eval(s, config={})

    assert filter_base64decode == expected_string


def test_regex_search_valid() -> None:
    expression_with_regex = "{{ '<https://this-is-test-link.com/?page=2>; rel=\"next\"' | regex_search('<(.*)>; rel=.*') }}"

    val = interpolation.eval(expression_with_regex, {})
    assert val == "https://this-is-test-link.com/?page=2"


def test_regex_search_no_match_group() -> None:
    # If no group is set in the regular expression, the result will be an empty string
    expression_with_regex = "{{ '<https://this-is-test-link.com/?page=2>; rel=\"next\"' | regex_search('<.*>; rel=.*') }}"

    val = interpolation.eval(expression_with_regex, {})
    assert val is None


def test_regex_search_no_match() -> None:
    # If no group is set in the regular expression, the result will be an empty string
    expression_with_regex = (
        "{{ '<https://this-is-test-link.com/?page=2>; rel=\"next\"' | regex_search('WATWATWAT') }}"
    )

    val = interpolation.eval(expression_with_regex, {})

    assert val is None


@pytest.mark.parametrize(
    "expression, expected",
    [
        pytest.param(
            "{{ 'hello world' | regex_replace('world', 'there') }}",
            "hello there",
            id="basic_replacement",
        ),
        pytest.param(
            "{{ 'abc123def456' | regex_replace('[0-9]+', '') }}",
            "abcdef",
            id="regex_pattern_strip_digits",
        ),
        pytest.param(
            "{{ 'hello world' | regex_replace('xyz', 'replaced') }}",
            "hello world",
            id="no_match_returns_original",
        ),
        pytest.param(
            "{{ 'aaa bbb aaa' | regex_replace('aaa', 'ccc') }}",
            "ccc bbb ccc",
            id="multiple_occurrences",
        ),
    ],
)
def test_regex_replace(expression: str, expected: str) -> None:
    val = interpolation.eval(expression, {})
    assert val == expected


def test_hmac_sha256_default() -> None:
    message = "test_message"
    secret_key = "test_secret_key"

    s = "{{ '%s' | hmac('%s') }}" % (message, secret_key)
    filter_hmac = interpolation.eval(s, config={})

    # compute expected hmac using the hmac library directly
    hmac_obj = hmac_lib.new(
        key=secret_key.encode("utf-8"), msg=message.encode("utf-8"), digestmod=hashlib.sha256
    )
    expected_hmac = hmac_obj.hexdigest()

    assert filter_hmac == expected_hmac


def test_hmac_sha256_explicit() -> None:
    message = "test_message"
    secret_key = "test_secret_key"

    s = "{{ '%s' | hmac('%s', 'sha256') }}" % (message, secret_key)
    filter_hmac = interpolation.eval(s, config={})

    # compute expected hmac using the hmac library directly
    hmac_obj = hmac_lib.new(
        key=secret_key.encode("utf-8"), msg=message.encode("utf-8"), digestmod=hashlib.sha256
    )
    expected_hmac = hmac_obj.hexdigest()

    assert filter_hmac == expected_hmac


def test_hmac_with_numeric_value() -> None:
    message = 12345
    secret_key = "test_secret_key"

    s = "{{ %d | hmac('%s') }}" % (message, secret_key)
    filter_hmac = interpolation.eval(s, config={})

    # compute expected hmac using the hmac library directly
    hmac_obj = hmac_lib.new(
        key=secret_key.encode("utf-8"), msg=str(message).encode("utf-8"), digestmod=hashlib.sha256
    )
    expected_hmac = hmac_obj.hexdigest()

    assert filter_hmac == expected_hmac


def test_hmac_with_invalid_hash_type() -> None:
    message = "test_message"
    secret_key = "test_secret_key"

    s = "{{ '%s' | hmac('%s', 'md5') }}" % (message, secret_key)

    with pytest.raises(ValueError):
        interpolation.eval(s, config={})
