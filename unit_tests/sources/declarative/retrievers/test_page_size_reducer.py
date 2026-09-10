#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.retrievers.page_size_reducer import (
    PageSizeReducer,
    PageSizeReduction,
    PageSizeResetPolicy,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

A_STREAM_NAME = "stream_name"


def _reducer(configured_page_size=100, **kwargs):
    return PageSizeReducer(
        PageSizeReduction(**kwargs), configured_page_size, stream_name=A_STREAM_NAME
    )


def test_given_no_reduction_when_page_size_override_then_return_none():
    assert _reducer().page_size_override is None


def test_when_reduce_then_halve_page_size():
    reducer = _reducer()

    reducer.reduce()
    assert reducer.page_size_override == 50

    reducer.reduce()
    assert reducer.page_size_override == 25

    reducer.reduce()
    assert reducer.page_size_override == 12


def test_given_reduction_factor_when_reduce_then_use_factor():
    reducer = _reducer(reduction_factor=4)

    reducer.reduce()

    assert reducer.page_size_override == 25


def test_given_minimum_page_size_when_reduce_then_clamp_to_minimum():
    reducer = _reducer(configured_page_size=30, minimum_page_size=20)

    reducer.reduce()

    assert reducer.page_size_override == 20


def test_given_page_size_cannot_be_reduced_when_reduce_then_raise_transient_error():
    reducer = _reducer(configured_page_size=1)

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.transient_error


def test_given_already_at_minimum_when_reduce_then_raise_transient_error():
    reducer = _reducer(configured_page_size=4, minimum_page_size=2)
    reducer.reduce()
    assert reducer.page_size_override == 2

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.transient_error


def test_given_more_reductions_than_max_attempts_when_reduce_then_raise_transient_error():
    reducer = _reducer(configured_page_size=1000, max_attempts=2)
    reducer.reduce()
    reducer.reduce()

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.transient_error
    assert reducer.page_size_override == 250


def test_given_paginator_has_no_page_size_when_reduce_then_raise_config_error():
    reducer = _reducer(configured_page_size=None)

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.config_error


def test_given_reset_policy_never_when_on_successful_page_then_keep_reduced_page_size():
    reducer = _reducer()
    reducer.reduce()

    reducer.on_successful_page()

    assert reducer.page_size_override == 50


def test_given_reset_policy_after_successful_page_when_on_successful_page_then_restore_page_size():
    reducer = _reducer(reset_policy=PageSizeResetPolicy.AFTER_SUCCESSFUL_PAGE)
    reducer.reduce()

    reducer.on_successful_page()

    assert reducer.page_size_override is None


def test_given_reset_policy_after_successful_page_when_on_successful_page_then_attempts_are_not_reset():
    reducer = _reducer(max_attempts=2, reset_policy=PageSizeResetPolicy.AFTER_SUCCESSFUL_PAGE)
    reducer.reduce()
    reducer.on_successful_page()
    reducer.reduce()
    reducer.on_successful_page()

    with pytest.raises(AirbyteTracedException):
        reducer.reduce()


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"reduction_factor": 1}, id="reduction_factor_does_not_reduce"),
        pytest.param({"minimum_page_size": 0}, id="minimum_page_size_is_not_positive"),
        pytest.param({"max_attempts": 0}, id="max_attempts_is_not_positive"),
    ],
)
def test_given_invalid_configuration_then_raise_value_error(kwargs):
    with pytest.raises(ValueError):
        PageSizeReduction(**kwargs)


def test_given_non_integer_page_size_when_reduce_then_raise_config_error():
    """A custom pagination strategy can return anything from `get_page_size`; reducing is
    arithmetic, so a non-integer has to be reported rather than raising a bare TypeError."""
    reducer = PageSizeReducer(
        PageSizeReduction(),
        "{{ config['page_size'] }}",  # type: ignore[arg-type]
        stream_name="a_stream",
    )

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.config_error
    assert "not a whole number" in exception.value.message
