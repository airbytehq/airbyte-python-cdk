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


def _reducer(configured_page_size=100, sleeps=None, **kwargs):
    return PageSizeReducer(
        PageSizeReduction(**kwargs),
        configured_page_size,
        stream_name=A_STREAM_NAME,
        # The reducer waits before each retry; tests record the waits instead of taking them.
        sleep=sleeps.append if sleeps is not None else lambda _: None,
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


def test_given_minimum_page_size_above_configured_page_size_when_reduce_then_raise_config_error():
    """
    No reduction can ever be applied here, so nothing about the response can fix it and the platform must not
    retry the whole job for it.
    """
    reducer = _reducer(configured_page_size=50, minimum_page_size=100)

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.config_error
    assert "already at or below the configured minimum" in exception.value.message


def test_given_page_size_cannot_be_reduced_when_reduce_then_raise_config_error():
    reducer = _reducer(configured_page_size=1)

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.config_error


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


def test_given_reset_policy_after_successful_page_when_on_successful_page_then_attempts_are_reset():
    """
    This policy exists for an API that rejects the configured page size on every page, so every page costs one
    reduction. A budget spanning the whole partition would fail the sync at page `max_attempts + 1` however
    healthy the reads are, which is the one workload the policy is for.
    """
    reducer = _reducer(max_attempts=2, reset_policy=PageSizeResetPolicy.AFTER_SUCCESSFUL_PAGE)

    for _ in range(10):
        reducer.reduce()
        assert reducer.page_size_override == 50
        reducer.on_successful_page()
        assert reducer.page_size_override is None


def test_given_reset_policy_after_successful_page_when_no_page_succeeds_then_max_attempts_still_applies():
    """The budget restarts on a successful page, not on a reduction, so an endpoint that fails whatever we ask
    for still terminates. This is the genuinely-stuck case: nothing got through, so the read has to end."""
    reducer = _reducer(max_attempts=2, reset_policy=PageSizeResetPolicy.AFTER_SUCCESSFUL_PAGE)
    reducer.reduce()
    reducer.reduce()

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.transient_error
    # the size named is the one that was just requested and failed, not the configured one
    assert "down to 25 records per page" in exception.value.message
    assert "2 times in a row without a single page succeeding" in exception.value.internal_message


def test_given_reset_policy_never_when_pages_succeed_then_attempts_are_reset():
    """
    `max_attempts` counts the reductions made in a row without a page getting through, which is what the
    terminal error claims happened, so a page that succeeded has to restart it under this policy too. A stream
    whose per-page cost varies - the GraphQL case this feature exists for - would otherwise fail at the
    `max_attempts + 1`-th heavy page of a long partition in which every reduction was followed by a page.
    """
    reducer = _reducer(configured_page_size=1000, max_attempts=2)

    for expected_page_size in [500, 250, 125, 62, 31, 15, 7, 3, 1]:
        reducer.reduce()
        assert reducer.page_size_override == expected_page_size
        # the page size is not restored under NEVER, only the budget is
        reducer.on_successful_page()
        assert reducer.page_size_override == expected_page_size


def test_given_reset_policy_never_when_pages_succeed_then_minimum_page_size_still_ends_the_read():
    """
    With the budget restarting, `minimum_page_size` is what bounds a NEVER partition: the page size strictly
    decreases, so the read cannot go on forever.
    """
    reducer = _reducer(configured_page_size=100, minimum_page_size=10, max_attempts=2)

    for expected_page_size in [50, 25, 12, 10]:
        reducer.reduce()
        assert reducer.page_size_override == expected_page_size
        reducer.on_successful_page()

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.transient_error
    assert "smallest page size" in exception.value.message


def test_given_no_page_succeeds_then_attempts_are_not_reset():
    reducer = _reducer(max_attempts=2)
    reducer.reduce()
    reducer.reduce()

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert "2 times in a row without a single page succeeding" in exception.value.internal_message


def test_given_reset_policy_after_successful_page_when_every_page_succeeds_then_never_fail():
    """
    A partition where every page gets through after one reduction is healthy, however long it is: this policy
    exists for an API that rejects the configured page size on every page. There is no partition-wide cap on
    the number of reductions, because any such cap would fail this stream at the page it happens to sit on.
    """
    reducer = _reducer(
        configured_page_size=1000,
        max_attempts=2,
        reset_policy=PageSizeResetPolicy.AFTER_SUCCESSFUL_PAGE,
    )

    for _ in range(5_000):
        reducer.reduce()
        assert reducer.page_size_override == 500
        reducer.on_successful_page()
        assert reducer.page_size_override is None


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


def test_when_reduce_then_wait_before_the_retry():
    """
    `PageSizeReductionRequiredException` bypasses the HTTP retry budget on purpose, so this wait is the only
    thing keeping an endpoint that fails at every page size from being hit in a burst.
    """
    sleeps: list = []
    reducer = _reducer(configured_page_size=1000, sleeps=sleeps)

    reducer.reduce()
    reducer.reduce()

    assert sleeps == [
        PageSizeReducer.BACKOFF_SECONDS,
        PageSizeReducer.BACKOFF_SECONDS * 2,
    ]
    assert all(wait > 0 for wait in sleeps)


@pytest.mark.parametrize(
    "reducer_kwargs,reductions",
    [
        pytest.param({"configured_page_size": 4, "minimum_page_size": 2}, 1, id="minimum_reached"),
        pytest.param(
            {"configured_page_size": 1000, "max_attempts": 2}, 2, id="max_attempts_exhausted"
        ),
    ],
)
def test_given_reduction_fails_then_message_names_the_stream_and_leaves_out_remediation(
    reducer_kwargs, reductions
):
    # The user cannot act on a `transient_error`, so the message states the failure alone. Naming the stream
    # is what makes it actionable for whoever reads the sync, since a sync reduces per stream.
    reducer = _reducer(**reducer_kwargs)
    for _ in range(reductions):
        reducer.reduce()

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.transient_error
    assert A_STREAM_NAME in exception.value.message
    assert "contact the API provider" not in exception.value.message
    assert "Try syncing fewer streams" not in exception.value.message


@pytest.mark.parametrize(
    "reducer_kwargs",
    [
        pytest.param({"configured_page_size": 1, "minimum_page_size": 1}, id="never_reducible"),
        pytest.param({"configured_page_size": None}, id="no_page_size_at_all"),
        pytest.param({"configured_page_size": "100"}, id="page_size_is_not_a_number"),
    ],
)
def test_given_misconfiguration_then_message_names_the_stream_and_keeps_remediation(reducer_kwargs):
    # A `config_error` is the user's to fix, so the remediation stays in the message.
    reducer = _reducer(**reducer_kwargs)

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.config_error
    assert A_STREAM_NAME in exception.value.message
    assert exception.value.message.rstrip().endswith(".")


@pytest.mark.parametrize(
    "reducer_kwargs,reductions",
    [
        pytest.param({"configured_page_size": 4, "minimum_page_size": 2}, 1, id="minimum_reached"),
        pytest.param(
            {"configured_page_size": 1000, "max_attempts": 2}, 2, id="max_attempts_exhausted"
        ),
    ],
)
def test_given_failure_message_when_reduction_fails_then_append_it(reducer_kwargs, reductions):
    # The CDK only knows that the API rejected every page size it asked for; what narrows a query down is
    # API-specific, so the connector supplies that sentence.
    reducer = _reducer(failure_message="Select fewer fields on this stream.", **reducer_kwargs)
    for _ in range(reductions):
        reducer.reduce()

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.transient_error
    assert exception.value.message.endswith(" Select fewer fields on this stream.")


def test_given_no_failure_message_when_reduction_fails_then_message_ends_with_the_cdk_sentence():
    reducer = _reducer(configured_page_size=4, minimum_page_size=2)
    reducer.reduce()

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.message.endswith("records per page).")


def test_given_failure_message_when_misconfigured_then_do_not_append_it():
    # A `config_error` is about the manifest, not about the API rejecting a page size, so the connector's
    # sentence about narrowing the query down would be misleading there.
    reducer = _reducer(
        configured_page_size=None, failure_message="Select fewer fields on this stream."
    )

    with pytest.raises(AirbyteTracedException) as exception:
        reducer.reduce()

    assert exception.value.failure_type == FailureType.config_error
    assert "Select fewer fields" not in exception.value.message
