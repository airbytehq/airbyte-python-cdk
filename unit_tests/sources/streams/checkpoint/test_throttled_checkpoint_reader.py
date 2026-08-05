#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#
"""`ThrottledCheckpointReader` rate-limits how often a checkpoint surfaces.

Suppression rides on the documented `get_checkpoint()` contract — returning
`None` means the caller emits no state message — so nothing in `Stream.read()`
needs to know throttling exists.

The reader is driven directly here rather than through a stream read, which
keeps the clock injectable and the call sequence explicit.
"""

import itertools
from typing import Any, Iterable, List, Mapping, Optional

import pytest

from airbyte_cdk.sources.streams.checkpoint import (
    DEFAULT_STATE_EMISSION_THROTTLE_SECONDS,
    CheckpointReader,
    IncrementalCheckpointReader,
    ThrottledCheckpointReader,
)


class _RecordingReader(CheckpointReader):
    """Minimal inner reader: yields the given slices, echoes observed state."""

    def __init__(self, slices: Iterable[Optional[Mapping[str, Any]]]):
        self._slices = iter(slices)
        self._state: Optional[Mapping[str, Any]] = None
        self._exhausted = False

    def next(self) -> Optional[Mapping[str, Any]]:
        try:
            return next(self._slices)
        except StopIteration:
            # Mirrors IncrementalCheckpointReader: drop state at the end so the
            # caller does not emit a duplicate final checkpoint.
            self._exhausted = True
            self._state = None
            return None

    def observe(self, new_state: Mapping[str, Any]) -> None:
        self._state = new_state

    def get_checkpoint(self) -> Optional[Mapping[str, Any]]:
        return self._state


def _drive(reader: CheckpointReader, cursors: List[str]) -> List[Mapping[str, Any]]:
    """Run the call sequence `Stream.read()` uses, collecting emitted checkpoints.

    Per slice: observe() then get_checkpoint(); then next(). After the loop, one
    final get_checkpoint().
    """
    emitted = []
    next_slice = reader.next()
    i = 0
    while next_slice is not None:
        reader.observe({"cursor": cursors[i]})
        checkpoint = reader.get_checkpoint()
        if checkpoint is not None:
            emitted.append(checkpoint)
        i += 1
        next_slice = reader.next()

    final = reader.get_checkpoint()
    if final is not None:
        emitted.append(final)
    return emitted


CURSORS = ["a", "b", "c", "d"]
SLICES: List[Optional[Mapping[str, Any]]] = [{"s": 1}, {"s": 2}, {"s": 3}, {"s": 4}]


@pytest.mark.parametrize(
    "throttle_seconds, tick, expected_cursors, reason",
    [
        pytest.param(
            600.0,
            10,
            ["a", "d"],
            "cold start emits; slices 2-4 sit inside the window; final is forced",
            id="suppresses_and_forces_final",
        ),
        pytest.param(
            15.0,
            10,
            ["a", "c", "d"],
            "slice 3 at t=20 is >=15s after t=0, so it emits and restarts the window",
            id="re_emits_once_window_elapses",
        ),
        pytest.param(
            10.0,
            10,
            ["a", "b", "c", "d"],
            "every slice lands exactly on the boundary (delta == throttle, not <)",
            id="boundary_emits_without_duplicate_final",
        ),
        pytest.param(
            0.0,
            10,
            ["a", "b", "c", "d"],
            "a non-positive window never suppresses, so behaviour matches unthrottled",
            id="zero_fails_open",
        ),
    ],
)
def test_throttle_emission_sequence(throttle_seconds, tick, expected_cursors, reason) -> None:
    clock = itertools.count(0, tick)
    reader = ThrottledCheckpointReader(
        _RecordingReader(SLICES),
        throttle_seconds=throttle_seconds,
        clock=lambda: next(clock),
    )

    emitted = _drive(reader, CURSORS)

    # Assert the whole sequence, not just the count: a count-only check also
    # passes for an implementation that inverted its comparison.
    assert [c["cursor"] for c in emitted] == expected_cursors, reason


def test_final_checkpoint_is_not_duplicated_when_nothing_was_suppressed() -> None:
    """The last slice emitted, so `_pending` is empty and the final call must not
    manufacture an extra state message."""
    clock = itertools.count(0, 10)
    reader = ThrottledCheckpointReader(
        _RecordingReader(SLICES), throttle_seconds=10.0, clock=lambda: next(clock)
    )
    _drive(reader, CURSORS)
    assert reader.get_checkpoint() is None


def test_inner_final_checkpoint_takes_precedence() -> None:
    """If the wrapped reader wants to emit its own final checkpoint, that wins
    over any held value — the throttle must not mask it."""

    class _FinalEmittingReader(_RecordingReader):
        def get_checkpoint(self) -> Optional[Mapping[str, Any]]:
            if self._exhausted:
                return {"cursor": "inner-final"}
            return self._state

    clock = itertools.count(0, 10)
    reader = ThrottledCheckpointReader(
        _FinalEmittingReader(SLICES), throttle_seconds=600.0, clock=lambda: next(clock)
    )
    emitted = _drive(reader, CURSORS)
    assert [c["cursor"] for c in emitted] == ["a", "inner-final"]


def test_delegates_iteration_and_observation_to_the_inner_reader() -> None:
    inner = IncrementalCheckpointReader(stream_state={}, stream_slices=SLICES)
    reader = ThrottledCheckpointReader(inner, throttle_seconds=600.0)

    assert reader.next() == {"s": 1}
    reader.observe({"cursor": "a"})
    assert inner.get_checkpoint() == {"cursor": "a"}


def test_default_throttle_is_the_shared_constant() -> None:
    reader = ThrottledCheckpointReader(_RecordingReader(SLICES))
    assert reader._throttle_seconds == DEFAULT_STATE_EMISSION_THROTTLE_SECONDS
