#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import threading

import requests

from airbyte_cdk.sources.streams.http.cache_stats import HttpCacheStats, HttpCacheStatsSnapshot


def _response(*, from_cache: bool | None = None) -> requests.Response:
    """A response with, or deliberately without, the `requests_cache` marker.

    `from_cache` absent is the case that matters most: it is what an uncached
    session produces, and counting it as a hit would report every connector that
    does no caching as caching perfectly.
    """
    response = requests.Response()
    response.status_code = 200
    if from_cache is not None:
        response.from_cache = from_cache  # type: ignore[attr-defined]
    return response


def test_a_fresh_counter_reports_nothing() -> None:
    assert HttpCacheStats().snapshot() == HttpCacheStatsSnapshot(requests=0, cache_hits=0)


def test_only_a_from_cache_response_counts_as_a_hit() -> None:
    stats = HttpCacheStats()

    stats.record_response(_response())
    stats.record_response(_response(from_cache=False))
    stats.record_response(_response(from_cache=True))

    assert stats.snapshot() == HttpCacheStatsSnapshot(requests=3, cache_hits=1)


def test_a_snapshot_does_not_move_under_the_reader() -> None:
    """The snapshot is a detached value, so a later request cannot backdate it."""
    stats = HttpCacheStats()
    stats.record_response(_response(from_cache=True))

    taken = stats.snapshot()
    stats.record_response(_response(from_cache=True))

    assert taken == HttpCacheStatsSnapshot(requests=1, cache_hits=1)


def test_concurrent_recording_loses_no_counts() -> None:
    """Concurrent sources read streams on a thread pool, so this is the real shape.

    `+= 1` on a plain attribute is not atomic under free-threaded CPython and is
    only accidentally so under the GIL, which is why the counters take a lock.
    """
    stats = HttpCacheStats()
    threads = [
        threading.Thread(
            target=lambda: [stats.record_response(_response(from_cache=True)) for _ in range(200)]
        )
        for _ in range(8)
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert stats.snapshot() == HttpCacheStatsSnapshot(requests=1600, cache_hits=1600)


def test_reset_zeroes_the_counters() -> None:
    stats = HttpCacheStats()
    stats.record_response(_response(from_cache=True))

    stats.reset()

    assert stats.snapshot() == HttpCacheStatsSnapshot(requests=0, cache_hits=0)
