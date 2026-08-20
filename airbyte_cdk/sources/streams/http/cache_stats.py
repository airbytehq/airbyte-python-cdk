#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

"""Process-wide counters for HTTP requests and `requests_cache` hits.

A `requests_cache` hit is served inside `Session.send()` and never reaches the
wire, so nothing outside the connector process can observe it -- not a proxy, not
the platform. The connector therefore has to count its own hits and report them,
which is what these counters exist for; `AirbyteEntrypoint.run` turns the final
snapshot into analytics messages at the end of every command.

The counters are module-level because the thing being measured is, too: one
`requests_cache` backend is shared by every stream of a run, and the question the
numbers answer ("does this connector's caching work, and did this version
regress it") is about the run rather than about any one stream or client. Being
process-wide, they are cumulative: a reader wanting one run's numbers takes a
snapshot when it starts and subtracts, which is what `AirbyteEntrypoint.run` does.

Scope: every `HttpClient` in the process records here, but only
`AirbyteEntrypoint.run` reports. Destinations (`Destination.run_cmd`), the
manifest server, and the Connector Builder drive their commands without going
through it, so they accumulate counts nothing reads. "No counts reported" means
"a source connector run through the entrypoint, or nothing".

`http-request-count` includes cache hits, so it is responses handled rather than
wire flows; subtract `http-cache-hit-count` to get the number a proxy would see.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

import requests


@dataclass(frozen=True)
class HttpCacheStatsSnapshot:
    """The counters at one instant, detached from the lock that guards them."""

    requests: int
    cache_hits: int


class HttpCacheStats:
    """Requests made and requests served from the connector's own cache.

    Guarded by a lock because concurrent sources read streams on a thread pool,
    so `record_response` is called from several threads at once and `+= 1` on a
    plain attribute would drop counts.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests = 0
        self._cache_hits = 0

    def record_response(self, response: requests.Response) -> None:
        """Count one request, and one cache hit when the response was cached.

        `from_cache` is set by `requests_cache.CacheMixin`, so it is present
        exactly when caching is in play and absent -- counted as a live request
        -- when it is not.
        """
        from_cache = bool(getattr(response, "from_cache", False))
        with self._lock:
            self._requests += 1
            if from_cache:
                self._cache_hits += 1

    def snapshot(self) -> HttpCacheStatsSnapshot:
        with self._lock:
            return HttpCacheStatsSnapshot(requests=self._requests, cache_hits=self._cache_hits)

    def reset(self) -> None:
        """Zero the counters. For tests, which share one process across cases."""
        with self._lock:
            self._requests = 0
            self._cache_hits = 0


HTTP_CACHE_STATS = HttpCacheStats()
