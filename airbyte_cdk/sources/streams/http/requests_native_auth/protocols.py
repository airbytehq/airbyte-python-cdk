#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""Optional capabilities an authenticator can offer the HTTP client.

Authenticators are normally write-only from the client's point of view: they sign a request and
that is the end of the conversation. An authenticator managing a quota, or several
interchangeable credentials, needs more than that -- it has to learn what the server said, and
it knows things about credential availability that the retry logic cannot work out on its own.

These protocols are how it says so. Both are optional and dispatched structurally: `HttpClient`
looks for the method and skips authenticators that do not define it, so implementing one does
not require inheriting from anything or importing the client.
"""

from typing import Protocol, runtime_checkable

import requests


@runtime_checkable
class ResponseAwareAuthenticator(Protocol):
    """An authenticator that wants to see responses, not just sign requests.

    Authenticators tracking per-token quota need a feedback channel: without one they can only
    guess at the server's view of the quota and cannot tell that a token has been rejected.
    `HttpClient` calls `update_from_response` once per attempt on any authenticator that
    implements this, skipping replayed cache hits -- a cached response carries stale rate-limit
    headers and consumed no quota.

    Implementations must not raise for a response they cannot interpret, and must be safe to
    call from multiple threads.
    """

    def update_from_response(
        self, request: requests.PreparedRequest, response: requests.Response
    ) -> None:
        pass


@runtime_checkable
class TokenRotatingAuthenticator(Protocol):
    """An authenticator holding several interchangeable credentials.

    A rate-limit backoff is computed from the response alone, so it assumes the only way
    forward is for that quota to come back. An authenticator with a spare credential knows
    better. `HttpClient` asks before sleeping out a rate-limit window.

    Implementations must only answer True when retrying immediately would actually use a
    different credential -- otherwise the retry hammers the same rejected one.
    """

    def has_alternative_token(self, request: requests.PreparedRequest) -> bool:
        pass
