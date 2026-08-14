#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import cycle
from typing import Any, List, Mapping, Optional

import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.streams.call_rate import RequestMatcher
from airbyte_cdk.sources.streams.http import HttpClient
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_cdk.utils.datetime_helpers import AirbyteDateTime, ab_datetime_now, ab_datetime_parse


@dataclass
class TokenQuota:
    """A named per-token quota pool.

    `matchers` classify outgoing requests into the pool; a pool with no matchers acts as the
    default pool. `remaining_path`/`reset_path`/`limit_path` locate the pool's values in the
    quota status response.

    `remaining_header`/`reset_header`/`limit_header`/`exhaustion_status_codes` are optional and
    enable reconciling the pool against what the server reports on each response. Without them
    the pool is only ever seeded from `quota_status_url`, so its counters drift whenever the
    token is shared with another client, requests are in flight concurrently, or the sync runs
    long enough for a single seeding to go stale.
    """

    name: str
    remaining_path: List[str]
    reset_path: List[str]
    limit_path: Optional[List[str]] = None
    matchers: List[RequestMatcher] = field(default_factory=list)
    remaining_header: Optional[str] = None
    reset_header: Optional[str] = None
    limit_header: Optional[str] = None
    exhaustion_status_codes: List[int] = field(default_factory=list)

    @property
    def is_response_aware(self) -> bool:
        return bool(
            self.remaining_header
            or self.reset_header
            or self.limit_header
            or self.exhaustion_status_codes
        )


@dataclass
class _QuotaState:
    remaining: int
    reset_at: AirbyteDateTime
    limit: int
    # True when the quota status endpoint reported this pool spent with a reset that had
    # *already* passed -- a self-contradictory answer (server clock skew, or a lagging quota
    # endpoint). Such a pool must not be restored from its elapsed window, or the connector
    # would invent quota the server has just denied and spin on it.
    stale_zero: bool = False


class RateLimitedMultipleTokenAuthenticator(DeclarativeAuthenticator):
    """Authenticator that rotates between multiple interchangeable tokens with per-token quota tracking.

    Each outgoing request is classified into a quota pool using the pool's request matchers.
    The active token's counter for the matched pool is decremented locally; when it is exhausted
    the authenticator rotates to the next token. When all tokens are exhausted for a pool, it
    waits until the earliest quota reset (bounded by `max_wait_time`) and then refreshes all
    counters from `quota_status_url`, or raises a transient error if the wait would be too long.

    A proactive throttling budget spreads the last calls over the time remaining until reset:
    once every token's remaining count for a pool drops below its reserve
    (`max(budget_min_reserve, budget_reserve_fraction * limit)`), a small delay proportional to
    `seconds_until_reset / total_remaining` (capped at 10s) is injected before each request.

    Counters are seeded per token from `quota_status_url` on first use and refreshed after an
    exhaustion wait. When a pool declares response headers, `update_from_response` additionally
    reconciles that pool against the server on every response, which keeps the counters honest
    between seedings and makes the authenticator rotate off a token the server has rejected even
    though the local count still looks healthy. All state transitions are guarded by a lock so
    the authenticator can be shared safely across concurrent streams; sleeps never hold the lock.
    """

    HEARTBEAT_INTERVAL = 60.0  # Log every 60s during exhaustion wait
    MAX_BUDGET_DELAY = 10.0  # Cap for the per-request proactive throttling delay
    MIN_EXHAUSTION_WAIT = 5.0  # Floor for the exhaustion wait, so stale reset timestamps can't cause a refresh busy-loop

    def __init__(
        self,
        tokens: List[str],
        quotas: List[TokenQuota],
        quota_status_url: str,
        quota_status_http_method: str = "GET",
        quota_status_headers: Optional[Mapping[str, str]] = None,
        auth_method: str = "Bearer",
        header: str = "Authorization",
        max_wait_time: timedelta = timedelta(hours=2),
        budget_reserve_fraction: float = 0.1,
        budget_min_reserve: int = 50,
    ) -> None:
        if not tokens:
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                internal_message="RateLimitedMultipleTokenAuthenticator requires at least one token",
                message="Authentication tokens are missing from the configuration.",
            )
        if not quotas:
            raise AirbyteTracedException(
                failure_type=FailureType.config_error,
                internal_message="RateLimitedMultipleTokenAuthenticator requires at least one quota pool",
                message="Quota pool configuration is missing.",
            )
        self._logger = logging.getLogger("airbyte")
        self._tokens = list(tokens)
        self._quotas = quotas
        self._quota_status_url = quota_status_url
        self._quota_status_http_method = quota_status_http_method
        self._quota_status_headers = dict(quota_status_headers or {})
        self._auth_method = auth_method
        self._header = header
        self._max_wait_time = max_wait_time
        self._budget_reserve_fraction = budget_reserve_fraction
        self._budget_min_reserve = budget_min_reserve

        self._lock = threading.RLock()
        self._refresh_lock = threading.Lock()
        self._initialized = False
        self._budget_logged = False
        self._unmatched_logged = False
        self._states: dict[str, dict[str, _QuotaState]] = {}
        self._token_to_http_client: Mapping[str, HttpClient] = {
            token: HttpClient(
                name="quota_status",
                logger=self._logger,
                authenticator=TokenAuthenticator(
                    token, auth_method=self._auth_method, auth_header=self._header
                ),
                use_cache=False,  # quota values change frequently; never reuse cached responses
            )
            for token in self._tokens
        }
        self._tokens_iter = cycle(self._tokens)
        self._active_token = next(self._tokens_iter)

    @property
    def auth_header(self) -> str:
        return self._header

    @property
    def token(self) -> str:
        with self._lock:
            return f"{self._auth_method} {self._active_token}".strip()

    def __call__(self, request: requests.PreparedRequest) -> Any:
        """Attach the HTTP headers required to authenticate on the HTTP request"""
        self._ensure_initialized()
        quota = self._match_quota(request)
        token = self._acquire_call(quota)
        request.headers[self._header] = f"{self._auth_method} {token}".strip()
        return request

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._refresh_lock:
            if not self._initialized:
                self._seed_all_tokens()
                self._initialized = True

    def _match_quota(self, request: requests.PreparedRequest) -> TokenQuota:
        default_quota: Optional[TokenQuota] = None
        for quota in self._quotas:
            if quota.matchers:
                if any(matcher(request) for matcher in quota.matchers):
                    return quota
            elif default_quota is None:
                default_quota = quota
        if default_quota is None:
            if not self._unmatched_logged:
                self._logger.warning(
                    "Request %s did not match any quota pool; falling back to '%s'. Consider defining a matcher-less default pool.",
                    request.url,
                    self._quotas[0].name,
                )
                self._unmatched_logged = True
        return default_quota or self._quotas[0]

    def _acquire_call(self, quota: TokenQuota) -> str:
        """Reserve one call from the matched pool and return the token it was charged to.

        `max_wait_time` bounds the *total* time spent waiting across all refresh attempts of a
        single exhaustion episode, so stale reset timestamps cannot cause an endless reseed loop.
        """
        exhaustion_deadline: Optional[float] = None
        while True:
            budget_delay: Optional[float] = None
            wait_for_reset: Optional[float] = None
            with self._lock:
                token = self._active_token
                state = self._states[token][quota.name]
                self._restore_if_window_elapsed(state)
                if state.remaining > 0:
                    state.remaining -= 1
                    budget_delay = self._compute_budget_delay(quota)
                elif all(
                    self._is_pool_spent(self._states[token][quota.name]) for token in self._tokens
                ):
                    now = time.monotonic()
                    if exhaustion_deadline is None:
                        exhaustion_deadline = now + self._max_wait_time.total_seconds()
                    remaining_budget = exhaustion_deadline - now
                    min_time_to_wait = min(
                        (
                            self._states[token][quota.name].reset_at - ab_datetime_now()
                        ).total_seconds()
                        for token in self._tokens
                    )
                    if remaining_budget <= 0 or min_time_to_wait >= remaining_budget:
                        raise AirbyteTracedException(
                            failure_type=FailureType.transient_error,
                            internal_message=f"Rate limits for all tokens (quota: {quota.name}) were reached and the next reset exceeds max_wait_time",
                            message="Rate limit is exceeded for all provided tokens.",
                        )
                    wait_for_reset = min(
                        max(min_time_to_wait, self.MIN_EXHAUSTION_WAIT),
                        remaining_budget,
                    )
                else:
                    self._active_token = next(self._tokens_iter)
                    continue

            if wait_for_reset is not None:
                self._logger.info(
                    "All tokens exhausted (quota: %s). Waiting %.0fs until rate limit resets.",
                    quota.name,
                    wait_for_reset,
                )
                self._sleep_with_heartbeat(wait_for_reset, quota.name)
                self._refresh_after_exhaustion(quota)
                continue

            if budget_delay is not None and budget_delay >= 0.1:
                if not self._budget_logged:
                    self._logger.info(
                        "API budget: throttling requests (%.1fs delay) for quota '%s'.",
                        budget_delay,
                        quota.name,
                    )
                    self._budget_logged = True
                time.sleep(budget_delay)
            return token

    @staticmethod
    def _is_pool_spent(state: _QuotaState) -> bool:
        """Whether a pool is out of calls with no prospect of recovering on its own.

        A pool whose window has already elapsed is not spent -- its allowance is due back, so
        the caller should pick it up rather than treat every token as exhausted and wait.
        """
        if state.remaining > 0:
            return False
        return state.stale_zero or state.reset_at > ab_datetime_now()

    @staticmethod
    def _restore_if_window_elapsed(state: _QuotaState) -> None:
        """Give a spent pool its allowance back once its quota window has passed.

        Rotation skips spent tokens and only the all-exhausted branch refills them, so a token
        that reached zero stays out until *every* token is also at zero. Reading quota from
        responses makes a pool reachable zero in a single rate-limited response, which would
        otherwise send the sync into the exhaustion wait even when a token's window has already
        rolled over and its calls are available again.

        Must be called while holding the lock.
        """
        if state.stale_zero:
            return
        if state.remaining <= 0 and state.reset_at <= ab_datetime_now():
            state.remaining = state.limit

    def _compute_budget_delay(self, quota: TokenQuota) -> Optional[float]:
        """Compute the proactive throttling delay. Must be called while holding the lock."""
        states = [self._states[token][quota.name] for token in self._tokens]
        if not all(state.remaining <= self._get_budget_reserve(state) for state in states):
            return None

        active_state = self._states[self._active_token][quota.name]
        seconds_to_reset = max((active_state.reset_at - ab_datetime_now()).total_seconds(), 0)
        total_remaining = sum(max(state.remaining, 0) for state in states)
        if total_remaining <= 0 or seconds_to_reset <= 0:
            return None

        return min(seconds_to_reset / total_remaining, self.MAX_BUDGET_DELAY)

    def _get_budget_reserve(self, state: _QuotaState) -> float:
        return max(self._budget_min_reserve, state.limit * self._budget_reserve_fraction)

    def _sleep_with_heartbeat(self, total_seconds: float, quota_name: str) -> None:
        """Sleep for `total_seconds` in chunks, logging progress so operators can see the connector is not stuck."""
        remaining = total_seconds
        while remaining > 0:
            chunk = min(remaining, self.HEARTBEAT_INTERVAL)
            time.sleep(chunk)
            remaining -= chunk
            if remaining > 0:
                self._logger.info(
                    "Rate limit exhausted (quota: %s). Waiting for reset — %.0fs remaining.",
                    quota_name,
                    remaining,
                )

    def _refresh_after_exhaustion(self, quota: TokenQuota) -> None:
        """Refresh counters after an exhaustion wait. Only one thread refreshes; others re-check state."""
        with self._refresh_lock:
            with self._lock:
                still_exhausted = all(
                    self._states[token][quota.name].remaining <= 0 for token in self._tokens
                )
            if still_exhausted:
                self._seed_all_tokens()

    def _seed_all_tokens(self) -> None:
        # The wholesale swap intentionally discards local decrements made by concurrent threads
        # between the fetch and the swap: the server response is the closest thing to truth, and
        # merging local decrements on top of it would double-count the calls the server has
        # already observed. The worst case is a slight overcount of `remaining` (requests that
        # were in flight during the fetch), which at most causes a few 429s near the quota
        # boundary that the stream-level error handler absorbs.
        states = {token: self._fetch_quota_states(token) for token in self._tokens}
        with self._lock:
            self._states = states
            self._budget_logged = False

    def _fetch_quota_states(self, token: str) -> dict[str, _QuotaState]:
        http_client = self._token_to_http_client[token]
        _, response = http_client.send_request(
            http_method=self._quota_status_http_method,
            url=self._quota_status_url,
            headers=self._quota_status_headers,
            request_kwargs={},
        )
        response_body = response.json()

        states = {}
        for quota in self._quotas:
            remaining = self._extract_path(response_body, quota.remaining_path)
            reset = self._extract_path(response_body, quota.reset_path)
            limit = (
                self._extract_path(response_body, quota.limit_path)
                if quota.limit_path
                else remaining
            )
            reset_at = ab_datetime_parse(reset)
            states[quota.name] = _QuotaState(
                remaining=int(remaining),
                reset_at=reset_at,
                limit=int(limit),
                stale_zero=int(remaining) <= 0 and reset_at <= ab_datetime_now(),
            )
        return states

    def _extract_path(self, response_body: Mapping[str, Any], path: List[str]) -> Any:
        value: Any = response_body
        for key in path:
            if not isinstance(value, Mapping) or key not in value:
                raise AirbyteTracedException(
                    failure_type=FailureType.config_error,
                    internal_message=f"Quota status response did not contain expected path: {path}",
                    message="Quota status response is missing an expected field.",
                )
            value = value[key]
        return value

    def update_from_response(
        self, request: requests.PreparedRequest, response: requests.Response
    ) -> None:
        """Reconcile the matched pool's counters against what the server reported.

        Called once per HTTP attempt by `HttpClient`. Inert unless the matched pool declares
        response headers, so behaviour is unchanged for pools configured only with paths.

        The update is attributed to the token that *sent* the request rather than to the
        currently active one: under concurrency the authenticator may have rotated between the
        request going out and the response coming back.
        """
        quota = self._match_quota(request)
        if not quota.is_response_aware:
            return
        token = self._token_from_request(request)
        if token is None:
            return

        remaining = self._header_int(response, quota.remaining_header)
        if remaining is None and response.status_code in quota.exhaustion_status_codes:
            # The server rejected the call for rate-limit reasons but told us nothing about the
            # remaining count. Treat the pool as spent so the next request rotates.
            remaining = 0
        reset_at = self._header_datetime(response, quota.reset_header)
        limit = self._header_int(response, quota.limit_header)
        if remaining is None and limit is None and reset_at is None:
            return

        with self._lock:
            state = self._states.get(token, {}).get(quota.name)
            if state is None:
                return  # not seeded yet; the initial seeding is the more authoritative source
            if limit is not None and limit > 0:
                state.limit = limit
            if reset_at is not None and reset_at > state.reset_at:
                # The quota window rolled over, so the local count is meaningless -- take the
                # server's numbers wholesale. With no remaining header to go on, a fresh window
                # is worth a full limit.
                state.reset_at = reset_at
                state.remaining = remaining if remaining is not None else state.limit
            elif remaining is not None:
                # Same window, or a response whose reset is older than what we hold. Only ever
                # tighten the estimate: responses arrive out of order and a slow one carries a
                # stale, higher count, so handing those calls back would let concurrent requests
                # overspend. Clamping unconditionally also means an exhaustion signal is never
                # dropped -- an earlier reset value must not be able to mask `remaining: 0`.
                # The window itself is never moved backwards.
                state.remaining = min(state.remaining, remaining)

    def _token_from_request(self, request: requests.PreparedRequest) -> Optional[str]:
        """Recover the token a request was signed with from its auth header."""
        value = request.headers.get(self._header)
        if not value:
            return None
        token = value[len(self._auth_method) :].strip() if self._auth_method else value.strip()
        return token if token in self._states else None

    @staticmethod
    def _header_int(response: requests.Response, header: Optional[str]) -> Optional[int]:
        if not header:
            return None
        value = response.headers.get(header)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _header_datetime(
        response: requests.Response, header: Optional[str]
    ) -> Optional[AirbyteDateTime]:
        if not header:
            return None
        value = response.headers.get(header)
        if value is None:
            return None
        try:
            # Same parsing rules as `reset_path`, so epoch seconds and ISO 8601 both work.
            return ab_datetime_parse(value)
        except Exception:
            return None
