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
    """

    name: str
    remaining_path: List[str]
    reset_path: List[str]
    limit_path: Optional[List[str]] = None
    matchers: List[RequestMatcher] = field(default_factory=list)


@dataclass
class _QuotaState:
    remaining: int
    reset_at: AirbyteDateTime
    limit: int


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
    exhaustion wait. All state transitions are guarded by a lock so the authenticator can be
    shared safely across concurrent streams; sleeps never hold the lock.
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
            raise ValueError(
                "RateLimitedMultipleTokenAuthenticator requires at least one quota pool"
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
            self._logger.debug(
                "Request %s did not match any quota pool; falling back to '%s'. Consider defining a matcher-less default pool.",
                request.url,
                self._quotas[0].name,
            )
        return default_quota or self._quotas[0]

    def _acquire_call(self, quota: TokenQuota) -> str:
        """Reserve one call from the matched pool and return the token it was charged to."""
        while True:
            budget_delay: Optional[float] = None
            wait_for_reset: Optional[float] = None
            with self._lock:
                token = self._active_token
                state = self._states[token][quota.name]
                if state.remaining > 0:
                    state.remaining -= 1
                    budget_delay = self._compute_budget_delay(quota)
                elif all(self._states[token][quota.name].remaining <= 0 for token in self._tokens):
                    min_time_to_wait = min(
                        (
                            self._states[token][quota.name].reset_at - ab_datetime_now()
                        ).total_seconds()
                        for token in self._tokens
                    )
                    if min_time_to_wait >= self._max_wait_time.total_seconds():
                        raise AirbyteTracedException(
                            failure_type=FailureType.transient_error,
                            internal_message=f"Rate limits for all tokens (quota: {quota.name}) were reached and the next reset exceeds max_wait_time",
                            message="Rate limit is exceeded for all provided tokens.",
                        )
                    wait_for_reset = min(
                        max(min_time_to_wait, self.MIN_EXHAUSTION_WAIT),
                        self._max_wait_time.total_seconds(),
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
            states[quota.name] = _QuotaState(
                remaining=int(remaining),
                reset_at=ab_datetime_parse(reset),
                limit=int(limit),
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
