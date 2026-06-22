#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import InitVar, dataclass, field
from itertools import cycle
from typing import Any, Dict, List, Mapping, Optional

import requests

from airbyte_cdk.utils.datetime_helpers import AirbyteDateTime, ab_datetime_now, ab_datetime_parse

logger = logging.getLogger("airbyte")


@dataclass
class TokenState:
    """Tracks rate-limit state for an individual token."""

    remaining: int = -1  # -1 means unknown
    reset_at: Optional[AirbyteDateTime] = None


class TokenRotationStrategy(ABC):
    """Base class for token rotation strategies."""

    @abstractmethod
    def get_active_token(self) -> str:
        """Return the currently active token."""
        ...

    def update_from_response(self, response: requests.Response) -> None:
        """Update internal state from an HTTP response. Override in subclasses."""
        pass


@dataclass
class RoundRobinRotation(TokenRotationStrategy):
    """Cycle through tokens on each `get_active_token()` call."""

    tokens: List[str]
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._iter = cycle(self.tokens)
        self._current = next(self._iter)

    def get_active_token(self) -> str:
        token = self._current
        self._current = next(self._iter)
        return token


@dataclass
class RateLimitAwareRotation(TokenRotationStrategy):
    """Track per-token quota from response headers and rotate when exhausted.

    When a token's remaining quota hits zero, rotate to the next token. When all
    tokens are exhausted, sleep until the earliest reset time. Proactive throttling
    spreads remaining calls over the reset window to avoid hitting the wall.
    """

    tokens: List[str]
    parameters: InitVar[Mapping[str, Any]]
    ratelimit_remaining_header: str = "x-ratelimit-remaining"
    ratelimit_reset_header: str = "x-ratelimit-reset"
    max_wait_seconds: int = 7200
    budget_reserve_fraction: float = 0.1
    budget_min_reserve: int = 50

    _token_state: Dict[str, TokenState] = field(default_factory=dict, init=False)
    _active_index: int = field(default=0, init=False)
    _budget_logged: bool = field(default=False, init=False)

    HEARTBEAT_INTERVAL: float = 60.0

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._token_state = {t: TokenState() for t in self.tokens}

    def get_active_token(self) -> str:
        """Return the active token, rotating if the current one is exhausted."""
        attempts = 0
        while attempts < len(self.tokens):
            token = self.tokens[self._active_index]
            state = self._token_state[token]
            # If remaining is unknown (-1) or > 0, use this token
            if state.remaining != 0:
                return token
            # Current token is exhausted, try next
            self._rotate()
            attempts += 1

        # All tokens are exhausted — sleep until earliest reset
        self._sleep_until_reset()
        return self.tokens[self._active_index]

    def update_from_response(self, response: requests.Response) -> None:
        """Update the active token's state from response headers."""
        token = self.tokens[self._active_index]
        state = self._token_state[token]

        remaining_header = response.headers.get(self.ratelimit_remaining_header)
        reset_header = response.headers.get(self.ratelimit_reset_header)

        if remaining_header is not None:
            try:
                state.remaining = int(remaining_header)
            except (ValueError, TypeError):
                pass

        if reset_header is not None:
            try:
                reset_ts = float(reset_header)
                state.reset_at = ab_datetime_parse(str(int(reset_ts)))
            except (ValueError, TypeError):
                pass

        # Proactive rotation: if remaining is below reserve, rotate
        if state.remaining >= 0:
            reserve = self._get_budget_reserve(state)
            if state.remaining <= reserve:
                self._maybe_throttle(state)
                if state.remaining == 0:
                    self._rotate()

    def _rotate(self) -> None:
        self._active_index = (self._active_index + 1) % len(self.tokens)

    def _get_budget_reserve(self, state: TokenState) -> int:
        """Return the minimum number of calls to keep in reserve for a token."""
        limit_estimate = max(5000, state.remaining) if state.remaining > 0 else 5000
        return max(self.budget_min_reserve, int(limit_estimate * self.budget_reserve_fraction))

    def _maybe_throttle(self, state: TokenState) -> None:
        """Inject a small delay when all tokens are running low."""
        if not all(
            s.remaining >= 0 and s.remaining <= self._get_budget_reserve(s)
            for s in self._token_state.values()
            if s.remaining >= 0
        ):
            return

        if state.reset_at is None:
            return

        seconds_to_reset = max((state.reset_at - ab_datetime_now()).total_seconds(), 0)
        total_remaining = sum(max(s.remaining, 0) for s in self._token_state.values())
        if total_remaining <= 0 or seconds_to_reset <= 0:
            return

        delay = min(seconds_to_reset / total_remaining, 10.0)
        if delay >= 0.1:
            if not self._budget_logged:
                logger.info(
                    "API budget: throttling requests (%.1fs delay). %d calls remaining across %d token(s), "
                    "%.0fs until reset.",
                    delay,
                    total_remaining,
                    len(self.tokens),
                    seconds_to_reset,
                )
                self._budget_logged = True
            time.sleep(delay)

    def _sleep_until_reset(self) -> None:
        """Sleep until the earliest token reset time, or raise if too long."""
        reset_times = [s.reset_at for s in self._token_state.values() if s.reset_at is not None]
        if not reset_times:
            raise RuntimeError(
                "All tokens in the pool are exhausted and no reset time is available."
            )

        earliest_reset = min(reset_times)
        wait_seconds = max((earliest_reset - ab_datetime_now()).total_seconds(), 0)

        if wait_seconds > self.max_wait_seconds:
            raise RuntimeError(
                f"All tokens in the pool are exhausted. Earliest reset in {wait_seconds:.0f}s "
                f"exceeds max_wait_seconds ({self.max_wait_seconds}s)."
            )

        logger.info(
            "All tokens exhausted. Sleeping %.0fs until rate limit resets.",
            wait_seconds,
        )
        self._sleep_with_heartbeat(wait_seconds)

        # Reset state for all tokens after sleeping
        for state in self._token_state.values():
            state.remaining = -1
            state.reset_at = None
        self._budget_logged = False

    def _sleep_with_heartbeat(self, total_seconds: float) -> None:
        """Sleep with periodic log messages to keep the heartbeat alive."""
        remaining = total_seconds
        while remaining > 0:
            chunk = min(remaining, self.HEARTBEAT_INTERVAL)
            time.sleep(chunk)
            remaining -= chunk
            if remaining > 0:
                logger.info(
                    "Rate limit exhausted. Waiting for reset — %.0fs remaining.",
                    remaining,
                )
