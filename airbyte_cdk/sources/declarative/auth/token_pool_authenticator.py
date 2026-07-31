#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass, field
from typing import Any, List, Mapping, Optional, Union

import requests

from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.declarative.auth.token_rotation_strategies import (
    RateLimitAwareRotation,
    RoundRobinRotation,
    TokenRotationStrategy,
)
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config


@dataclass
class TokenPoolAuthenticator(DeclarativeAuthenticator):
    """Authenticator that rotates through multiple API tokens.

    Accepts a list of tokens (or a delimited string) and rotates through them
    using a configurable strategy (round-robin or rate-limit-aware). This enables
    distributing rate-limit consumption across multiple credentials.

    Attributes:
        tokens: Interpolated string resolving to the token(s). Can be a single token
            or multiple tokens joined by `token_separator`.
        config: The user-provided configuration.
        parameters: Additional runtime parameters for string interpolation.
        token_separator: Delimiter used to split `tokens` into individual values.
        auth_method: Prefix for the token value in the header (e.g., "Bearer", "token", "").
        header: HTTP header name to set.
        rotation_strategy: Strategy controlling how tokens are rotated.
    """

    tokens: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    token_separator: str = ","
    auth_method: str = "Bearer"
    header: str = "Authorization"
    rotation_strategy: Optional[TokenRotationStrategy] = None

    _token_list: List[str] = field(default_factory=list, init=False, repr=False)
    _strategy: TokenRotationStrategy = field(init=False, repr=False)

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        tokens_interpolated = InterpolatedString.create(self.tokens, parameters=parameters)
        raw_tokens = str(tokens_interpolated.eval(self.config))
        self._token_list = [t.strip() for t in raw_tokens.split(self.token_separator) if t.strip()]

        if not self._token_list:
            raise ValueError("TokenPoolAuthenticator requires at least one token.")

        if self.rotation_strategy is not None:
            self._strategy = self.rotation_strategy
            # Inject token list into strategy if not already populated
            if hasattr(self._strategy, "tokens") and not self._strategy.tokens:
                self._strategy.tokens = self._token_list
                if isinstance(self._strategy, RoundRobinRotation):
                    self._strategy.__post_init__(parameters)
                elif isinstance(self._strategy, RateLimitAwareRotation):
                    self._strategy.__post_init__(parameters)
        else:
            # Default to round-robin
            self._strategy = RoundRobinRotation(tokens=self._token_list, parameters=parameters)

    @property
    def auth_header(self) -> str:
        return self.header

    @property
    def token(self) -> str:
        raw_token = self._strategy.get_active_token()
        if self.auth_method:
            return f"{self.auth_method} {raw_token}"
        return raw_token

    def on_http_response(self, response: requests.Response) -> None:
        """Called after each HTTP response to update per-token rate-limit state."""
        self._strategy.update_from_response(response)

    def update_token(self) -> None:
        """Force rotation to the next token.

        Provided for compatibility with imperative-style connectors that call
        `authenticator.update_token()` from backoff strategies.
        """
        if isinstance(self._strategy, RateLimitAwareRotation):
            self._strategy._rotate()
        elif isinstance(self._strategy, RoundRobinRotation):
            # RoundRobinRotation advances on each get_active_token() call, so
            # calling get_active_token() once consumes the rotation.
            self._strategy.get_active_token()
