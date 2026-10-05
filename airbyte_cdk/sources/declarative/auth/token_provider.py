#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import datetime
import threading
from abc import abstractmethod
from dataclasses import InitVar, dataclass, field
from typing import Any, List, Mapping, Optional, Union

import dpath
from isodate import Duration

from airbyte_cdk.sources.declarative.decoders.decoder import Decoder
from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from airbyte_cdk.sources.declarative.exceptions import ReadException
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.requester import Requester
from airbyte_cdk.sources.http_logger import format_http_message
from airbyte_cdk.sources.message import MessageRepository, NoopMessageRepository
from airbyte_cdk.sources.types import Config
from airbyte_cdk.utils.datetime_helpers import AirbyteDateTime, ab_datetime_now


class TokenProvider:
    @abstractmethod
    def get_token(self) -> str:
        pass


@dataclass
class SessionTokenProvider(TokenProvider):
    login_requester: Requester
    session_token_path: List[str]
    expiration_duration: Optional[Union[datetime.timedelta, Duration]]
    parameters: InitVar[Mapping[str, Any]]
    message_repository: MessageRepository = NoopMessageRepository()
    decoder: Decoder = field(default_factory=lambda: JsonDecoder(parameters={}))

    _next_expiration_time: Optional[AirbyteDateTime] = None
    _token: Optional[str] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._lock = threading.Lock()

    def get_token(self) -> str:
        self._refresh_if_necessary()
        if self._token is None:
            raise ReadException("Failed to get session token, token is None")
        return self._token

    def _refresh_if_necessary(self) -> None:
        if self.expiration_duration is None:
            # Without an expiry nothing is cached and every call logs in, so there is nothing to lock
            self._refresh()
        elif self._token_has_expired():
            with self._lock:
                if self._token_has_expired():
                    self._refresh()

    def _token_has_expired(self) -> bool:
        return self._next_expiration_time is None or self._next_expiration_time < ab_datetime_now()

    def _refresh(self) -> None:
        response = self.login_requester.send_request(
            log_formatter=lambda response: format_http_message(
                response,
                "Login request",
                "Obtains session token",
                None,
                is_auxiliary=True,
                type="AUTH",
            ),
        )
        if response is None:
            raise ReadException("Failed to get session token, response got ignored by requester")
        session_token = dpath.get(next(self.decoder.decode(response)), self.session_token_path)
        # Set the token first: `get_token` reads it without the lock as soon as the expiry is fresh
        self._token = session_token  # type: ignore # Returned decoded response will be Mapping and therefore session_token will be str or None
        if self.expiration_duration is not None:
            self._next_expiration_time = ab_datetime_now() + self.expiration_duration


@dataclass
class InterpolatedStringTokenProvider(TokenProvider):
    """Provides a token by interpolating a string with config values."""

    config: Config
    api_token: Union[InterpolatedString, str]
    parameters: Mapping[str, Any]

    def __post_init__(self) -> None:
        self._token = InterpolatedString.create(self.api_token, parameters=self.parameters)

    def get_token(self) -> str:
        return str(self._token.eval(self.config))


@dataclass
class InterpolatedSessionTokenProvider(TokenProvider):
    """Provides a token by interpolating a template with the session token.

    This allows flexible token formatting, such as "Token {{ session_token }}"
    for Django REST Framework APIs that expect "Authorization: Token <value>".
    """

    config: Config
    api_token: Union[InterpolatedString, str]
    session_token_provider: TokenProvider
    parameters: Mapping[str, Any]

    def __post_init__(self) -> None:
        self._token_template = InterpolatedString.create(self.api_token, parameters=self.parameters)

    def get_token(self) -> str:
        session_token = self.session_token_provider.get_token()
        return str(self._token_template.eval(self.config, session_token=session_token))
