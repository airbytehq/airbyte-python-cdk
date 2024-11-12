#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import logging
import os
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import InitVar, dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

from airbyte_cdk.sources.declarative.auth.declarative_authenticator import (
    DeclarativeAuthenticator,
    NoAuth,
)
from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_request_options_provider import (
    InterpolatedRequestOptionsProvider,
)
from airbyte_cdk.sources.declarative.requesters.requester import HttpMethod, Requester
from airbyte_cdk.sources.message import MessageRepository, NoopMessageRepository
from airbyte_cdk.sources.streams.http import HttpClient
from airbyte_cdk.utils.mapping_helpers import combine_mappings


if TYPE_CHECKING:
    import requests

    from airbyte_cdk.sources.declarative.decoders import Decoder
    from airbyte_cdk.sources.streams.http.error_handlers import ErrorHandler
    from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class HttpRequester(Requester):
    """Default implementation of a Requester

    Attributes:
        name (str): Name of the stream. Only used for request/response caching
        url_base (Union[InterpolatedString, str]): Base url to send requests to
        path (Union[InterpolatedString, str]): Path to send requests to
        http_method (Union[str, HttpMethod]): HTTP method to use when sending requests
        request_options_provider (Optional[InterpolatedRequestOptionsProvider]): request option provider defining the options to set on outgoing requests
        authenticator (DeclarativeAuthenticator): Authenticator defining how to authenticate to the source
        error_handler (Optional[ErrorHandler]): Error handler defining how to detect and handle errors
        backoff_strategies (Optional[List[BackoffStrategy]]): List of backoff strategies to use when retrying requests
        config (Config): The user-provided configuration as specified by the source's spec
        use_cache (bool): Indicates that data should be cached for this stream
    """

    name: str
    url_base: InterpolatedString | str
    path: InterpolatedString | str
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    authenticator: DeclarativeAuthenticator | None = None
    http_method: str | HttpMethod = HttpMethod.GET
    request_options_provider: InterpolatedRequestOptionsProvider | None = None
    error_handler: ErrorHandler | None = None
    disable_retries: bool = False
    message_repository: MessageRepository = NoopMessageRepository()  # noqa: RUF009  (function in default arg)
    use_cache: bool = False
    _exit_on_rate_limit: bool = False
    stream_response: bool = False
    decoder: Decoder = field(default_factory=lambda: JsonDecoder(parameters={}))

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._url_base = InterpolatedString.create(self.url_base, parameters=parameters)
        self._path = InterpolatedString.create(self.path, parameters=parameters)
        if self.request_options_provider is None:
            self._request_options_provider = InterpolatedRequestOptionsProvider(
                config=self.config, parameters=parameters
            )
        elif isinstance(self.request_options_provider, dict):
            self._request_options_provider = InterpolatedRequestOptionsProvider(
                config=self.config, **self.request_options_provider
            )
        else:
            self._request_options_provider = self.request_options_provider
        self._authenticator = self.authenticator or NoAuth(parameters=parameters)
        self._http_method = (
            HttpMethod[self.http_method] if isinstance(self.http_method, str) else self.http_method
        )
        self.error_handler = self.error_handler
        self._parameters = parameters

        if self.error_handler is not None and hasattr(self.error_handler, "backoff_strategies"):
            backoff_strategies = self.error_handler.backoff_strategies
        else:
            backoff_strategies = None

        self._http_client = HttpClient(
            name=self.name,
            logger=self.logger,
            error_handler=self.error_handler,
            authenticator=self._authenticator,
            use_cache=self.use_cache,
            backoff_strategy=backoff_strategies,
            disable_retries=self.disable_retries,
            message_repository=self.message_repository,
        )

    @property
    def exit_on_rate_limit(self) -> bool:
        return self._exit_on_rate_limit

    @exit_on_rate_limit.setter
    def exit_on_rate_limit(self, value: bool) -> None:
        self._exit_on_rate_limit = value

    def get_authenticator(self) -> DeclarativeAuthenticator:
        return self._authenticator

    def get_url_base(self) -> str:
        return os.path.join(self._url_base.eval(self.config), "")  # noqa: PTH118  (prefer pathlib)

    def get_path(
        self,
        *,
        stream_state: StreamState | None,
        stream_slice: StreamSlice | None,
        next_page_token: Mapping[str, Any] | None,
    ) -> str:
        kwargs = {
            "stream_state": stream_state,
            "stream_slice": stream_slice,
            "next_page_token": next_page_token,
        }
        path = str(self._path.eval(self.config, **kwargs))
        return path.lstrip("/")

    def get_method(self) -> HttpMethod:
        return self._http_method

    def get_request_params(
        self,
        *,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> MutableMapping[str, Any]:
        return self._request_options_provider.get_request_params(
            stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token
        )

    def get_request_headers(
        self,
        *,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        return self._request_options_provider.get_request_headers(
            stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token
        )

    # fixing request options provider types has a lot of dependencies
    def get_request_body_data(  # type: ignore
        self,
        *,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any] | str:
        return (
            self._request_options_provider.get_request_body_data(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            )
            or {}
        )

    # fixing request options provider types has a lot of dependencies
    def get_request_body_json(  # type: ignore
        self,
        *,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any] | None:
        return self._request_options_provider.get_request_body_json(
            stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token
        )

    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f"airbyte.HttpRequester.{self.name}")

    def _get_request_options(
        self,
        stream_state: StreamState | None,
        stream_slice: StreamSlice | None,
        next_page_token: Mapping[str, Any] | None,
        requester_method: Callable[..., Mapping[str, Any] | str | None],
        auth_options_method: Callable[..., Mapping[str, Any] | str | None],
        extra_options: Mapping[str, Any] | str | None = None,
    ) -> Mapping[str, Any] | str:
        """Get the request_option from the requester, the authenticator and extra_options passed in.
        Raise a ValueError if there's a key collision
        Returned merged mapping otherwise
        """
        return combine_mappings(
            [
                requester_method(
                    stream_state=stream_state,
                    stream_slice=stream_slice,
                    next_page_token=next_page_token,
                ),
                auth_options_method(),
                extra_options,
            ]
        )

    def _request_headers(
        self,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
        extra_headers: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        """Specifies request headers.
        Authentication headers will overwrite any overlapping headers returned from this method.
        """
        headers = self._get_request_options(
            stream_state,
            stream_slice,
            next_page_token,
            self.get_request_headers,
            self.get_authenticator().get_auth_header,
            extra_headers,
        )
        if isinstance(headers, str):
            raise ValueError("Request headers cannot be a string")  # noqa: TRY004  (expected TypeError)
        return {str(k): str(v) for k, v in headers.items()}

    def _request_params(
        self,
        stream_state: StreamState | None,
        stream_slice: StreamSlice | None,
        next_page_token: Mapping[str, Any] | None,
        extra_params: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        """Specifies the query parameters that should be set on an outgoing HTTP request given the inputs.

        E.g: you might want to define query parameters for paging if next_page_token is not None.
        """
        options = self._get_request_options(
            stream_state,
            stream_slice,
            next_page_token,
            self.get_request_params,
            self.get_authenticator().get_request_params,
            extra_params,
        )
        if isinstance(options, str):
            raise ValueError("Request params cannot be a string")  # noqa: TRY004  (expected TypeError)

        for k, v in options.items():
            if isinstance(v, dict):
                raise ValueError(  # noqa: TRY004  (expected TypeError)
                    f"Invalid value for `{k}` parameter. The values of request params cannot be an object."
                )

        return options

    def _request_body_data(
        self,
        stream_state: StreamState | None,
        stream_slice: StreamSlice | None,
        next_page_token: Mapping[str, Any] | None,
        extra_body_data: Mapping[str, Any] | str | None = None,
    ) -> Mapping[str, Any] | str | None:
        """Specifies how to populate the body of the request with a non-JSON payload.

        If returns a ready text that it will be sent as is.
        If returns a dict that it will be converted to a urlencoded form.
        E.g. {"key1": "value1", "key2": "value2"} => "key1=value1&key2=value2"

        At the same time only one of the 'request_body_data' and 'request_body_json' functions can be overridden.
        """
        # Warning: use self.state instead of the stream_state passed as argument!
        return self._get_request_options(
            stream_state,
            stream_slice,
            next_page_token,
            self.get_request_body_data,
            self.get_authenticator().get_request_body_data,
            extra_body_data,
        )

    def _request_body_json(
        self,
        stream_state: StreamState | None,
        stream_slice: StreamSlice | None,
        next_page_token: Mapping[str, Any] | None,
        extra_body_json: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any] | None:
        """Specifies how to populate the body of the request with a JSON payload.

        At the same time only one of the 'request_body_data' and 'request_body_json' functions can be overridden.
        """
        # Warning: use self.state instead of the stream_state passed as argument!
        options = self._get_request_options(
            stream_state,
            stream_slice,
            next_page_token,
            self.get_request_body_json,
            self.get_authenticator().get_request_body_json,
            extra_body_json,
        )
        if isinstance(options, str):
            raise ValueError("Request body json cannot be a string")  # noqa: TRY004  (expected TypeError)
        return options

    @classmethod
    def _join_url(cls, url_base: str, path: str) -> str:
        return urljoin(url_base, path)

    def send_request(  # noqa: PLR0913, PLR0917  (too many args)
        self,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
        next_page_token: Mapping[str, Any] | None = None,
        path: str | None = None,
        request_headers: Mapping[str, Any] | None = None,
        request_params: Mapping[str, Any] | None = None,
        request_body_data: Mapping[str, Any] | str | None = None,
        request_body_json: Mapping[str, Any] | None = None,
        log_formatter: Callable[[requests.Response], Any] | None = None,
    ) -> requests.Response | None:
        _, response = self._http_client.send_request(
            http_method=self.get_method().value,
            url=self._join_url(
                self.get_url_base(),
                path
                or self.get_path(
                    stream_state=stream_state,
                    stream_slice=stream_slice,
                    next_page_token=next_page_token,
                ),
            ),
            request_kwargs={"stream": self.stream_response},
            headers=self._request_headers(
                stream_state, stream_slice, next_page_token, request_headers
            ),
            params=self._request_params(
                stream_state, stream_slice, next_page_token, request_params
            ),
            json=self._request_body_json(
                stream_state, stream_slice, next_page_token, request_body_json
            ),
            data=self._request_body_data(
                stream_state, stream_slice, next_page_token, request_body_data
            ),
            dedupe_query_params=True,
            log_formatter=log_formatter,
            exit_on_rate_limit=self._exit_on_rate_limit,
        )

        return response
