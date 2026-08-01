#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from datetime import timedelta
from typing import Any, List, Mapping, Optional, Sequence, Tuple, Union
import requests
from requests.auth import AuthBase
import dpath
import json
from json import JSONDecodeError
from abc import abstractmethod

from airbyte_cdk.config_observation import (
    create_connector_config_control_message,
    emit_configuration_as_airbyte_control_message,
)
from airbyte_cdk.sources.message import MessageRepository, NoopMessageRepository
from airbyte_cdk.utils.datetime_helpers import (
    AirbyteDateTime,
    ab_datetime_now,
    ab_datetime_parse,
)
from airbyte_cdk.models import FailureType, Level
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_cdk.sources.http_logger import format_http_message

class AbstractOauth2Authenticator(AuthBase):
    """Base class for OAuth2 authentication with shared token refresh logic"""

    def __init__(
        self,
        token_refresh_endpoint: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        client_id_name: str = "client_id",
        client_secret_name: str = "client_secret",
        refresh_token_name: str = "refresh_token",
        scopes: Optional[List[str]] = None,
        access_token_name: str = "access_token",
        expires_in_name: str = "expires_in",
        refresh_request_body: Optional[Mapping[str, Any]] = None,
        refresh_request_headers: Optional[Mapping[str, Any]] = None,
        grant_type_name: str = "grant_type",
        grant_type: str = "refresh_token",
        token_expiry_date: Optional[AirbyteDateTime] = None,
        token_expiry_date_format: Optional[str] = None,
        token_expiry_is_time_of_expiration: bool = False,
        refresh_token_error_status_codes: Tuple[int, ...] = (),
        refresh_token_error_key: str = "",
        refresh_token_error_values: Tuple[str, ...] = (),
    ):
        self._token_refresh_endpoint = token_refresh_endpoint
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._client_id_name = client_id_name
        self._client_secret_name = client_secret_name
        self._refresh_token_name = refresh_token_name
        self._scopes = scopes
        self._access_token_name = access_token_name
        self._expires_in_name = expires_in_name
        self._refresh_request_body = refresh_request_body
        self._refresh_request_headers = refresh_request_headers
        self._grant_type_name = grant_type_name
        self._grant_type = grant_type
        self._token_expiry_date = token_expiry_date
        self._token_expiry_date_format = token_expiry_date_format
        self._token_expiry_is_time_of_expiration = token_expiry_is_time_of_expiration
        self._refresh_token_error_status_codes = refresh_token_error_status_codes
        self._refresh_token_error_key = refresh_token_error_key
        self._refresh_token_error_values = refresh_token_error_values

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        request.headers.update(self.get_auth_header())
        return request

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"Authorization": f"Bearer {self.get_access_token()}"}

    def get_access_token(self) -> str:
        """Get the current access token, refreshing if expired"""
        if self.token_has_expired():
            self.refresh_access_token()
        return self.access_token

    def token_has_expired(self) -> bool:
        """Check if the current token has expired"""
        return not self.get_token_expiry_date() or ab_datetime_now() > self.get_token_expiry_date()

    def _make_refresh_request(self) -> Mapping[str, Any]:
        """
        Make the HTTP request to refresh OAuth tokens.
        
        Returns: 
            Mapping[str, Any]: The JSON response from the token refresh endpoint.
        
        Raises:
            DefaultBackoffException: If the response status code is 429 (Too Many Requests)
                                     or any 5xx server error.
            AirbyteTracedException: If the refresh token is invalid or expired, prompting
                                    re-authentication.
        """
        try:
            response = requests.post(
                url=self._token_refresh_endpoint,
                data=self._get_refresh_request_body(),
                headers=self._refresh_request_headers,
            )
            self._log_response(response)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if e.response is not None:
                if e.response.status_code == 429 or e.response.status_code >= 500:
                    raise DefaultBackoffException(request=e.response.request, response=e.response)
                if e.response.status_code in self._refresh_token_error_status_codes:
                    try:
                        error_value = e.response.json().get(self._refresh_token_error_key)
                        if error_value in self._refresh_token_error_values:
                            message = "Refresh token is invalid or expired. Please re-authenticate from Sources/<your source>/Settings."
                            raise AirbyteTracedException(
                                message=message,
                                internal_message=message,
                                failure_type=FailureType.config_error
                            )
                    except JSONDecodeError:
                        pass
            raise

    def _get_refresh_request_body(self) -> Mapping[str, Any]:
        """Get the request body for token refresh"""
        body = {
            self._grant_type_name: self._grant_type,
            self._client_id_name: self._client_id,
            self._client_secret_name: self._client_secret,
            self._refresh_token_name: self._refresh_token,
        }
        if self._scopes:
            body["scopes"] = self._scopes
        if self._refresh_request_body:
            # We defer to existing oauth constructs over custom configured fields
            for key, val in self._refresh_request_body.items():
                if key not in body:
                    body[key] = val
        return body

    def _parse_token_expiration_date(self, value: Union[str, int]) -> AirbyteDateTime:
        """Parse token expiration field from response into an AirbyteDateTime object"""
        if self._token_expiry_is_time_of_expiration:
            if not self._token_expiry_date_format:
                raise ValueError("Token expiry date format required when using expiration time")
            return ab_datetime_parse(str(value))
        else:
            try:
                seconds = int(float(str(value)))
                return ab_datetime_now() + timedelta(seconds=seconds)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid expires_in value: {value}")

    @property
    def _message_repository(self) -> MessageRepository:
        """Override in subclasses that need message logging"""
        return NoopMessageRepository()

    def _log_response(self, response: requests.Response) -> None:
        """Log the response if a message repository is configured"""
        if self._message_repository:
            self._message_repository.log_message(
                Level.DEBUG,
                lambda: format_http_message(
                    response,
                    "Refresh token",
                    "Obtains access token",
                    None,
                    is_auxiliary=True,
                    type="AUTH",
                ),
            )

    @property
    @abstractmethod
    def access_token(self) -> str:
        """Get current access token - implemented by subclasses"""
        pass

    @abstractmethod
    def refresh_access_token(self) -> None:
        """Refresh the access token - implemented by subclasses"""
        pass

    @abstractmethod
    def get_token_expiry_date(self) -> Optional[AirbyteDateTime]:
        """Get token expiry date - implemented by subclasses"""
        pass

class Oauth2Authenticator(AbstractOauth2Authenticator):
    """OAuth2 authenticator that stores tokens in memory"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._access_token: Optional[str] = None
        self._token_expiry_date: Optional[AirbyteDateTime] = None

    def refresh_access_token(self) -> Tuple[str, Union[str, int]]:
        """Refresh access token and return token and expiry"""
        response = self._make_refresh_request()
        access_token = response[self._access_token_name]
        expires_in = response.get(self._expires_in_name)
        
        self._access_token = access_token
        if expires_in:
            self._token_expiry_date = self._parse_token_expiration_date(expires_in)
        
        return access_token, expires_in

    def get_token_expiry_date(self) -> Optional[AirbyteDateTime]:
        return self._token_expiry_date

    @property
    def access_token(self) -> str:
        if not self._access_token:
            raise ValueError("Access token not set")
        return self._access_token

class SingleUseRefreshTokenOauth2Authenticator(AbstractOauth2Authenticator):
    """OAuth2 authenticator that stores tokens in config and emits updates"""

    def __init__(
        self,
        connector_config: Mapping[str, Any],
        token_refresh_endpoint: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        access_token_config_path: Sequence[str] = ("credentials", "access_token"),
        refresh_token_config_path: Sequence[str] = ("credentials", "refresh_token"),
        token_expiry_date_config_path: Sequence[str] = ("credentials", "token_expiry_date"),
        message_repository: MessageRepository = NoopMessageRepository(),
        **kwargs
    ):
        self._connector_config = connector_config
        self._access_token_config_path = access_token_config_path
        self._refresh_token_config_path = refresh_token_config_path
        self._token_expiry_date_config_path = token_expiry_date_config_path
        self.__message_repository = message_repository

        # Get credentials from config if not provided
        if not client_id:
            client_id = self._get_config_value_by_path(("credentials", "client_id"))
        if not client_secret:
            client_secret = self._get_config_value_by_path(("credentials", "client_secret"))

        super().__init__(
            token_refresh_endpoint=token_refresh_endpoint,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=self._get_config_value_by_path(refresh_token_config_path),
            **kwargs
        )

    def refresh_access_token(self) -> Tuple[str, Union[str, int], str]:
        """Refresh access token and update config"""
        response = self._make_refresh_request()
        
        access_token = response[self._access_token_name]
        refresh_token = response.get(self._refresh_token_name)
        expires_in = response.get(self._expires_in_name)

        self._update_config(access_token, refresh_token, expires_in)
        return access_token, expires_in, refresh_token

    def _update_config(
        self, 
        access_token: str,
        refresh_token: Optional[str],
        expires_in: Optional[Union[str, int]]
    ) -> None:
        """Update the config with new token values"""
        dpath.new(self._connector_config, self._access_token_config_path, access_token)
        if refresh_token:
            dpath.new(self._connector_config, self._refresh_token_config_path, refresh_token)
            self._refresh_token = refresh_token
        
        if expires_in:
            expiry_date = self._parse_token_expiration_date(expires_in)
            dpath.new(
                self._connector_config,
                self._token_expiry_date_config_path,
                str(expiry_date)
            )

        self._emit_control_message()

    def _emit_control_message(self) -> None:
        """Emit control message for config update"""
        if not isinstance(self.__message_repository, NoopMessageRepository):
            self.__message_repository.emit_message(
                create_connector_config_control_message(self._connector_config)
            )
        else:
            emit_configuration_as_airbyte_control_message(self._connector_config)

    def get_token_expiry_date(self) -> AirbyteDateTime:
        """Get token expiry date from config"""
        expiry_date = self._get_config_value_by_path(self._token_expiry_date_config_path)
        if expiry_date == "":
            return ab_datetime_now() - timedelta(days=1)
        return ab_datetime_parse(str(expiry_date))

    @property
    def access_token(self) -> str:
        """Get access token from config"""
        return self._get_config_value_by_path(self._access_token_config_path)

    def _get_config_value_by_path(
        self,
        config_path: Union[str, Sequence[str]],
        default: Optional[str] = None
    ) -> str:
        """Get a value from the config using a path"""
        return dpath.get(
            self._connector_config,
            config_path,
            default=default if default is not None else "",
        )

    @property
    def _message_repository(self) -> MessageRepository:
        return self.__message_repository