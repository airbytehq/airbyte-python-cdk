#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import base64
import json
from dataclasses import InitVar, dataclass
from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional, Union, cast

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey
from cryptography.hazmat.primitives.asymmetric.ed448 import Ed448PrivateKey
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean
from airbyte_cdk.sources.declarative.interpolation.interpolated_mapping import InterpolatedMapping
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.request_option import (
    RequestOption,
    RequestOptionType,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

# Type alias for keys that JWT library accepts
JwtKeyTypes = Union[
    RSAPrivateKey, EllipticCurvePrivateKey, Ed25519PrivateKey, Ed448PrivateKey, str, bytes
]


_UNUSABLE_KEY_MESSAGE = (
    "The key used to sign the JWT could not be used with the {algorithm} algorithm. Check that it "
    "is the complete key of the expected type, including its BEGIN and END lines and its line "
    "breaks, and, if the key is encrypted, its passphrase."
)


class JwtAlgorithm(str):
    """
    Enum for supported JWT algorithms
    """

    HS256 = "HS256"
    HS384 = "HS384"
    HS512 = "HS512"
    ES256 = "ES256"
    ES256K = "ES256K"
    ES384 = "ES384"
    ES512 = "ES512"
    RS256 = "RS256"
    RS384 = "RS384"
    RS512 = "RS512"
    PS256 = "PS256"
    PS384 = "PS384"
    PS512 = "PS512"
    EdDSA = "EdDSA"


@dataclass
class JwtAuthenticator(DeclarativeAuthenticator):
    """
    Generates a JSON Web Token (JWT) based on a declarative connector configuration file. The generated token is attached to each request via the Authorization header.

    Attributes:
        config (Mapping[str, Any]): The user-provided configuration as specified by the source's spec
        secret_key (Union[InterpolatedString, str]): The secret key used to sign the JWT
        algorithm (Union[str, JwtAlgorithm]): The algorithm used to sign the JWT
        token_duration (Optional[int]): The duration in seconds for which the token is valid
        iat_backdate_seconds (int): Seconds to move `iat` and `nbf` into the past to tolerate clock drift; `exp` is unaffected
        base64_encode_secret_key (Optional[Union[InterpolatedBoolean, str, bool]]): Whether to base64 encode the secret key
        header_prefix (Optional[Union[InterpolatedString, str]]): The prefix to add to the Authorization header
        kid (Optional[Union[InterpolatedString, str]]): The key identifier to be included in the JWT header
        typ (Optional[Union[InterpolatedString, str]]): The type of the JWT.
        cty (Optional[Union[InterpolatedString, str]]): The content type of the JWT.
        iss (Optional[Union[InterpolatedString, str]]): The issuer of the JWT.
        sub (Optional[Union[InterpolatedString, str]]): The subject of the JWT.
        aud (Optional[Union[InterpolatedString, str]]): The audience of the JWT.
        additional_jwt_headers (Optional[Mapping[str, Any]]): Additional headers to include in the JWT.
        additional_jwt_payload (Optional[Mapping[str, Any]]): Additional payload to include in the JWT.
    """

    config: Mapping[str, Any]
    parameters: InitVar[Mapping[str, Any]]
    secret_key: Union[InterpolatedString, str]
    algorithm: Union[str, JwtAlgorithm]
    token_duration: Optional[int]
    base64_encode_secret_key: Optional[Union[InterpolatedBoolean, str, bool]] = False
    header_prefix: Optional[Union[InterpolatedString, str]] = None
    kid: Optional[Union[InterpolatedString, str]] = None
    typ: Optional[Union[InterpolatedString, str]] = None
    cty: Optional[Union[InterpolatedString, str]] = None
    iss: Optional[Union[InterpolatedString, str]] = None
    sub: Optional[Union[InterpolatedString, str]] = None
    aud: Optional[Union[InterpolatedString, str]] = None
    additional_jwt_headers: Optional[Mapping[str, Any]] = None
    additional_jwt_payload: Optional[Mapping[str, Any]] = None
    passphrase: Optional[Union[InterpolatedString, str]] = None
    request_option: Optional[RequestOption] = None
    iat_backdate_seconds: int = 0

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.iat_backdate_seconds = self.iat_backdate_seconds or 0
        if self.iat_backdate_seconds < 0:
            raise ValueError(
                f"iat_backdate_seconds must be zero or positive, got {self.iat_backdate_seconds}"
            )
        self._secret_key = InterpolatedString.create(self.secret_key, parameters=parameters)
        self._algorithm = (
            JwtAlgorithm(self.algorithm) if isinstance(self.algorithm, str) else self.algorithm
        )
        self._base64_encode_secret_key = (
            InterpolatedBoolean(self.base64_encode_secret_key, parameters=parameters)
            if isinstance(self.base64_encode_secret_key, str)
            else self.base64_encode_secret_key
        )
        self._token_duration = self.token_duration
        self._header_prefix = (
            InterpolatedString.create(self.header_prefix, parameters=parameters)
            if self.header_prefix
            else None
        )
        self._kid = InterpolatedString.create(self.kid, parameters=parameters) if self.kid else None
        self._typ = InterpolatedString.create(self.typ, parameters=parameters) if self.typ else None
        self._cty = InterpolatedString.create(self.cty, parameters=parameters) if self.cty else None
        self._iss = InterpolatedString.create(self.iss, parameters=parameters) if self.iss else None
        self._sub = InterpolatedString.create(self.sub, parameters=parameters) if self.sub else None
        self._aud = InterpolatedString.create(self.aud, parameters=parameters) if self.aud else None
        self._additional_jwt_headers = InterpolatedMapping(
            self.additional_jwt_headers or {}, parameters=parameters
        )
        self._additional_jwt_payload = InterpolatedMapping(
            self.additional_jwt_payload or {}, parameters=parameters
        )
        self._passphrase = (
            InterpolatedString.create(self.passphrase, parameters=parameters)
            if self.passphrase
            else None
        )

        # When we first implemented the JWT authenticator, we assumed that the signed token was always supposed
        # to be loaded into the request headers under the `Authorization` key. This is not always the case, but
        # this default option allows for backwards compatibility to be retained for existing connectors
        self._request_option = self.request_option or RequestOption(
            inject_into=RequestOptionType.header, field_name="Authorization", parameters=parameters
        )

    def _get_jwt_headers(self) -> dict[str, Any]:
        """
        Builds and returns the headers used when signing the JWT.
        """
        headers = self._additional_jwt_headers.eval(self.config, json_loads=json.loads)
        if any(prop in headers for prop in ["kid", "alg", "typ", "cty"]):
            raise ValueError(
                "'kid', 'alg', 'typ', 'cty' are reserved headers and should not be set as part of 'additional_jwt_headers'"
            )

        if self._kid:
            headers["kid"] = self._eval_string_claim(self._kid)
        if self._typ:
            headers["typ"] = self._typ.eval(self.config, json_loads=json.loads)
        if self._cty:
            headers["cty"] = self._cty.eval(self.config, json_loads=json.loads)
        headers["alg"] = self._algorithm
        return headers

    def _get_jwt_payload(self) -> dict[str, Any]:
        """
        Builds and returns the payload used when signing the JWT.
        """
        now = int(datetime.now().timestamp())
        exp = now + self._token_duration if isinstance(self._token_duration, int) else now
        issued_at = now - self.iat_backdate_seconds

        payload = self._additional_jwt_payload.eval(self.config, json_loads=json.loads)
        if any(prop in payload for prop in ["iss", "sub", "aud", "iat", "exp", "nbf"]):
            raise ValueError(
                "'iss', 'sub', 'aud', 'iat', 'exp', 'nbf' are reserved properties and should not be set as part of 'additional_jwt_payload'"
            )

        if self._iss:
            payload["iss"] = self._eval_string_claim(self._iss)
        if self._sub:
            payload["sub"] = self._eval_string_claim(self._sub)
        if self._aud:
            payload["aud"] = self._aud.eval(self.config, json_loads=json.loads)

        payload["iat"] = issued_at
        payload["exp"] = exp
        payload["nbf"] = issued_at
        return payload

    def _eval_string_claim(self, claim: InterpolatedString) -> Optional[str]:
        """
        Evaluates `iss`, `sub` or `kid`, which RFC 7519 and RFC 7515 define as strings.

        Interpolation would turn a value that only looks numeric (an application, account or key ID)
        into an int or float. PyJWT refuses a non-string `kid`, and from 2.11 a non-string `iss`;
        older versions emit `iss` and `sub` as JSON numbers.
        """
        value = claim.eval(self.config, json_loads=json.loads, valid_types=(str,))
        # A missing optional config value renders as "None"; keep emitting null for it, as before.
        return None if value == "None" else cast(Optional[str], value)

    def _get_secret_key(self) -> JwtKeyTypes:
        """
        Returns the secret key used to sign the JWT.
        """
        secret_key: str = self._secret_key.eval(self.config, json_loads=json.loads)

        if self._passphrase:
            passphrase_value = self._passphrase.eval(self.config, json_loads=json.loads)
            if passphrase_value:
                try:
                    private_key = serialization.load_pem_private_key(
                        secret_key.encode(),
                        password=passphrase_value.encode(),
                    )
                except (ValueError, TypeError) as e:
                    raise AirbyteTracedException(
                        message=_UNUSABLE_KEY_MESSAGE.format(algorithm=self._algorithm),
                        internal_message=f"Failed to load the private key: {e}",
                        failure_type=FailureType.config_error,
                    ) from e
                return cast(JwtKeyTypes, private_key)

        return (
            base64.b64encode(secret_key.encode()).decode()
            if self._base64_encode_secret_key
            else secret_key
        )

    def _get_signed_token(self) -> Union[str, Any]:
        """
        Signed the JWT using the provided secret key and algorithm and the generated headers and payload. For additional information on PyJWT see: https://pyjwt.readthedocs.io/en/stable/
        """
        try:
            return jwt.encode(
                payload=self._get_jwt_payload(),
                key=self._prepare_signing_key(self._get_secret_key()),
                algorithm=self._algorithm,
                headers=self._get_jwt_headers(),
            )
        except AirbyteTracedException:
            raise
        except Exception as e:
            raise ValueError(f"Failed to sign token: {e}")

    def _prepare_signing_key(self, key: JwtKeyTypes) -> Any:
        """
        Parses the key the way PyJWT would, so an unusable key is reported as a config error whichever
        algorithm family it belongs to: PyJWT raises `InvalidKeyError` for RSA and for a key of the
        wrong type, but lets cryptography's `ValueError` through for EC and EdDSA keys.
        """
        try:
            algorithm = jwt.get_algorithm_by_name(self._algorithm)
        except NotImplementedError:
            # An unsupported algorithm is a manifest problem; `jwt.encode` reports it.
            return key
        try:
            return algorithm.prepare_key(key)
        except (jwt.exceptions.InvalidKeyError, ValueError, TypeError) as e:
            # PyJWT's messages talk about a "public key" even for a private one, so they stay internal.
            raise AirbyteTracedException(
                message=_UNUSABLE_KEY_MESSAGE.format(algorithm=self._algorithm),
                internal_message=f"Failed to sign token: {e}",
                failure_type=FailureType.config_error,
            ) from e

    def _get_header_prefix(self) -> Union[str, None]:
        """
        Returns the header prefix to be used when attaching the token to the request.
        """
        return (
            self._header_prefix.eval(self.config, json_loads=json.loads)
            if self._header_prefix
            else None
        )

    @property
    def auth_header(self) -> str:
        options = self._get_request_options(RequestOptionType.header)
        return next(iter(options.keys()), "")

    @property
    def token(self) -> str:
        return (
            f"{self._get_header_prefix()} {self._get_signed_token()}"
            if self._get_header_prefix()
            else self._get_signed_token()
        )

    def get_request_params(self) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.request_parameter)

    def get_request_body_data(self) -> Union[Mapping[str, Any], str]:
        return self._get_request_options(RequestOptionType.body_data)

    def get_request_body_json(self) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.body_json)

    def _get_request_options(self, option_type: RequestOptionType) -> Mapping[str, Any]:
        options: MutableMapping[str, Any] = {}
        if self._request_option.inject_into == option_type:
            self._request_option.inject_into_request(options, self.token, self.config)
        return options
