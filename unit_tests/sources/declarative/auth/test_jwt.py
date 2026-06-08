#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import base64
import logging
from datetime import datetime

import freezegun
import jwt
import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from airbyte_cdk.sources.declarative.auth.jwt import JwtAuthenticator
from airbyte_cdk.sources.declarative.requesters.request_option import (
    RequestOption,
    RequestOptionType,
)

LOGGER = logging.getLogger(__name__)


class TestJwtAuthenticator:
    """
    Test class for JWT Authenticator.
    """

    @pytest.mark.parametrize(
        "algorithm, kid, typ, cty, additional_jwt_headers, expected",
        [
            (
                "ALGORITHM",
                "test_kid",
                "test_typ",
                "test_cty",
                {"test": "test"},
                {
                    "kid": "test_kid",
                    "typ": "test_typ",
                    "cty": "test_cty",
                    "test": "test",
                    "alg": "ALGORITHM",
                },
            ),
            ("ALGORITHM", None, None, None, None, {"alg": "ALGORITHM"}),
        ],
    )
    def test_get_jwt_headers(self, algorithm, kid, typ, cty, additional_jwt_headers, expected):
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            algorithm=algorithm,
            secret_key="test_key",
            token_duration=1200,
            kid=kid,
            typ=typ,
            cty=cty,
            additional_jwt_headers=additional_jwt_headers,
        )
        assert authenticator._get_jwt_headers() == expected

    def test_given_overriden_reserverd_properties_get_jwt_headers_throws_error(self):
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            algorithm="ALGORITHM",
            secret_key="test_key",
            token_duration=1200,
            additional_jwt_headers={"kid": "test_kid"},
        )
        with pytest.raises(ValueError):
            authenticator._get_jwt_headers()

    @pytest.mark.parametrize(
        "iss, sub, aud, additional_jwt_payload, expected",
        [
            (
                "test_iss",
                "test_sub",
                "test_aud",
                {"test": "test"},
                {"iss": "test_iss", "sub": "test_sub", "aud": "test_aud", "test": "test"},
            ),
            (None, None, None, None, {}),
        ],
    )
    def test_get_jwt_payload(self, iss, sub, aud, additional_jwt_payload, expected):
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            algorithm="ALGORITHM",
            secret_key="test_key",
            token_duration=1000,
            iss=iss,
            sub=sub,
            aud=aud,
            additional_jwt_payload=additional_jwt_payload,
        )
        with freezegun.freeze_time("2022-01-01 00:00:00"):
            expected["iat"] = int(datetime.now().timestamp())
            expected["exp"] = expected["iat"] + 1000
            expected["nbf"] = expected["iat"]
            assert authenticator._get_jwt_payload() == expected

    def test_given_overriden_reserverd_properties_get_jwt_payload_throws_error(self):
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            algorithm="ALGORITHM",
            secret_key="test_key",
            token_duration=0,
            additional_jwt_payload={"exp": 1234},
        )
        with pytest.raises(ValueError):
            authenticator._get_jwt_payload()

    @pytest.mark.parametrize(
        "base64_encode_secret_key, secret_key, expected",
        [
            (True, "test", base64.b64encode("test".encode()).decode()),
            (False, "test", "test"),
        ],
    )
    def test_get_secret_key(self, base64_encode_secret_key, secret_key, expected):
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            secret_key=secret_key,
            algorithm="test_algo",
            token_duration=1200,
            base64_encode_secret_key=base64_encode_secret_key,
        )
        assert authenticator._get_secret_key() == expected

    def test_get_secret_key_from_config(
        self,
    ):
        authenticator = JwtAuthenticator(
            config={"secrets": '{"secret_key": "test"}'},
            parameters={},
            secret_key="{{ json_loads(config['secrets'])['secret_key']  }}",
            algorithm="test_algo",
            token_duration=1200,
            base64_encode_secret_key=False,
        )
        expected = "test"
        assert authenticator._get_secret_key() == expected

    def test_get_signed_token(self):
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            secret_key="test",
            algorithm="HS256",
            token_duration=1000,
            typ="JWT",
            iss="iss",
        )
        assert authenticator._get_signed_token() == jwt.encode(
            payload=authenticator._get_jwt_payload(),
            key=authenticator._get_secret_key(),
            algorithm=authenticator._algorithm,
            headers=authenticator._get_jwt_headers(),
        )

    def test_given_invalid_algorithm_get_signed_token_throws_error(self):
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            secret_key="test",
            algorithm="invalid algorithm type",
            token_duration=1000,
            base64_encode_secret_key=False,
            header_prefix="Bearer",
            typ="JWT",
            iss="iss",
            additional_jwt_headers={},
            additional_jwt_payload={},
        )
        with pytest.raises(ValueError):
            authenticator._get_signed_token()

    @pytest.mark.parametrize("header_prefix, expected", [("test", "test"), (None, None)])
    def test_get_header_prefix(self, header_prefix, expected):
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            secret_key="key",
            algorithm="test_algo",
            token_duration=1200,
            header_prefix=header_prefix,
        )
        assert authenticator._get_header_prefix() == expected

    def test_get_secret_key_with_passphrase(self):
        """Test _get_secret_key method with encrypted private key and passphrase."""
        # Generate a test RSA private key
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        )

        passphrase = b"test_passphrase"
        encrypted_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.BestAvailableEncryption(passphrase),
        )

        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            secret_key=encrypted_pem.decode(),
            algorithm="RS256",
            token_duration=1200,
            passphrase="test_passphrase",
        )

        result_key = authenticator._get_secret_key()

        assert isinstance(result_key, rsa.RSAPrivateKey)

        original_public_key = private_key.public_key()
        result_public_key = result_key.public_key()

        original_public_numbers = original_public_key.public_numbers()
        result_public_numbers = result_public_key.public_numbers()

        assert original_public_numbers.n == result_public_numbers.n
        assert original_public_numbers.e == result_public_numbers.e

    def test_get_secret_key_with_wrong_passphrase_raises_error(self):
        """Test that _get_secret_key raises error with wrong passphrase."""
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        )

        passphrase = b"correct_passphrase"
        encrypted_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.BestAvailableEncryption(passphrase),
        )

        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            secret_key=encrypted_pem.decode(),
            algorithm="RS256",
            token_duration=1200,
            passphrase="wrong_passphrase",
        )

        with pytest.raises(Exception):
            authenticator._get_secret_key()

    def test_get_signed_token_with_passphrase_protected_key(self):
        """Test that JWT signing works with passphrase-protected RSA private key."""
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        )

        passphrase = b"test_passphrase"
        encrypted_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.BestAvailableEncryption(passphrase),
        )

        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            secret_key=encrypted_pem.decode(),
            algorithm="RS256",
            token_duration=1000,
            passphrase="test_passphrase",
            typ="JWT",
            iss="test_issuer",
        )

        signed_token = authenticator._get_signed_token()

        assert isinstance(signed_token, str)
        assert len(signed_token.split(".")) == 3

        public_key = private_key.public_key()
        decoded_payload = jwt.decode(signed_token, public_key, algorithms=["RS256"])

        assert decoded_payload["iss"] == "test_issuer"
        assert "iat" in decoded_payload
        assert "exp" in decoded_payload

    @pytest.mark.parametrize(
        "request_option, expected_request_key",
        [
            pytest.param(
                RequestOption(
                    inject_into=RequestOptionType.request_parameter,
                    field_name="custom_parameter",
                    parameters={},
                ),
                "custom_parameter",
                id="test_get_request_headers",
            ),
            pytest.param(
                RequestOption(
                    inject_into=RequestOptionType.body_data, field_name="custom_body", parameters={}
                ),
                "custom_body",
                id="test_get_request_headers",
            ),
            pytest.param(
                RequestOption(
                    inject_into=RequestOptionType.body_json, field_name="custom_json", parameters={}
                ),
                "custom_json",
                id="test_get_request_headers",
            ),
        ],
    )
    def test_get_request_options(self, request_option, expected_request_key):
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            algorithm="HS256",
            secret_key="test_key",
            token_duration=1000,
            iss="test_iss",
            sub="test_sub",
            aud="test_aud",
            additional_jwt_payload={"kid": "test_kid"},
            request_option=request_option,
        )

        expected_request_options = {
            expected_request_key: jwt.encode(
                payload=authenticator._get_jwt_payload(),
                key=authenticator._get_secret_key(),
                algorithm=authenticator._algorithm,
                headers=authenticator._get_jwt_headers(),
            )
        }

        match request_option.inject_into:
            case RequestOptionType.request_parameter:
                actual_request_options = authenticator.get_request_params()
            case RequestOptionType.body_data:
                actual_request_options = authenticator.get_request_body_data()
            case RequestOptionType.body_json:
                actual_request_options = authenticator.get_request_body_json()
            case _:
                actual_request_options = None

        assert actual_request_options == expected_request_options

    @pytest.mark.parametrize(
        "request_option, expected_header_key",
        [
            pytest.param(
                RequestOption(
                    inject_into=RequestOptionType.header,
                    field_name="custom_authorization",
                    parameters={},
                ),
                "custom_authorization",
                id="test_get_request_headers",
            ),
            pytest.param(None, "Authorization", id="test_with_default_authorization_header"),
        ],
    )
    def test_get_request_headers(self, request_option, expected_header_key):
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            algorithm="HS256",
            secret_key="test_key",
            token_duration=1000,
            iss="test_iss",
            sub="test_sub",
            aud="test_aud",
            additional_jwt_payload={"kid": "test_kid"},
            request_option=request_option,
        )

        expected_headers = {
            expected_header_key: jwt.encode(
                payload=authenticator._get_jwt_payload(),
                key=authenticator._get_secret_key(),
                algorithm=authenticator._algorithm,
                headers=authenticator._get_jwt_headers(),
            )
        }

        assert authenticator.get_auth_header() == expected_headers

    def test_get_signed_token_with_escaped_newlines_in_pem_key(self):
        """Test that JWT signing works with PEM keys containing escaped newlines."""
        # Generate a test RSA private key
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        )

        # Get the PEM representation with actual newlines
        pem_with_newlines = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()

        # Create a version with escaped newlines (as stored in some systems)
        pem_with_escaped_newlines = pem_with_newlines.replace("\n", "\\n")

        # Test with escaped newlines - should work after normalization
        authenticator = JwtAuthenticator(
            config={},
            parameters={},
            secret_key=pem_with_escaped_newlines,
            algorithm="RS256",
            token_duration=1000,
            typ="JWT",
            iss="test_issuer",
        )

        signed_token = authenticator._get_signed_token()

        # Verify the token is valid
        assert isinstance(signed_token, str)
        assert len(signed_token.split(".")) == 3

        # Verify we can decode it with the public key
        public_key = private_key.public_key()
        decoded_payload = jwt.decode(signed_token, public_key, algorithms=["RS256"])

        assert decoded_payload["iss"] == "test_issuer"
        assert "iat" in decoded_payload
        assert "exp" in decoded_payload
