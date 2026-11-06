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
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, rsa

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.auth.jwt import JwtAuthenticator
from airbyte_cdk.sources.declarative.requesters.request_option import (
    RequestOption,
    RequestOptionType,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

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

    @freezegun.freeze_time("2022-01-01 00:00:00")
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
    @freezegun.freeze_time("2022-01-01 00:00:00")
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
    @freezegun.freeze_time("2022-01-01 00:00:00")
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


@pytest.mark.parametrize(
    "claim_value",
    [
        pytest.param("111", id="integer_like"),
        pytest.param("1.5", id="float_like"),
        pytest.param("True", id="boolean_like"),
        pytest.param("issuer-abc", id="plain_string"),
    ],
)
def test_iss_sub_and_kid_from_config_stay_strings(claim_value):
    """Interpolation literal-evaluates its output, so `{{ config['value'] }}` holding "111" used to
    become the int 111: PyJWT refused a non-string `kid` (and, from 2.11, `iss`), and older versions
    emitted `iss`/`sub` as JSON numbers."""
    authenticator = JwtAuthenticator(
        config={"value": claim_value},
        parameters={},
        secret_key="test",
        algorithm="HS256",
        token_duration=600,
        kid="{{ config['value'] }}",
        iss="{{ config['value'] }}",
        sub="{{ config['value'] }}",
    )

    token = authenticator._get_signed_token()

    claims = jwt.decode(token, "test", algorithms=["HS256"], options={"verify_aud": False})
    assert claims["iss"] == claim_value
    assert claims["sub"] == claim_value
    assert jwt.get_unverified_header(token)["kid"] == claim_value


@freezegun.freeze_time("2022-01-01 00:00:00")
@pytest.mark.parametrize(
    "iat_backdate_seconds, expected_iat",
    [
        pytest.param(None, 1640995200, id="default_is_now"),
        pytest.param(0, 1640995200, id="zero_is_now"),
        pytest.param(60, 1640995140, id="backdated_by_60s"),
    ],
)
def test_iat_backdate_moves_iat_and_nbf_but_not_exp(iat_backdate_seconds, expected_iat):
    kwargs = {} if iat_backdate_seconds is None else {"iat_backdate_seconds": iat_backdate_seconds}
    authenticator = JwtAuthenticator(
        config={},
        parameters={},
        secret_key="test",
        algorithm="HS256",
        token_duration=540,
        **kwargs,
    )

    payload = authenticator._get_jwt_payload()

    assert payload["iat"] == expected_iat
    assert payload["nbf"] == expected_iat
    assert payload["exp"] == 1640995200 + 540


def test_missing_optional_claim_is_omitted():
    """Jinja renders a missing optional value as "None". The claim must be left out rather than sent
    as that text, or as a null `kid`, which PyJWT refuses to sign."""
    authenticator = JwtAuthenticator(
        config={},
        parameters={},
        secret_key="test",
        algorithm="HS256",
        token_duration=600,
        kid="{{ config.get('key_id') }}",
        iss="{{ config.get('issuer') }}",
        sub="{{ config.get('subject') }}",
    )

    token = authenticator._get_signed_token()

    claims = jwt.decode(token, "test", algorithms=["HS256"])
    assert "iss" not in claims
    assert "sub" not in claims
    assert "kid" not in jwt.get_unverified_header(token)


@pytest.mark.parametrize(
    "secret",
    [
        pytest.param("1234567890", id="integer_like"),
        pytest.param("1.5", id="float_like"),
        pytest.param("True", id="boolean_like"),
    ],
)
@pytest.mark.parametrize("base64_encode_secret_key", [False, True])
def test_numeric_looking_hmac_secret_from_config_signs(secret, base64_encode_secret_key):
    authenticator = JwtAuthenticator(
        config={"secret": secret},
        parameters={},
        secret_key="{{ config['secret'] }}",
        algorithm="HS256",
        token_duration=600,
        base64_encode_secret_key=base64_encode_secret_key,
    )

    token = authenticator._get_signed_token()

    expected_key = (
        base64.b64encode(secret.encode()).decode() if base64_encode_secret_key else secret
    )
    assert jwt.decode(token, expected_key, algorithms=["HS256"])["exp"]


def test_numeric_looking_passphrase_from_config_signs():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    authenticator = JwtAuthenticator(
        config={"passphrase": "123456"},
        parameters={},
        secret_key=_pem(private_key, serialization.BestAvailableEncryption(b"123456")),
        algorithm="RS256",
        token_duration=600,
        passphrase="{{ config['passphrase'] }}",
    )

    token = authenticator._get_signed_token()

    assert jwt.decode(token, private_key.public_key(), algorithms=["RS256"])["exp"]


def test_missing_optional_passphrase_signs_an_unencrypted_key():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    authenticator = JwtAuthenticator(
        config={},
        parameters={},
        secret_key=_pem(private_key),
        algorithm="RS256",
        token_duration=600,
        passphrase="{{ config.get('passphrase') }}",
    )

    token = authenticator._get_signed_token()

    assert jwt.decode(token, private_key.public_key(), algorithms=["RS256"])["exp"]


def test_missing_hmac_secret_is_config_error_about_the_secret():
    authenticator = JwtAuthenticator(
        config={},
        parameters={},
        secret_key="{{ config.get('secret') }}",
        algorithm="HS256",
        token_duration=600,
    )

    with pytest.raises(AirbyteTracedException) as error:
        authenticator.token

    assert error.value.failure_type == FailureType.config_error
    assert "Check that the secret is set and complete." in error.value.message
    assert "BEGIN" not in error.value.message


@pytest.mark.parametrize(
    "iat_backdate_seconds, expected",
    [
        pytest.param(None, 0, id="none_defaults_to_zero"),
        pytest.param(30, 30, id="positive_is_kept"),
    ],
)
def test_iat_backdate_seconds_is_normalized(iat_backdate_seconds, expected):
    authenticator = JwtAuthenticator(
        config={},
        parameters={},
        secret_key="test",
        algorithm="HS256",
        token_duration=540,
        iat_backdate_seconds=iat_backdate_seconds,
    )

    assert authenticator.iat_backdate_seconds == expected


def test_negative_iat_backdate_seconds_is_rejected():
    with pytest.raises(ValueError, match="iat_backdate_seconds must be zero or positive"):
        JwtAuthenticator(
            config={},
            parameters={},
            secret_key="test",
            algorithm="HS256",
            token_duration=540,
            iat_backdate_seconds=-60,
        )


def _pem(private_key, encryption=serialization.NoEncryption()):
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=encryption,
    ).decode()


_RSA_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_GARBLED_PEM = "-----BEGIN PRIVATE KEY-----\\nMIIEowIBAAKCAQEA\\n-----END PRIVATE KEY-----"


@pytest.mark.parametrize(
    "algorithm, secret_key, passphrase",
    [
        pytest.param("RS256", _GARBLED_PEM, None, id="rs256_line_breaks_escaped_as_text"),
        pytest.param("RS256", "not a key", None, id="rs256_not_a_pem"),
        pytest.param("PS256", "not a key", None, id="ps256_not_a_pem"),
        pytest.param("ES256", "not a key", None, id="es256_not_a_pem"),
        pytest.param("ES256", _GARBLED_PEM, None, id="es256_garbled_pem"),
        pytest.param("EdDSA", _GARBLED_PEM, None, id="eddsa_garbled_pem"),
        pytest.param("ES256", _pem(_RSA_KEY), None, id="es256_given_an_rsa_key"),
        pytest.param("HS256", _pem(_RSA_KEY), None, id="hs256_given_an_rsa_key"),
        pytest.param(
            "RS256",
            _pem(_RSA_KEY, serialization.BestAvailableEncryption(b"correct_passphrase")),
            None,
            id="encrypted_key_without_passphrase",
        ),
        pytest.param(
            "RS256",
            _pem(_RSA_KEY, serialization.BestAvailableEncryption(b"correct_passphrase")),
            "wrong_passphrase",
            id="wrong_passphrase",
        ),
        pytest.param(
            "RS256", _pem(_RSA_KEY), "unused_passphrase", id="passphrase_for_unencrypted_key"
        ),
    ],
)
def test_unusable_key_is_config_error(algorithm, secret_key, passphrase):
    """PyJWT and cryptography report an unusable key with different exception types per algorithm
    family, and PyJWT's text talks about a "public key"; each must surface as the same config error."""
    authenticator = JwtAuthenticator(
        config={},
        parameters={},
        secret_key=secret_key,
        algorithm=algorithm,
        token_duration=540,
        passphrase=passphrase,
    )

    with pytest.raises(AirbyteTracedException) as error:
        authenticator.token

    assert error.value.failure_type == FailureType.config_error
    assert f"could not be used with the {algorithm} algorithm" in error.value.message
    assert "public key" not in error.value.message
    for secret in (passphrase, "correct_passphrase"):
        if secret:
            assert secret not in error.value.message
            assert secret not in error.value.internal_message


@pytest.mark.parametrize(
    "algorithm, private_key",
    [
        pytest.param("RS256", _RSA_KEY, id="rs256"),
        pytest.param("PS256", _RSA_KEY, id="ps256"),
        pytest.param("ES256", ec.generate_private_key(ec.SECP256R1()), id="es256"),
        pytest.param("EdDSA", ed25519.Ed25519PrivateKey.generate(), id="eddsa"),
    ],
)
def test_valid_asymmetric_key_signs(algorithm, private_key):
    authenticator = JwtAuthenticator(
        config={},
        parameters={},
        secret_key=_pem(private_key),
        algorithm=algorithm,
        token_duration=540,
    )

    token = authenticator._get_signed_token()

    claims = jwt.decode(token, private_key.public_key(), algorithms=[algorithm])
    assert claims["exp"] - claims["iat"] == 540


def test_unsupported_algorithm_is_still_a_value_error():
    """Only unreadable keys became config errors; a manifest-side signing failure keeps its type."""
    authenticator = JwtAuthenticator(
        config={},
        parameters={},
        secret_key="test",
        algorithm="invalid algorithm type",
        token_duration=540,
    )

    with pytest.raises(ValueError, match="Failed to sign token"):
        authenticator._get_signed_token()
