import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import jwt
import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.testclient import TestClient

from airbyte_cdk.manifest_server.app import app
from airbyte_cdk.manifest_server.helpers.auth import verify_jwt_token


class TestVerifyJwtToken:
    """Test cases for JWT token verification."""

    @pytest.mark.parametrize(
        "env",
        [
            pytest.param({}, id="secret_unset"),
            pytest.param({"AB_JWT_SIGNATURE_SECRET": ""}, id="secret_empty"),
            pytest.param({"AB_JWT_SIGNATURE_SECRET": "  \n"}, id="secret_whitespace_only"),
            pytest.param(
                {"AIRBYTE_MANIFEST_SERVER_ALLOW_UNAUTHENTICATED": "false"},
                id="secret_unset_opt_out_false",
            ),
        ],
    )
    @pytest.mark.parametrize(
        "credentials",
        [
            pytest.param(None, id="no_token"),
            pytest.param(
                HTTPAuthorizationCredentials(scheme="Bearer", credentials="any-token"),
                id="arbitrary_token",
            ),
        ],
    )
    def test_no_secret_rejects_requests(self, env, credentials):
        """Without AB_JWT_SIGNATURE_SECRET, requests are rejected instead of allowed through."""
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(HTTPException) as exc_info:
                verify_jwt_token(credentials)

        assert exc_info.value.status_code == 503
        assert "AB_JWT_SIGNATURE_SECRET" in exc_info.value.detail

    @pytest.mark.parametrize("opt_out_value", ["true", "TRUE"])
    def test_no_secret_with_explicit_opt_out_allows_requests(self, opt_out_value):
        """Running without authentication requires an explicit opt-out."""
        with patch.dict(
            os.environ,
            {"AIRBYTE_MANIFEST_SERVER_ALLOW_UNAUTHENTICATED": opt_out_value},
            clear=True,
        ):
            verify_jwt_token(None)
            verify_jwt_token(HTTPAuthorizationCredentials(scheme="Bearer", credentials="any-token"))

    def test_opt_out_is_ignored_when_secret_is_set(self):
        """A configured secret is always enforced, even if the opt-out is set."""
        with patch.dict(
            os.environ,
            {
                "AB_JWT_SIGNATURE_SECRET": "test-secret",
                "AIRBYTE_MANIFEST_SERVER_ALLOW_UNAUTHENTICATED": "true",
            },
            clear=True,
        ):
            with pytest.raises(HTTPException) as exc_info:
                verify_jwt_token(None)

        assert exc_info.value.status_code == 401

    def test_missing_credentials_with_secret_raises_401(self):
        """Test that missing credentials raise 401 when secret is configured."""
        with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": "test-secret"}):
            with pytest.raises(HTTPException) as exc_info:
                verify_jwt_token(None)

            assert exc_info.value.status_code == 401
            assert exc_info.value.detail == "Bearer token required"
            assert exc_info.value.headers == {"WWW-Authenticate": "Bearer"}

    def test_invalid_token_raises_401(self):
        """Test that invalid JWT tokens raise 401."""
        with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": "test-secret"}):
            invalid_credentials = HTTPAuthorizationCredentials(
                scheme="Bearer", credentials="invalid.jwt.token"
            )

            with pytest.raises(HTTPException) as exc_info:
                verify_jwt_token(invalid_credentials)

            assert exc_info.value.status_code == 401
            assert exc_info.value.detail == "Invalid token"
            assert exc_info.value.headers == {"WWW-Authenticate": "Bearer"}

    def test_malformed_token_raises_401(self):
        """Test that malformed tokens raise 401."""
        with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": "test-secret"}):
            malformed_credentials = HTTPAuthorizationCredentials(
                scheme="Bearer", credentials="not-a-jwt-token"
            )

            with pytest.raises(HTTPException) as exc_info:
                verify_jwt_token(malformed_credentials)

            assert exc_info.value.status_code == 401
            assert exc_info.value.detail == "Invalid token"

    def test_valid_token_passes(self):
        """Test that valid JWT tokens pass verification."""
        secret = "test-secret-key"
        payload = {
            "exp": datetime.now(timezone.utc) + timedelta(hours=1),
            "iat": datetime.now(timezone.utc),
            "sub": "test-user",
        }
        valid_token = jwt.encode(payload, secret, algorithm="HS256")

        with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": secret}):
            valid_credentials = HTTPAuthorizationCredentials(
                scheme="Bearer", credentials=valid_token
            )

            # Should not raise any exception
            verify_jwt_token(valid_credentials)

    def test_expired_token_raises_401(self):
        """Test that expired JWT tokens raise 401."""
        secret = "test-secret-key"
        expired_payload = {
            "exp": datetime.now(timezone.utc) - timedelta(hours=1),
            "iat": datetime.now(timezone.utc) - timedelta(hours=2),
            "sub": "test-user",
        }
        expired_token = jwt.encode(expired_payload, secret, algorithm="HS256")

        with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": secret}):
            expired_credentials = HTTPAuthorizationCredentials(
                scheme="Bearer", credentials=expired_token
            )

            with pytest.raises(HTTPException) as exc_info:
                verify_jwt_token(expired_credentials)

            assert exc_info.value.status_code == 401
            assert exc_info.value.detail == "Invalid token"

    def test_wrong_secret_raises_401(self):
        """Test that tokens signed with wrong secret raise 401."""
        correct_secret = "correct-secret"
        wrong_secret = "wrong-secret"

        payload = {
            "exp": datetime.now(timezone.utc) + timedelta(hours=1),
            "iat": datetime.now(timezone.utc),
            "sub": "test-user",
        }
        token_with_wrong_secret = jwt.encode(payload, wrong_secret, algorithm="HS256")

        with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": correct_secret}):
            wrong_credentials = HTTPAuthorizationCredentials(
                scheme="Bearer", credentials=token_with_wrong_secret
            )

            with pytest.raises(HTTPException) as exc_info:
                verify_jwt_token(wrong_credentials)

            assert exc_info.value.status_code == 401
            assert exc_info.value.detail == "Invalid token"

    def test_token_without_expiry_raises_401(self):
        """A token with a valid signature but no expiry would never expire, so it is rejected."""
        secret = "test-secret"
        never_expiring_token = jwt.encode({"custom": "data"}, secret, algorithm="HS256")

        with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": secret}):
            with pytest.raises(HTTPException) as exc_info:
                verify_jwt_token(
                    HTTPAuthorizationCredentials(scheme="Bearer", credentials=never_expiring_token)
                )

        assert exc_info.value.status_code == 401


def _token(secret, **claims):
    payload = {"exp": datetime.now(timezone.utc) + timedelta(minutes=5), **claims}
    return HTTPAuthorizationCredentials(
        scheme="Bearer", credentials=jwt.encode(payload, secret, algorithm="HS256")
    )


@pytest.mark.parametrize(
    "audience",
    [
        pytest.param("airbyte-server", id="other_service"),
        pytest.param(["airbyte-server", "airbyte-workload-api"], id="other_services"),
    ],
)
def test_token_for_another_audience_raises_401(audience):
    with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": "shared"}):
        with pytest.raises(HTTPException) as exc_info:
            verify_jwt_token(_token("shared", aud=audience))

    assert exc_info.value.status_code == 401


@pytest.mark.parametrize(
    "audience",
    [
        pytest.param("airbyte-manifest-server", id="manifest_server"),
        pytest.param(["airbyte-server", "airbyte-manifest-server"], id="list_with_manifest_server"),
    ],
)
def test_token_for_the_manifest_server_passes(audience):
    with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": "shared"}):
        verify_jwt_token(_token("shared", aud=audience))


def test_token_without_audience_passes_unless_audience_is_required():
    with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": "shared"}):
        verify_jwt_token(_token("shared"))

    with patch.dict(
        os.environ,
        {"AB_JWT_SIGNATURE_SECRET": "shared", "AB_MANIFEST_SERVER_JWT_REQUIRE_AUDIENCE": "true"},
    ):
        with pytest.raises(HTTPException) as exc_info:
            verify_jwt_token(_token("shared"))
        verify_jwt_token(_token("shared", aud="airbyte-manifest-server"))

    assert exc_info.value.status_code == 401


def test_dedicated_signing_key_replaces_the_shared_key():
    """With its own key set, the manifest server no longer accepts tokens signed with the shared key."""
    env = {
        "AB_JWT_SIGNATURE_SECRET": "shared",
        "AB_MANIFEST_SERVER_JWT_SIGNATURE_SECRET": "manifest-server-only",
    }
    with patch.dict(os.environ, env):
        with pytest.raises(HTTPException) as exc_info:
            verify_jwt_token(_token("shared", aud="airbyte-manifest-server"))
        verify_jwt_token(_token("manifest-server-only", aud="airbyte-manifest-server"))

    assert exc_info.value.status_code == 401


def test_dedicated_signing_key_with_whitespace_is_used_as_is():
    secret = " manifest-server-only\n"
    with patch.dict(os.environ, {"AB_MANIFEST_SERVER_JWT_SIGNATURE_SECRET": secret}):
        verify_jwt_token(_token(secret))
        with pytest.raises(HTTPException) as exc_info:
            verify_jwt_token(_token(secret.strip()))

    assert exc_info.value.status_code == 401


def test_dedicated_signing_key_alone_enables_authentication():
    with patch.dict(
        os.environ, {"AB_MANIFEST_SERVER_JWT_SIGNATURE_SECRET": "manifest-server-only"}
    ):
        with pytest.raises(HTTPException) as exc_info:
            verify_jwt_token(None)
        verify_jwt_token(_token("manifest-server-only"))

    assert exc_info.value.status_code == 401


def test_secret_with_surrounding_whitespace_is_used_as_is():
    """The platform signs with the exact secret value, so whitespace in it must not be stripped."""
    secret = " padded-secret\n"
    payload = {"exp": datetime.now(timezone.utc) + timedelta(hours=1)}
    exact = HTTPAuthorizationCredentials(
        scheme="Bearer", credentials=jwt.encode(payload, secret, algorithm="HS256")
    )
    stripped = HTTPAuthorizationCredentials(
        scheme="Bearer", credentials=jwt.encode(payload, secret.strip(), algorithm="HS256")
    )

    with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": secret}):
        verify_jwt_token(exact)
        with pytest.raises(HTTPException) as exc_info:
            verify_jwt_token(stripped)

    assert exc_info.value.status_code == 401


MANIFEST_ROUTES = [
    "/v1/manifest/check",
    "/v1/manifest/discover",
    "/v1/manifest/resolve",
    "/v1/manifest/full_resolve",
    "/v1/manifest/test_read",
]


@pytest.mark.parametrize("route", MANIFEST_ROUTES)
def test_manifest_routes_reject_unauthenticated_requests_when_secret_unset(route):
    """With no AB_JWT_SIGNATURE_SECRET, an unauthenticated caller cannot reach any manifest route."""
    client = TestClient(app)

    with patch.dict(os.environ, {}, clear=True):
        response = client.post(route, json={"manifest": {}, "config": {}})

    assert response.status_code == 503
    assert "AB_JWT_SIGNATURE_SECRET" in response.json()["detail"]
