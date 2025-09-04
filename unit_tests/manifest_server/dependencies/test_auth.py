import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import jwt
import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from airbyte_cdk.manifest_server.dependencies import verify_jwt_token


class TestVerifyJwtToken:
    """Test cases for JWT token verification."""

    def test_no_secret_allows_all_requests(self):
        """Test that when AB_JWT_SIGNATURE_SECRET is not set, all requests pass through."""
        with patch.dict(os.environ, {}, clear=True):
            # Should not raise any exception
            verify_jwt_token(None)
            verify_jwt_token(HTTPAuthorizationCredentials(scheme="Bearer", credentials="any-token"))

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

    def test_empty_secret_allows_all_requests(self):
        """Test that empty secret string allows all requests."""
        with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": ""}):
            # Should not raise any exception
            verify_jwt_token(None)
            verify_jwt_token(HTTPAuthorizationCredentials(scheme="Bearer", credentials="any-token"))

    def test_token_without_required_claims_passes(self):
        """Test that tokens without standard claims still pass if signature is valid."""
        secret = "test-secret"
        minimal_payload = {"custom": "data"}  # No exp, iat, sub etc.
        minimal_token = jwt.encode(minimal_payload, secret, algorithm="HS256")

        with patch.dict(os.environ, {"AB_JWT_SIGNATURE_SECRET": secret}):
            minimal_credentials = HTTPAuthorizationCredentials(
                scheme="Bearer", credentials=minimal_token
            )

            # Should not raise any exception - we only verify signature
            verify_jwt_token(minimal_credentials)
