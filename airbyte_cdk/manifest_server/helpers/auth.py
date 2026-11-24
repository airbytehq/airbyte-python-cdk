import os
from typing import Any, Optional

import jwt
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

security = HTTPBearer(auto_error=False)

ALLOW_UNAUTHENTICATED_ENV_VAR = "AIRBYTE_MANIFEST_SERVER_ALLOW_UNAUTHENTICATED"
MANIFEST_SERVER_SIGNATURE_SECRET_ENV_VAR = "AB_MANIFEST_SERVER_JWT_SIGNATURE_SECRET"
SHARED_SIGNATURE_SECRET_ENV_VAR = "AB_JWT_SIGNATURE_SECRET"
REQUIRE_AUDIENCE_ENV_VAR = "AB_MANIFEST_SERVER_JWT_REQUIRE_AUDIENCE"
MANIFEST_SERVER_AUDIENCE = "airbyte-manifest-server"


def _signature_secret() -> str:
    """The manifest server's own signing key, falling back to the key shared by Airbyte services."""
    return (
        os.getenv(MANIFEST_SERVER_SIGNATURE_SECRET_ENV_VAR, "").strip()
        or os.getenv(SHARED_SIGNATURE_SECRET_ENV_VAR, "").strip()
    )


def _audience_is_valid(audience: Any) -> bool:
    """Tokens addressed to another audience are rejected; tokens without one only when it is required."""
    if audience is None:
        return os.getenv(REQUIRE_AUDIENCE_ENV_VAR, "false").lower() != "true"
    audiences = [audience] if isinstance(audience, str) else audience
    return isinstance(audiences, list) and MANIFEST_SERVER_AUDIENCE in audiences


def verify_jwt_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security),
) -> None:
    """
    Verify the JWT bearer token against the manifest server's signing key.

    The key is AB_MANIFEST_SERVER_JWT_SIGNATURE_SECRET, or AB_JWT_SIGNATURE_SECRET when that is
    not set. Tokens must carry an expiry, and an audience other than "airbyte-manifest-server" is
    rejected. With AB_MANIFEST_SERVER_JWT_REQUIRE_AUDIENCE=true, tokens without an audience are
    rejected too.

    Requests are rejected when the secret is not set, unless authentication is
    explicitly disabled with AIRBYTE_MANIFEST_SERVER_ALLOW_UNAUTHENTICATED=true.

    Args:
        credentials: Bearer token credentials from request header

    Raises:
        HTTPException: If the secret is not configured, or the token is missing or invalid
    """
    jwt_secret = _signature_secret()

    if not jwt_secret:
        if os.getenv(ALLOW_UNAUTHENTICATED_ENV_VAR, "false").lower() == "true":
            return
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Manifest server authentication is not configured. Set "
                f"{MANIFEST_SERVER_SIGNATURE_SECRET_ENV_VAR} or {SHARED_SIGNATURE_SECRET_ENV_VAR}, "
                f"or set {ALLOW_UNAUTHENTICATED_ENV_VAR}=true to run without authentication."
            ),
        )

    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    invalid_token = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        claims = jwt.decode(
            credentials.credentials,
            jwt_secret,
            algorithms=["HS256"],
            options={"require": ["exp"], "verify_aud": False},
        )
    except jwt.InvalidTokenError:
        raise invalid_token
    if not _audience_is_valid(claims.get("aud")):
        raise invalid_token
