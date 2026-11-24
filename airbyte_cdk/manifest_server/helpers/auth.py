import os
from typing import Optional

import jwt
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

security = HTTPBearer(auto_error=False)

ALLOW_UNAUTHENTICATED_ENV_VAR = "AIRBYTE_MANIFEST_SERVER_ALLOW_UNAUTHENTICATED"


def verify_jwt_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security),
) -> None:
    """
    Verify the JWT bearer token against AB_JWT_SIGNATURE_SECRET.

    Requests are rejected when the secret is not set, unless authentication is
    explicitly disabled with AIRBYTE_MANIFEST_SERVER_ALLOW_UNAUTHENTICATED=true.

    Args:
        credentials: Bearer token credentials from request header

    Raises:
        HTTPException: If the secret is not configured, or the token is missing or invalid
    """
    jwt_secret = os.getenv("AB_JWT_SIGNATURE_SECRET", "").strip()

    if not jwt_secret:
        if os.getenv(ALLOW_UNAUTHENTICATED_ENV_VAR, "false").lower() == "true":
            return
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Manifest server authentication is not configured. Set AB_JWT_SIGNATURE_SECRET, "
                f"or set {ALLOW_UNAUTHENTICATED_ENV_VAR}=true to run without authentication."
            ),
        )

    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        jwt.decode(credentials.credentials, jwt_secret, algorithms=["HS256"])
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
