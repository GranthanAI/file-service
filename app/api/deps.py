from uuid import UUID

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWTError

from app.core.config import settings
from app.db.session import get_db  # noqa: F401 — re-exported for routers

_bearer_scheme = HTTPBearer()


async def get_current_user_id(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> UUID:
    """Verifies a JWT issued by auth-service and returns the caller's user
    id (the `sub` claim) as `owner_id`/`requester_id` throughout File
    Service. Purely local verification — no network call to auth-service,
    no DB lookup — matching auth-service's own actual implementation
    (HS256 + shared JWT_SECRET_KEY, see docs/core/config.py's Auth section).

    File Service has no local user table, so unlike auth-service's
    get_current_user (which returns a full User row), this only returns the
    UUID — that's all any File Service endpoint ever needs.
    """
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
            options={"require": ["sub", "exp"]},
        )
    except PyJWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from e

    try:
        return UUID(payload["sub"])
    except (KeyError, ValueError) as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token subject",
            headers={"WWW-Authenticate": "Bearer"},
        ) from e
