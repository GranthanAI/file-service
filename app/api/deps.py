from collections.abc import Callable, Coroutine
from typing import Any
from uuid import UUID

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWTError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import PermissionLevel
from app.db.session import get_db  # noqa: F401 — re-exported for routers
from app.models.file import File
from app.services import permission_service

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


def require_permission(level: PermissionLevel) -> Callable[..., Coroutine[Any, Any, File]]:
    """Dependency factory (docs/LLD.md §4.3): routers declare their required
    access level declaratively, e.g. `Depends(require_permission(PermissionLevel.OWNER))`.
    Raises PermissionDeniedError/FileRecordNotFoundError (mapped to 403/404 by
    the handlers in app/core/exceptions.py) rather than returning a bool, so a
    failed check short-circuits the route entirely.

    Returns the resolved File row so a route can use it without a second
    lookup if it wants to (most routes ignore the return value).
    """

    async def _dependency(
        document_id: UUID,
        principal_id: UUID = Depends(get_current_user_id),
        db: AsyncSession = Depends(get_db),
    ) -> File:
        return await permission_service.assert_access(db, document_id, principal_id, level)

    return _dependency
