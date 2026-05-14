import os
from typing import Optional

from fastapi import Depends, Header, HTTPException, status

from grip.auth import get_current_user


async def require_access(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    user: Optional[dict] = Depends(get_current_user),
):
    """
    Access contract (FINAL):
    1. Valid JWT → allowed
    2. Valid API key → allowed
    3. Otherwise → denied
    """

    # Case 1: JWT present and valid
    if user is not None:
        return user

    # Case 2: API key
    expected = os.getenv("GRIP_API_KEY")
    if expected and x_api_key == expected:
        return {
            "sub": "api_key",
            "role": "service",
        }

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Access denied",
    )


def require_admin(identity: dict = Depends(require_access)):
    if identity.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )
    return identity