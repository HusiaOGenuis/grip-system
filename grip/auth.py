import os
import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

security = HTTPBearer(auto_error=False)

PROJECT_REF = os.getenv("SUPABASE_PROJECT_REF")
AUDIENCE = os.getenv("SUPABASE_JWT_AUDIENCE", "authenticated")

ISSUER = f"https://{PROJECT_REF}.supabase.co/auth/v1"
JWKS_URL = f"{ISSUER}/keys"

_jwks_cache = None


async def _get_jwks():
    global _jwks_cache
    if _jwks_cache is None:
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                resp = await client.get(JWKS_URL)
                resp.raise_for_status()
                _jwks_cache = resp.json()
        except httpx.HTTPError:
            return None
    return _jwks_cache


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
):
    if credentials is None:
        return None

    jwks = await _get_jwks()
    if jwks is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="JWT verification unavailable",
        )

    try:
        return jwt.decode(
            credentials.credentials,
            jwks,
            algorithms=["RS256"],
            audience=AUDIENCE,
            issuer=ISSUER,
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired JWT",
        )