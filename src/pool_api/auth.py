"""Authentication checks for protected API routes."""

import secrets

from fastapi import HTTPException, status

from settings import settings


def verify_api_key(api_key: str) -> None:
    """Reject a bearer token that does not match the configured API key."""
    if not secrets.compare_digest(api_key, settings.api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
