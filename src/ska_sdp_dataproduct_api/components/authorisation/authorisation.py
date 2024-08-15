"""
This module provides functions for handling authentication and authorization within the
application.
"""

import logging
from typing import Any, Awaitable, Callable

import httpx
from fastapi import Depends, HTTPException, Request

from ska_sdp_dataproduct_api.configuration.settings import (
    SKA_PERMISSIONS_API_HOST,
    SKA_PERMISSIONS_API_PORT,
)
from ska_sdp_dataproduct_api.utilities.exceptions import AuthError

logger = logging.getLogger(__name__)


async def get_token_auth_header(request: Request):
    """Obtains the Access Token from the Authorization Header"""
    auth = request.headers.get("Authorization", None)
    if not auth:
        raise AuthError("authorization_header_missing: Authorization header is expected.", 401)

    parts = auth.split()

    if parts[0].lower() != "bearer":
        raise AuthError("invalid_header: Authorization header must start with Bearer.", 401)

    if len(parts) == 1:
        raise AuthError("invalid_header: Token not found.", 401)

    if len(parts) > 2:
        raise AuthError("invalid_header: Authorization header must be Bearer token.", 401)

    token = parts[1]
    return token


def extract_token(function: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Decorator to require authentication for a function.

    Args:
        f: The function to decorate.

    Returns:
        The decorated function.
    """

    async def decorated(request: Request, token: str = Depends(get_token_from_request)) -> Any:
        """Decorated function that handles token validation.

        Args:
            token: The validated access token.
            **kwargs: Additional arguments passed to the decorated function.

        Returns:
            The result of the decorated function.
        """
        return await function(token=token, request=request)

    return decorated


async def get_token_from_request(request: Request) -> str:
    """Extracts and validates an access token from a request.

    Args:
        request: The incoming request.

    Returns:
        The validated access token.

    Raises:
        HTTPException: If token is invalid or unauthorized access.
    """
    try:
        token = await get_token_auth_header(request)
        return token
    except AuthError as error:
        logger.warning("No valid token found, error: %s", error)
        return None


async def get_user_groups(token: str | None) -> dict[str, list[str]]:
    """Extracts and validates an access token from a request.

    Args:
        request: The incoming request.

    Returns:
        The validated access token.

    Raises:
        HTTPException: If token is invalid or unauthorized access.
    """
    try:
        if token is None:
            return {"user_groups": []}

        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(timeout=10) as client:
            permissions_api_verification_endpoint = (
                f"{SKA_PERMISSIONS_API_HOST}:{SKA_PERMISSIONS_API_PORT}/v1/getusergroupids"
            )
            response = await client.get(permissions_api_verification_endpoint, headers=headers)
            response.raise_for_status()  # Raise exception for non-200 status codes
        return response.json()
    except httpx.HTTPStatusError as error:
        raise HTTPException(
            status_code=401, detail=f"Token verification failed: {error}"
        ) from error
    except AuthError as error:
        raise HTTPException(status_code=401, detail=f"Invalid token: {error}") from error
