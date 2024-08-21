"""
This module provides functions for handling authentication and authorization within the
application.
"""

import logging
from typing import Any, Awaitable, Callable, Optional

import httpx
from fastapi import Depends, Request
from httpx import ConnectError, HTTPStatusError, TimeoutException

from ska_sdp_dataproduct_api.configuration.settings import (
    SKA_PERMISSIONS_API_HOST,
    SKA_PERMISSIONS_API_PORT,
)
from ska_sdp_dataproduct_api.utilities.exceptions import AuthError

logger = logging.getLogger(__name__)


async def get_token_auth_header(request: Request) -> Optional[str]:
    """
    Obtains the Access Token from the Authorization Header.

    Raises:
        AuthError: If the Authorization header is missing, invalid, or malformed.
    """

    auth_header = request.headers.get("Authorization")

    if not auth_header:
        raise AuthError("authorization_header_missing: Authorization header is expected.", 401)

    parts = auth_header.split()

    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise AuthError(
            "invalid_header: Authorization header must be in the format Bearer token.", 401
        )

    return parts[1]


def extract_token(function: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Extract a token for a decorated function.

    Args:
        function: The function to decorate.

    Returns:
        The decorated function.
    """

    async def decorated(request: Request, token: str = Depends(get_token_from_request)) -> Any:
        """Decorated function that handles token validation.

        Args:
            request: The FastAPI request object.
            token: The validated access token.

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
    """Fetches user groups from the permissions API.

    Args:
        token: The access token.

    Returns:
        A dictionary containing the user's groups, or an empty dictionary
        if there's an error or no token is provided.

    Raises:
        None
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
    except (HTTPStatusError, AuthError, ConnectError, TimeoutException) as error:
        logger.error("Error fetching user groups: %s", error)
        return {"user_groups": []}
