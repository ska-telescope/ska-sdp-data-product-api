"""Module to test authorisation"""
import pytest

from ska_sdp_dataproduct_api.components.authorisation.authorisation import (
    AuthError,
    get_token_auth_header,
)


class MockRequest:  # pylint: disable=too-few-public-methods
    """Mock the Request object"""

    def __init__(self, headers):
        self.headers = headers


@pytest.mark.asyncio
async def test_get_token_auth_header_success():
    """Tests successful extraction of token from Authorization header."""
    request = MockRequest(headers={"Authorization": "Bearer my_token"})
    assert await get_token_auth_header(request) == "my_token"


@pytest.mark.asyncio
async def test_get_token_auth_header_missing():
    """Tests raising AuthError when Authorization header is missing."""
    request = MockRequest(headers={})
    with pytest.raises(AuthError) as excinfo:
        assert await get_token_auth_header(request) == "my_token"
    assert excinfo.value.status_code == 401
    assert "authorization_header_missing" in str(excinfo.value)


@pytest.mark.asyncio
async def test_get_token_auth_header_invalid():
    """Tests raising AuthError when Authorization header is malformed."""
    request = MockRequest(headers={"Authorization": "Invalid Header"})
    with pytest.raises(AuthError) as excinfo:
        assert await get_token_auth_header(request) == "my_token"
    assert excinfo.value.status_code == 401
    assert "invalid_header" in str(excinfo.value)
