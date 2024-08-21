"""Module to test exceptions"""
from ska_sdp_dataproduct_api.utilities.exceptions import AuthError


def test_auth_error_initialization():
    """Tests the initialization of the AuthError class."""

    # Test with both message and status code
    error = AuthError("Unauthorized", 401)
    assert error.message == "Unauthorized"
    assert error.status_code == 401

    # Test with only message
    error = AuthError("Forbidden")
    assert error.message == "Forbidden"
    assert error.status_code == 401  # Default status code

    # Test with empty message
    error = AuthError("")
    assert error.message == ""
    assert error.status_code == 401  # Default status code
