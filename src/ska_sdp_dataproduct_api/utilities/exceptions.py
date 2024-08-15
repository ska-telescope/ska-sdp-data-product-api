"""Module containing classes used to handle errors"""
from typing import Union


class AuthError(Exception):
    """
    Represents an authentication error.

    Attributes:
        message (str): A descriptive error message.
        status_code (int): The HTTP status code associated with the error.
    """

    def __init__(self, message: str, status_code: Union[int, None] = None):
        self.message = message
        self.status_code = status_code or 401  # Default to unauthorized if not provided
